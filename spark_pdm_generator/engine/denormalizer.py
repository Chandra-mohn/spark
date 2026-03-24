"""Denormalizer: merges 1:1 relationships, embeds small dimensions, flattens hierarchies."""

import networkx as nx

from spark_pdm_generator.models.graph import ERGraph
from spark_pdm_generator.models.logical import (
    Attribute,
    DenormalizationMode,
    Entity,
    EntityType,
    LogicalModel,
)
from spark_pdm_generator.models.physical import (
    PhysicalAttribute,
    PhysicalEntity,
    PhysicalEntityType,
    PhysicalModel,
    TransformationRule,
    WarningLevel,
)
from spark_pdm_generator.engine.utils import sanitize_name as _sanitize_name
from spark_pdm_generator.rules import defaults


class DenormalizationPlan:
    """Tracks which entities get merged into which physical tables."""

    def __init__(self) -> None:
        # entity_name -> physical_entity_name it gets merged into
        self.merge_targets: dict[str, str] = {}
        # entity_name -> True if this entity is absorbed (not a standalone table)
        self.absorbed: set[str] = set()
        # physical_entity_name -> list of source entity names
        self.physical_sources: dict[str, list[str]] = {}

    def mark_absorbed(self, entity_name: str, into_entity: str) -> None:
        """Mark an entity as absorbed into another physical table.

        Handles transitive absorption: if entity_name already has sources
        absorbed into it (e.g., composition children), those sources are
        carried over to into_entity so no attributes are orphaned.
        """
        self.absorbed.add(entity_name)
        self.merge_targets[entity_name] = into_entity
        if into_entity not in self.physical_sources:
            self.physical_sources[into_entity] = [into_entity]
        if entity_name not in self.physical_sources[into_entity]:
            self.physical_sources[into_entity].append(entity_name)

        # Carry over any sources that were previously absorbed into entity_name
        if entity_name in self.physical_sources:
            for prior_source in self.physical_sources[entity_name]:
                if (
                    prior_source != entity_name
                    and prior_source != into_entity
                    and prior_source not in self.physical_sources[into_entity]
                ):
                    self.physical_sources[into_entity].append(prior_source)

    def is_absorbed(self, entity_name: str) -> bool:
        return entity_name in self.absorbed

    def get_target(self, entity_name: str) -> str:
        return self.merge_targets.get(entity_name, entity_name)


def build_denormalization_plan(
    model: LogicalModel, graph: ERGraph, output: PhysicalModel,
    overrides: "OverrideRegistry | None" = None,
) -> DenormalizationPlan:
    """Analyze the model and determine which entities to merge.

    Strategy:
    1. Merge 1:1 relationships (always)
    2. Embed small dimensions into facts (when row_count < threshold)
    3. Log all decisions

    Returns a DenormalizationPlan that downstream steps use.
    """
    from spark_pdm_generator.engine.utils import OverrideRegistry

    config = model.config
    threshold = config.small_dim_row_threshold
    plan = DenormalizationPlan()

    # Validate graph has no cycles before denormalization
    cyclic_entities: set[str] = set()
    if not nx.is_directed_acyclic_graph(graph._graph):
        cycles = list(nx.simple_cycles(graph._graph))
        for cycle in cycles:
            cyclic_entities.update(cycle)
        cycle_str = "; ".join(" -> ".join(c) for c in cycles[:3])
        output.add_warning(
            level=WarningLevel.WARNING,
            message=(
                f"ER graph contains cycles: {cycle_str}. "
                f"Entities in cycles will be excluded from denormalization."
            ),
            recommendation="Remove circular relationships or mark them as non-identifying.",
        )

    blocked_denorm = overrides.blocked_denorm if overrides else set()
    # Entities in cycles are treated as blocked to prevent infinite loops
    blocked_denorm = blocked_denorm | cyclic_entities

    # Phase 1: Merge 1:1 relationships
    for entity in model.entities:
        if entity.entity_type != EntityType.FACT:
            continue
        one_to_one_parents = graph.get_one_to_one_parents(entity.entity_name)
        for parent_name in one_to_one_parents:
            if parent_name in blocked_denorm:
                output.add_log_entry(
                    rule=TransformationRule.OVERRIDE_APPLIED,
                    source_entity=parent_name,
                    target_entity=entity.entity_name,
                    description=f"BLOCK_DENORMALIZE override: keeping '{parent_name}' separate",
                    rationale="User override applied",
                )
                continue

            plan.mark_absorbed(parent_name, entity.entity_name)
            output.add_log_entry(
                rule=TransformationRule.DENORMALIZE_1_TO_1,
                source_entity=parent_name,
                target_entity=entity.entity_name,
                description=f"Merged '{parent_name}' into '{entity.entity_name}'",
                rationale=f"1:1 relationship -- always merge to reduce joins",
            )

    # Phase 1b: Absorb composition children into FACT tables only.
    # Composition means the child data is embedded in the parent (MongoDB
    # subdocuments). We only absorb when the parent is a FACT -- this avoids
    # cascading absorption where dimension parents swallow fact children.
    # Non-FACT composition parents are handled by AGGRESSIVE mode later
    # (the fact absorbs the dimension, which transitively brings the
    # composition children along).
    for entity in model.entities:
        if entity.entity_type != EntityType.FACT:
            continue
        comp_children = graph.get_composition_children(entity.entity_name)
        for child_name in comp_children:
            if child_name in blocked_denorm:
                output.add_log_entry(
                    rule=TransformationRule.OVERRIDE_APPLIED,
                    source_entity=child_name,
                    target_entity=entity.entity_name,
                    description=f"BLOCK_DENORMALIZE override: keeping '{child_name}' separate",
                    rationale="User override applied",
                )
                continue

            if plan.is_absorbed(child_name):
                continue

            plan.mark_absorbed(child_name, entity.entity_name)
            output.add_log_entry(
                rule=TransformationRule.DENORMALIZE_COMPOSITION,
                source_entity=child_name,
                target_entity=entity.entity_name,
                description=(
                    f"Absorbed composition child '{child_name}' "
                    f"into '{entity.entity_name}'"
                ),
                rationale=(
                    "Composition relationship (embedded subdocument) -- "
                    "absorbing into parent FACT table"
                ),
            )

    denorm_mode = config.denormalization_mode

    if denorm_mode == DenormalizationMode.AGGRESSIVE:
        # Aggressive: absorb ALL 1:N parent dimensions regardless of size
        _plan_aggressive_denorm(model, graph, plan, output, blocked_denorm)
    elif denorm_mode == DenormalizationMode.CONSERVATIVE:
        # Conservative: only absorb 1:1, never absorb 1:N dimensions
        _plan_conservative_denorm(model, graph, plan, output, blocked_denorm)
    else:
        # Auto: absorb small dims, keep large ones separate
        _plan_auto_denorm(model, graph, plan, output, blocked_denorm, threshold)

    return plan


def _plan_aggressive_denorm(
    model: LogicalModel,
    graph: ERGraph,
    plan: DenormalizationPlan,
    output: PhysicalModel,
    blocked_denorm: set[str],
) -> None:
    """Aggressive mode: absorb ALL parent dimensions into fact tables."""
    for entity in model.entities:
        if entity.entity_type != EntityType.FACT:
            continue
        all_dims = graph.get_all_dimension_parents(entity.entity_name)
        for dim_name in all_dims:
            if dim_name in blocked_denorm:
                output.add_log_entry(
                    rule=TransformationRule.OVERRIDE_APPLIED,
                    source_entity=dim_name,
                    target_entity=entity.entity_name,
                    description=f"BLOCK_DENORMALIZE override: keeping '{dim_name}' separate",
                    rationale="User override applied",
                )
                continue

            if plan.is_absorbed(dim_name):
                continue

            row_count = model.get_entity_row_count(dim_name)
            row_str = f"{row_count:,}" if row_count else "unknown"

            plan.mark_absorbed(dim_name, entity.entity_name)
            output.add_log_entry(
                rule=TransformationRule.DENORMALIZE_SMALL_DIM,
                source_entity=dim_name,
                target_entity=entity.entity_name,
                description=(
                    f"Absorbed '{dim_name}' into '{entity.entity_name}' "
                    f"(aggressive mode)"
                ),
                rationale=(
                    f"denormalization_mode=AGGRESSIVE; "
                    f"estimated_row_count={row_str}; "
                    f"absorbing all parent dimensions for join elimination"
                ),
            )

    # Recursively absorb parents of absorbed entities (transitive absorption)
    _absorb_transitive_parents(model, graph, plan, output, blocked_denorm)


def _absorb_transitive_parents(
    model: LogicalModel,
    graph: ERGraph,
    plan: DenormalizationPlan,
    output: PhysicalModel,
    blocked_denorm: set[str],
) -> None:
    """Absorb parents of already-absorbed entities into the same target fact."""
    changed = True
    while changed:
        changed = False
        for entity_name in list(plan.absorbed):
            target = plan.get_target(entity_name)
            parents = graph.get_all_dimension_parents(entity_name)
            one_to_one_parents = graph.get_one_to_one_parents(entity_name)
            parents = parents + one_to_one_parents

            for parent_name in parents:
                if parent_name in blocked_denorm:
                    continue
                if plan.is_absorbed(parent_name):
                    continue

                row_count = model.get_entity_row_count(parent_name)
                row_str = f"{row_count:,}" if row_count else "unknown"

                plan.mark_absorbed(parent_name, target)
                output.add_log_entry(
                    rule=TransformationRule.DENORMALIZE_SMALL_DIM,
                    source_entity=parent_name,
                    target_entity=target,
                    description=(
                        f"Absorbed '{parent_name}' into '{target}' "
                        f"(transitive via '{entity_name}')"
                    ),
                    rationale=(
                        f"denormalization_mode=AGGRESSIVE; "
                        f"estimated_row_count={row_str}; "
                        f"transitive parent absorption for full flattening"
                    ),
                )
                changed = True


def _plan_conservative_denorm(
    model: LogicalModel,
    graph: ERGraph,
    plan: DenormalizationPlan,
    output: PhysicalModel,
    blocked_denorm: set[str],
) -> None:
    """Conservative mode: only 1:1 merges were done (Phase 1). Keep all 1:N separate."""
    for entity in model.entities:
        if entity.entity_type != EntityType.FACT:
            continue
        all_dims = graph.get_all_dimension_parents(entity.entity_name)
        for dim_name in all_dims:
            if plan.is_absorbed(dim_name):
                continue
            row_count = model.get_entity_row_count(dim_name)
            row_str = f"{row_count:,}" if row_count else "unknown"
            output.add_log_entry(
                rule=TransformationRule.KEEP_SEPARATE,
                source_entity=dim_name,
                target_entity=dim_name,
                description=f"Keeping '{dim_name}' as separate dimension table",
                rationale=(
                    f"denormalization_mode=CONSERVATIVE; "
                    f"estimated_row_count={row_str}; "
                    f"only 1:1 merges allowed in conservative mode"
                ),
            )


def _plan_auto_denorm(
    model: LogicalModel,
    graph: ERGraph,
    plan: DenormalizationPlan,
    output: PhysicalModel,
    blocked_denorm: set[str],
    threshold: int,
) -> None:
    """Auto mode: absorb small dims, keep large ones separate (original behavior)."""
    # Phase 2: Embed small dimensions into facts
    for entity in model.entities:
        if entity.entity_type != EntityType.FACT:
            continue
        small_dims = graph.get_small_dimension_parents(
            entity.entity_name, model, threshold
        )
        for dim_name in small_dims:
            if dim_name in blocked_denorm:
                output.add_log_entry(
                    rule=TransformationRule.OVERRIDE_APPLIED,
                    source_entity=dim_name,
                    target_entity=entity.entity_name,
                    description=f"BLOCK_DENORMALIZE override: keeping '{dim_name}' separate",
                    rationale="User override applied",
                )
                continue

            if plan.is_absorbed(dim_name):
                continue

            row_count = model.get_entity_row_count(dim_name)

            # Check if this dimension is referenced by multiple facts
            children = graph.get_children(dim_name)
            fact_children = [
                c
                for c in children
                if model.get_entity(c)
                and model.get_entity(c).entity_type == EntityType.FACT
            ]

            if len(fact_children) > 1:
                output.add_log_entry(
                    rule=TransformationRule.DENORMALIZE_SMALL_DIM,
                    source_entity=dim_name,
                    target_entity=entity.entity_name,
                    description=(
                        f"Embedded '{dim_name}' attributes into '{entity.entity_name}' "
                        f"(shared by {len(fact_children)} facts)"
                    ),
                    rationale=(
                        f"estimated_row_count={row_count:,} < threshold={threshold:,}; "
                        f"small dimension denormalized for join elimination"
                    ),
                )
            else:
                output.add_log_entry(
                    rule=TransformationRule.DENORMALIZE_SMALL_DIM,
                    source_entity=dim_name,
                    target_entity=entity.entity_name,
                    description=f"Embedded '{dim_name}' attributes into '{entity.entity_name}'",
                    rationale=(
                        f"estimated_row_count={row_count:,} < threshold={threshold:,}; "
                        f"small dimension denormalized for join elimination"
                    ),
                )

            plan.mark_absorbed(dim_name, entity.entity_name)

    # Phase 3: Large dimensions -- keep separate, log decision
    for entity in model.entities:
        if entity.entity_type != EntityType.FACT:
            continue
        large_dims = graph.get_large_dimension_parents(
            entity.entity_name, model, threshold
        )
        for dim_name in large_dims:
            if plan.is_absorbed(dim_name):
                continue
            row_count = model.get_entity_row_count(dim_name)
            row_str = f"{row_count:,}" if row_count else "unknown"
            output.add_log_entry(
                rule=TransformationRule.KEEP_SEPARATE,
                source_entity=dim_name,
                target_entity=dim_name,
                description=f"Keeping '{dim_name}' as separate dimension table",
                rationale=(
                    f"estimated_row_count={row_str} >= threshold={threshold:,}; "
                    f"too large to denormalize -- will use bucketed join"
                ),
            )


def apply_denormalization(
    model: LogicalModel,
    graph: ERGraph,
    plan: DenormalizationPlan,
    output: PhysicalModel,
) -> None:
    """Create physical entities and attributes based on the denormalization plan.

    For each non-absorbed entity, creates a PhysicalEntity with:
    - Its own attributes
    - Attributes from all entities absorbed into it (prefixed to avoid collision)
    """
    config = model.config

    for entity in model.entities:
        if plan.is_absorbed(entity.entity_name):
            continue

        # Determine physical entity name and type
        phys_name = _make_physical_name(entity)
        phys_type = _map_entity_type(entity.entity_type)
        source_entities = plan.physical_sources.get(
            entity.entity_name, [entity.entity_name]
        )

        phys_entity = PhysicalEntity(
            physical_entity_name=phys_name,
            source_entities=source_entities,
            entity_type=phys_type,
            grain_description=entity.grain_description,
            domain=entity.domain,
            estimated_row_count=entity.estimated_row_count,
            compression_codec=config.compression.value,
            row_group_size_mb=config.row_group_size_mb,
        )
        output.physical_entities.append(phys_entity)

        # Add own attributes
        for attr in model.get_attributes_for_entity(entity.entity_name):
            _add_physical_attribute(output, phys_name, entity.entity_name, attr, model)

        # Add absorbed entity attributes
        for absorbed_name in source_entities:
            if absorbed_name == entity.entity_name:
                continue
            absorbed_attrs = model.get_attributes_for_entity(absorbed_name)
            # Find relationship in either direction (parent->child or child->parent)
            rel = graph.get_relationship(absorbed_name, entity.entity_name)
            if not rel:
                rel = graph.get_relationship(entity.entity_name, absorbed_name)
            # Skip FK columns that duplicate the join key
            fk_cols: set[str] = set()
            if rel:
                fk_cols = set(rel.child_key_columns) | set(rel.parent_key_columns)

            for attr in absorbed_attrs:
                if attr.is_primary_key and attr.attribute_name in fk_cols:
                    continue
                _add_physical_attribute(
                    output,
                    phys_name,
                    absorbed_name,
                    attr,
                    model,
                    prefix=absorbed_name,
                )


def _make_physical_name(entity: Entity) -> str:
    """Generate the physical table name with appropriate prefix."""
    name = _sanitize_name(entity.entity_name.lower())
    if entity.entity_type == EntityType.FACT:
        if not name.startswith(defaults.FACT_PREFIX):
            return f"{defaults.FACT_PREFIX}{name}"
    elif entity.entity_type == EntityType.DIMENSION:
        if not name.startswith(defaults.DIMENSION_PREFIX):
            return f"{defaults.DIMENSION_PREFIX}{name}"
    elif entity.entity_type == EntityType.BRIDGE:
        if not name.startswith(defaults.BRIDGE_PREFIX):
            return f"{defaults.BRIDGE_PREFIX}{name}"
    elif entity.entity_type == EntityType.REFERENCE:
        if not name.startswith(defaults.DIMENSION_PREFIX):
            return f"{defaults.DIMENSION_PREFIX}{name}"
    return name



def _map_entity_type(entity_type: EntityType) -> PhysicalEntityType:
    """Map logical entity type to physical entity type."""
    mapping = {
        EntityType.FACT: PhysicalEntityType.FACT_TABLE,
        EntityType.DIMENSION: PhysicalEntityType.DIMENSION_TABLE,
        EntityType.BRIDGE: PhysicalEntityType.BRIDGE_TABLE,
        EntityType.REFERENCE: PhysicalEntityType.DIMENSION_TABLE,
        EntityType.AGGREGATE: PhysicalEntityType.AGGREGATE_TABLE,
        EntityType.UNKNOWN: PhysicalEntityType.DIMENSION_TABLE,
    }
    return mapping.get(entity_type, PhysicalEntityType.DIMENSION_TABLE)


def _add_physical_attribute(
    output: PhysicalModel,
    phys_entity_name: str,
    source_entity: str,
    attr: Attribute,
    model: LogicalModel,
    prefix: str = "",
) -> None:
    """Add a physical attribute to the output model."""
    attr_name = attr.attribute_name
    if prefix:
        attr_name = f"{_sanitize_name(prefix)}_{attr.attribute_name}"

    dist = model.get_distribution(source_entity, attr.attribute_name)
    cardinality = dist.distinct_count if dist else None
    null_pct = dist.null_percentage if dist else 0.0

    phys_attr = PhysicalAttribute(
        physical_entity_name=phys_entity_name,
        attribute_name=attr_name,
        source_entity=source_entity,
        source_attribute=attr.attribute_name,
        nullable=attr.nullable,
        is_primary_key=attr.is_primary_key,
        estimated_cardinality=cardinality,
        estimated_null_pct=null_pct,
    )
    output.physical_attributes.append(phys_attr)
