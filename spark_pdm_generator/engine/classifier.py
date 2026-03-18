"""Entity classifier: detects entity types and identifies grain keys."""

from spark_pdm_generator.models.graph import ERGraph
from spark_pdm_generator.models.logical import (
    EntityType,
    LogicalModel,
)
from spark_pdm_generator.models.physical import (
    PhysicalModel,
    TransformationRule,
    WarningLevel,
)


def classify_entities(
    model: LogicalModel, graph: ERGraph, output: PhysicalModel
) -> None:
    """Classify entities that have UNKNOWN type based on graph structure.

    Heuristics:
    - Entities with many children (out_degree > 2) and few parents -> likely FACT
    - Entities with many parents referencing them but few children -> likely DIMENSION
    - Entities that are both parent and child in M:N -> likely BRIDGE
    - Entities with very few rows (< 1000) and only referenced -> likely REFERENCE
    - Default: DIMENSION

    Modifies entities in-place on the LogicalModel.
    """
    for entity in model.entities:
        if entity.entity_type != EntityType.UNKNOWN:
            output.add_log_entry(
                rule=TransformationRule.ENTITY_CLASSIFY,
                source_entity=entity.entity_name,
                target_entity=entity.entity_name,
                description=f"Entity type '{entity.entity_type.value}' was provided in input",
                rationale="User-specified entity type preserved",
            )
            continue

        classified_type = _infer_entity_type(entity.entity_name, model, graph)
        entity.entity_type = classified_type

        output.add_log_entry(
            rule=TransformationRule.ENTITY_CLASSIFY,
            source_entity=entity.entity_name,
            target_entity=entity.entity_name,
            description=f"Classified as {classified_type.value}",
            rationale=_classification_rationale(entity.entity_name, graph, model),
        )


def _infer_entity_type(
    entity_name: str, model: LogicalModel, graph: ERGraph
) -> EntityType:
    """Infer entity type from graph structure and stats."""
    in_deg = graph.in_degree(entity_name)
    out_deg = graph.out_degree(entity_name)
    attr_count = graph.get_attribute_count(entity_name)
    row_count = model.get_entity_row_count(entity_name)
    m2m = graph.get_many_to_many_relationships(entity_name)

    # Bridge: involved in M:N relationships with exactly 2 FK parents
    # and has mostly FK columns
    if m2m:
        pks = model.get_primary_keys(entity_name)
        fk_attrs = [
            a for a in model.get_attributes_for_entity(entity_name) if a.is_foreign_key
        ]
        if len(fk_attrs) >= 2 and attr_count <= len(fk_attrs) + 5:
            return EntityType.BRIDGE

    # Reference: very small, only referenced by others, few attributes
    if row_count is not None and row_count < 1000 and out_deg == 0 and attr_count < 20:
        return EntityType.REFERENCE

    # Fact: many FK parents (in_degree high), wide, large row count
    if in_deg >= 2 and attr_count > 20:
        return EntityType.FACT

    # Fact: very large row count
    if row_count is not None and row_count > 10_000_000:
        return EntityType.FACT

    # Dimension: referenced by others (has children), moderate size
    if out_deg > 0 and in_deg <= 2:
        return EntityType.DIMENSION

    # Default
    return EntityType.DIMENSION


def _classification_rationale(
    entity_name: str, graph: ERGraph, model: LogicalModel
) -> str:
    """Build a human-readable rationale for the classification."""
    parts = []
    parts.append(f"in_degree={graph.in_degree(entity_name)}")
    parts.append(f"out_degree={graph.out_degree(entity_name)}")
    parts.append(f"attribute_count={graph.get_attribute_count(entity_name)}")

    row_count = model.get_entity_row_count(entity_name)
    if row_count is not None:
        parts.append(f"estimated_row_count={row_count:,}")
    else:
        parts.append("estimated_row_count=unknown")

    return "; ".join(parts)


def validate_classifications(
    model: LogicalModel, graph: ERGraph, output: PhysicalModel
) -> None:
    """Validate entity classifications and emit warnings for potential issues."""
    fact_count = sum(
        1 for e in model.entities if e.entity_type == EntityType.FACT
    )
    dim_count = sum(
        1 for e in model.entities if e.entity_type == EntityType.DIMENSION
    )

    if fact_count == 0:
        output.add_warning(
            level=WarningLevel.WARNING,
            message="No FACT entities detected. The model may be missing fact tables "
            "or entity types may need manual classification.",
            recommendation="Review entity types in the input or add entity_type column.",
        )

    for entity in model.entities:
        if entity.entity_type == EntityType.FACT:
            pks = model.get_primary_keys(entity.entity_name)
            if not pks:
                output.add_warning(
                    level=WarningLevel.WARNING,
                    entity=entity.entity_name,
                    message="FACT entity has no primary key defined.",
                    recommendation="Define primary key columns for grain identification.",
                )
