"""Partitioner: handles vertical splitting and horizontal partition column selection."""

from collections import Counter
from typing import Optional

from spark_pdm_generator.engine.utils import find_logical_attribute, is_date_like
from spark_pdm_generator.models.logical import LogicalModel
from spark_pdm_generator.models.physical import (
    PhysicalAttribute,
    PhysicalEntity,
    PhysicalEntityType,
    PhysicalModel,
    TransformationRule,
    WarningLevel,
)
from spark_pdm_generator.rules import defaults


def apply_vertical_split(
    model: LogicalModel, output: PhysicalModel,
    overrides: "OverrideRegistry | None" = None,
) -> None:
    """Split wide physical entities into domain-aligned groups.

    Any physical entity with more columns than column_threshold_for_vertical_split
    gets split into sub-tables grouped by domain_group.
    """
    from spark_pdm_generator.engine.utils import OverrideRegistry

    threshold = model.config.column_threshold_for_vertical_split
    force_domain_groups = overrides.force_domain_groups if overrides else {}

    entities_to_split = []
    for phys_entity in output.physical_entities:
        attrs = output.get_attributes_for_entity(phys_entity.physical_entity_name)
        if len(attrs) > threshold:
            entities_to_split.append(phys_entity)

    for phys_entity in entities_to_split:
        _split_entity(model, output, phys_entity, force_domain_groups)


def _split_entity(
    model: LogicalModel,
    output: PhysicalModel,
    phys_entity: PhysicalEntity,
    force_domain_groups: dict[str, str],
) -> None:
    """Split a single wide entity into domain groups."""
    attrs = output.get_attributes_for_entity(phys_entity.physical_entity_name)

    # Group attributes by domain
    domain_groups: dict[str, list[PhysicalAttribute]] = {}
    key_attrs: list[PhysicalAttribute] = []

    # Build set of primary key attribute names for this entity
    pk_names: set[str] = set()
    for source_name in phys_entity.source_entities:
        for pk_attr in model.get_primary_keys(source_name):
            pk_names.add(pk_attr.attribute_name)
            pk_names.add(f"{source_name}_{pk_attr.attribute_name}")

    for attr in attrs:
        # Check for forced domain group override
        if attr.attribute_name in force_domain_groups:
            domain = force_domain_groups[attr.attribute_name]
        elif attr.source_attribute in force_domain_groups:
            domain = force_domain_groups[attr.source_attribute]
        else:
            # Look up domain_group from the logical attribute
            logical_attr = find_logical_attribute(model, attr.source_entity, attr.source_attribute)
            if logical_attr and logical_attr.domain_group:
                domain = logical_attr.domain_group
            else:
                # Fall back to source entity's domain
                source_entity = model.get_entity(attr.source_entity)
                domain = source_entity.domain if source_entity else defaults.DEFAULT_DOMAIN

        # Key columns (PK, bucket, partition) go into ALL domain groups
        is_pk = attr.attribute_name in pk_names
        if is_pk or attr.is_bucket_col or attr.is_partition_col:
            key_attrs.append(attr)
            continue

        if domain not in domain_groups:
            domain_groups[domain] = []
        domain_groups[domain].append(attr)

    # If we only get one domain group, don't split
    if len(domain_groups) <= 1:
        single_domain = next(iter(domain_groups), defaults.DEFAULT_DOMAIN)
        output.add_warning(
            level=WarningLevel.INFO,
            entity=phys_entity.physical_entity_name,
            message=(
                f"Entity has {len(attrs)} columns but all belong to domain "
                f"'{single_domain}'. No vertical split applied."
            ),
            recommendation="Add domain_group to attributes for finer splitting.",
        )
        return

    # Remove original entity and its attributes
    output.physical_entities = [
        e
        for e in output.physical_entities
        if e.physical_entity_name != phys_entity.physical_entity_name
    ]
    output.physical_attributes = [
        a
        for a in output.physical_attributes
        if a.physical_entity_name != phys_entity.physical_entity_name
    ]

    # Create domain group sub-entities
    base_name = phys_entity.physical_entity_name
    for domain, domain_attrs in domain_groups.items():
        group_name = f"{base_name}{defaults.DOMAIN_SEPARATOR}{domain}"

        group_entity = PhysicalEntity(
            physical_entity_name=group_name,
            source_entities=phys_entity.source_entities,
            entity_type=PhysicalEntityType.DOMAIN_GROUP,
            grain_description=phys_entity.grain_description,
            domain=domain,
            partition_columns=phys_entity.partition_columns,
            bucket_column=phys_entity.bucket_column,
            bucket_count=phys_entity.bucket_count,
            sort_columns=phys_entity.sort_columns,
            estimated_row_count=phys_entity.estimated_row_count,
            compression_codec=phys_entity.compression_codec,
            row_group_size_mb=phys_entity.row_group_size_mb,
            storage_format=phys_entity.storage_format,
            denormalization_notes=f"Domain group '{domain}' split from '{base_name}'",
        )
        output.physical_entities.append(group_entity)

        # Add key columns to this group
        for key_attr in key_attrs:
            new_attr = key_attr.model_copy()
            new_attr.physical_entity_name = group_name
            output.physical_attributes.append(new_attr)

        # Add domain-specific columns
        for attr in domain_attrs:
            new_attr = attr.model_copy()
            new_attr.physical_entity_name = group_name
            output.physical_attributes.append(new_attr)

        output.add_log_entry(
            rule=TransformationRule.VERTICAL_SPLIT,
            source_entity=base_name,
            target_entity=group_name,
            description=(
                f"Created domain group '{group_name}' with "
                f"{len(key_attrs) + len(domain_attrs)} columns "
                f"({len(key_attrs)} key + {len(domain_attrs)} domain)"
            ),
            rationale=(
                f"Parent entity had {len(attrs)} columns > "
                f"threshold={model.config.column_threshold_for_vertical_split}; "
                f"split by domain_group"
            ),
        )


def select_partition_columns(
    model: LogicalModel, output: PhysicalModel,
    overrides: "OverrideRegistry | None" = None,
) -> None:
    """Select horizontal partition columns for each physical entity.

    Strategy:
    1. If query_patterns available: pick most common filter attribute with
       cardinality < max_partition_cardinality
    2. Otherwise: look for date/timestamp columns in primary keys
    3. Fallback: no partitioning (with warning)
    """
    from spark_pdm_generator.engine.utils import OverrideRegistry

    max_cardinality = model.config.max_partition_cardinality
    forced_partitions = overrides.forced_partitions if overrides else {}

    for phys_entity in output.physical_entities:
        name = phys_entity.physical_entity_name

        # Apply override if present
        if name in forced_partitions:
            phys_entity.partition_columns = forced_partitions[name]
            output.add_log_entry(
                rule=TransformationRule.OVERRIDE_APPLIED,
                source_entity=name,
                target_entity=name,
                description=f"Partition columns set to {forced_partitions[name]}",
                rationale="User override applied via FORCE_PARTITION_COL",
            )
            _mark_partition_attributes(output, name, forced_partitions[name])
            continue

        # Strategy 1: Query patterns
        partition_col = _select_from_query_patterns(
            model, output, phys_entity, max_cardinality
        )

        # Strategy 2: Date columns
        if not partition_col:
            partition_col = _select_date_column(model, output, phys_entity)

        if partition_col:
            phys_entity.partition_columns = [partition_col]
            output.add_log_entry(
                rule=TransformationRule.PARTITION_SELECT,
                source_entity=name,
                target_entity=name,
                description=f"Selected '{partition_col}' as partition column",
                rationale=_partition_rationale(model, output, phys_entity, partition_col),
            )
            _mark_partition_attributes(output, name, [partition_col])
        else:
            output.add_warning(
                level=WarningLevel.WARNING,
                entity=name,
                message="No suitable partition column found.",
                recommendation=(
                    "Add a date column or provide query_patterns with filter attributes. "
                    "Unpartitioned tables may have poor query performance."
                ),
            )


def _select_from_query_patterns(
    model: LogicalModel,
    output: PhysicalModel,
    phys_entity: PhysicalEntity,
    max_cardinality: int,
) -> Optional[str]:
    """Select partition column from query pattern filter attributes."""
    if not model.query_patterns:
        return None

    # Collect filter attributes across all relevant query patterns
    filter_counts: Counter = Counter()
    phys_attrs = {
        a.attribute_name
        for a in output.get_attributes_for_entity(phys_entity.physical_entity_name)
    }

    for pattern in model.query_patterns:
        # Check if this pattern involves our entity
        if pattern.primary_entity in phys_entity.source_entities:
            for filter_attr in pattern.filter_attributes:
                if filter_attr in phys_attrs:
                    # Weight by priority
                    weight = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(
                        pattern.priority.value, 1
                    )
                    filter_counts[filter_attr] += weight

    # Pick the most common filter attribute with acceptable cardinality
    for attr_name, _count in filter_counts.most_common():
        # Check cardinality
        phys_attr = _find_physical_attribute(output, phys_entity.physical_entity_name, attr_name)
        if phys_attr and phys_attr.estimated_cardinality is not None:
            if phys_attr.estimated_cardinality <= max_cardinality:
                return attr_name
        else:
            # Unknown cardinality -- accept if it looks like a date column
            if is_date_like(attr_name):
                return attr_name

    return None


def _select_date_column(
    model: LogicalModel,
    output: PhysicalModel,
    phys_entity: PhysicalEntity,
) -> Optional[str]:
    """Select a date column as partition key based on naming patterns."""
    attrs = output.get_attributes_for_entity(phys_entity.physical_entity_name)
    date_candidates = []

    for attr in attrs:
        if is_date_like(attr.attribute_name):
            # Prefer columns that are part of the key
            logical_attr = find_logical_attribute(
                model, attr.source_entity, attr.source_attribute
            )
            is_key = logical_attr.is_primary_key if logical_attr else False
            date_candidates.append((attr.attribute_name, is_key))

    # Prefer key date columns
    for name, is_key in date_candidates:
        if is_key:
            return name
    # Otherwise first date column
    if date_candidates:
        return date_candidates[0][0]

    return None


def _partition_rationale(
    model: LogicalModel,
    output: PhysicalModel,
    phys_entity: PhysicalEntity,
    partition_col: str,
) -> str:
    """Build rationale string for partition column selection."""
    parts = []
    phys_attr = _find_physical_attribute(
        output, phys_entity.physical_entity_name, partition_col
    )
    if phys_attr and phys_attr.estimated_cardinality is not None:
        parts.append(f"cardinality={phys_attr.estimated_cardinality:,}")
    if model.query_patterns:
        usage_count = sum(
            1
            for p in model.query_patterns
            if partition_col in p.filter_attributes
        )
        parts.append(f"used_in_filter_by={usage_count}/{len(model.query_patterns)}_patterns")
    if is_date_like(partition_col):
        parts.append("date-like column name")
    return "; ".join(parts) if parts else "Selected as best available candidate"


def _mark_partition_attributes(
    output: PhysicalModel, entity_name: str, partition_cols: list[str]
) -> None:
    """Mark attributes as partition columns."""
    for attr in output.physical_attributes:
        if (
            attr.physical_entity_name == entity_name
            and attr.attribute_name in partition_cols
        ):
            attr.is_partition_col = True


def _find_physical_attribute(
    output: PhysicalModel, entity_name: str, attr_name: str
) -> Optional[PhysicalAttribute]:
    """Find a physical attribute by entity and attribute name."""
    for attr in output.physical_attributes:
        if (
            attr.physical_entity_name == entity_name
            and attr.attribute_name == attr_name
        ):
            return attr
    return None


