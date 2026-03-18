"""Optimizer: calculates bucket counts, sort keys, type mappings, and encoding."""

import math
from collections import Counter
from typing import Optional

from spark_pdm_generator.engine.utils import find_logical_attribute, is_date_like, is_id_like
from spark_pdm_generator.models.logical import Attribute, Config, LogicalModel, TargetFormat
from spark_pdm_generator.models.physical import (
    JoinCost,
    JoinStrategy,
    ParquetEncoding,
    PhysicalAttribute,
    PhysicalEntity,
    PhysicalModel,
    PhysicalRelationship,
    SortColumn,
    SparkConfigEntry,
    TransformationRule,
    WarningLevel,
)
from spark_pdm_generator.rules import defaults


def calculate_bucketing(
    model: LogicalModel, output: PhysicalModel,
    overrides: "OverrideRegistry | None" = None,
) -> None:
    """Calculate bucket column and bucket count for each physical entity.

    Strategy:
    - Bucket column: primary key column (first non-partition PK column)
    - Bucket count: ceil(estimated_total_bytes / target_file_size_bytes)
    - All co-joined entities share the same bucket count
    """
    from spark_pdm_generator.engine.utils import OverrideRegistry

    config = model.config
    target_file_bytes = config.target_file_size_mb * 1024 * 1024

    forced_bucket_count = overrides.forced_bucket_count if overrides else None
    if overrides and overrides.bucket_count_error:
        output.add_warning(
            level=WarningLevel.WARNING,
            message=overrides.bucket_count_error,
        )

    # Calculate bucket count per entity
    bucket_counts: dict[str, int] = {}
    for phys_entity in output.physical_entities:
        name = phys_entity.physical_entity_name

        # Select bucket column
        bucket_col = _select_bucket_column(model, output, phys_entity)
        if bucket_col:
            phys_entity.bucket_column = bucket_col
            _mark_bucket_attribute(output, name, bucket_col)

        # Calculate bucket count
        if forced_bucket_count is not None:
            count = forced_bucket_count
            output.add_log_entry(
                rule=TransformationRule.OVERRIDE_APPLIED,
                source_entity=name,
                target_entity=name,
                description=f"Bucket count set to {count}",
                rationale="User override applied via FORCE_BUCKET_COUNT",
            )
        else:
            count = _calculate_bucket_count(phys_entity, config, target_file_bytes, model)
            output.add_log_entry(
                rule=TransformationRule.BUCKET_CALCULATE,
                source_entity=name,
                target_entity=name,
                description=f"Bucket count = {count}, bucket_column = '{bucket_col or 'none'}'",
                rationale=_bucket_rationale(phys_entity, count, target_file_bytes),
            )

        phys_entity.bucket_count = count
        bucket_counts[name] = count

        # Estimate file count and size
        if phys_entity.estimated_row_count and phys_entity.estimated_size_bytes:
            phys_entity.estimated_file_count = count
        elif phys_entity.estimated_row_count:
            avg_row = defaults.DEFAULT_AVG_ROW_SIZE_BYTES
            total_bytes = phys_entity.estimated_row_count * avg_row
            phys_entity.estimated_size_bytes = total_bytes
            phys_entity.estimated_file_count = count


def select_sort_keys(model: LogicalModel, output: PhysicalModel) -> None:
    """Select sort keys for each physical entity.

    Strategy:
    1. Primary sort: bucket column (for join efficiency)
    2. Secondary sort: most common filter/group-by column from query patterns
    """
    for phys_entity in output.physical_entities:
        name = phys_entity.physical_entity_name
        sort_cols: list[SortColumn] = []

        # Primary sort: bucket column
        if phys_entity.bucket_column:
            sort_cols.append(SortColumn(column_name=phys_entity.bucket_column))

        # Secondary sort from query patterns
        secondary = _select_secondary_sort(model, output, phys_entity)
        if secondary and secondary != phys_entity.bucket_column:
            sort_cols.append(SortColumn(column_name=secondary))

        phys_entity.sort_columns = sort_cols

        # Mark sort attributes
        for i, sc in enumerate(sort_cols):
            for attr in output.physical_attributes:
                if (
                    attr.physical_entity_name == name
                    and attr.attribute_name == sc.column_name
                ):
                    attr.is_sort_col = True
                    attr.sort_order = sc.order

        if sort_cols:
            output.add_log_entry(
                rule=TransformationRule.SORT_KEY_SELECT,
                source_entity=name,
                target_entity=name,
                description=(
                    f"Sort keys: "
                    + ", ".join(f"{sc.column_name} {sc.order.value}" for sc in sort_cols)
                ),
                rationale="Primary=bucket_column for join efficiency; secondary=top filter/group-by column",
            )


def apply_type_mapping(model: LogicalModel, output: PhysicalModel) -> None:
    """Map logical data types to Parquet physical types and select encoding."""
    config = model.config
    encoding_threshold = config.dictionary_encoding_cardinality_threshold

    for attr in output.physical_attributes:
        # Find the source logical attribute
        logical_attr = find_logical_attribute(model, attr.source_entity, attr.source_attribute)
        if not logical_attr:
            continue

        # Map type
        logical_type_upper = logical_attr.logical_data_type.upper()
        type_entry = defaults.TYPE_MAP.get(logical_type_upper)

        if type_entry:
            attr.parquet_type = type_entry[0]
            attr.logical_type = type_entry[1]

            # Handle DECIMAL precision
            if logical_type_upper in ("DECIMAL", "NUMERIC"):
                precision = logical_attr.precision or 18
                scale = logical_attr.scale or 2
                attr.logical_type = f"DECIMAL({precision},{scale})"
        else:
            attr.parquet_type = "BINARY"
            attr.logical_type = "STRING"
            attr.notes = f"Unknown logical type '{logical_attr.logical_data_type}' mapped to STRING"

        # Select encoding
        attr.encoding = _select_encoding(attr, logical_attr, encoding_threshold)

        output.add_log_entry(
            rule=TransformationRule.TYPE_OPTIMIZE,
            source_entity=attr.physical_entity_name,
            target_entity=attr.physical_entity_name,
            description=(
                f"'{attr.attribute_name}': "
                f"{logical_attr.logical_data_type} -> "
                f"{attr.parquet_type}/{attr.logical_type}, "
                f"encoding={attr.encoding.value}"
            ),
            rationale=_type_rationale(attr, logical_attr, encoding_threshold),
        )


def build_physical_relationships(
    model: LogicalModel, output: PhysicalModel
) -> None:
    """Create physical relationships between non-absorbed entities.

    Determines join strategy based on:
    - Co-bucketed? -> BUCKET_JOIN (no shuffle)
    - Small dimension? -> BROADCAST_JOIN
    - Otherwise -> SORT_MERGE_JOIN
    """
    config = model.config

    # Build map of logical entity -> physical entity name
    entity_to_physical: dict[str, str] = {}
    for phys_entity in output.physical_entities:
        for source in phys_entity.source_entities:
            entity_to_physical[source] = phys_entity.physical_entity_name

    for rel in model.relationships:
        parent_phys = entity_to_physical.get(rel.parent_entity)
        child_phys = entity_to_physical.get(rel.child_entity)

        # Skip if both ended up in the same physical table (denormalized)
        if not parent_phys or not child_phys:
            continue
        if parent_phys == child_phys:
            continue

        parent_entity = output.get_entity(parent_phys)
        child_entity = output.get_entity(child_phys)
        if not parent_entity or not child_entity:
            continue

        # Determine join strategy
        co_bucketed = (
            parent_entity.bucket_column is not None
            and child_entity.bucket_column is not None
            and parent_entity.bucket_count == child_entity.bucket_count
        )
        co_partitioned = parent_entity.partition_columns == child_entity.partition_columns

        parent_row_count = parent_entity.estimated_row_count
        is_small = (
            parent_row_count is not None
            and parent_row_count < config.small_dim_row_threshold
        )

        if co_bucketed:
            join_type = JoinStrategy.BUCKET_JOIN
            cost = JoinCost.LOW
        elif is_small:
            join_type = JoinStrategy.BROADCAST_JOIN
            cost = JoinCost.LOW
        else:
            join_type = JoinStrategy.SORT_MERGE_JOIN
            cost = JoinCost.HIGH

        phys_rel = PhysicalRelationship(
            parent_physical_entity=parent_phys,
            child_physical_entity=child_phys,
            join_type=join_type,
            join_columns=rel.child_key_columns,
            co_partitioned=co_partitioned,
            co_bucketed=co_bucketed,
            estimated_join_cost=cost,
            notes=f"From logical relationship: {rel.parent_entity} -> {rel.child_entity} ({rel.cardinality.value})",
        )
        output.physical_relationships.append(phys_rel)


def generate_spark_config(model: LogicalModel, output: PhysicalModel) -> None:
    """Generate recommended Spark configuration settings."""
    config = model.config
    configs = [
        SparkConfigEntry(
            config_key="spark.sql.parquet.compression.codec",
            config_value=config.compression.value.lower(),
            description="Parquet compression codec",
        ),
        SparkConfigEntry(
            config_key="spark.sql.sources.bucketing.enabled",
            config_value="true",
            description="Enable bucket-aware query optimization",
        ),
        SparkConfigEntry(
            config_key="spark.sql.sources.bucketing.autoBucketedScan.enabled",
            config_value="true",
            description="Enable automatic bucket scan optimization",
        ),
        SparkConfigEntry(
            config_key="spark.sql.adaptive.enabled",
            config_value="true",
            description="Enable Adaptive Query Execution for dynamic optimization",
        ),
        SparkConfigEntry(
            config_key="spark.sql.parquet.filterPushdown",
            config_value="true",
            description="Enable predicate pushdown for Parquet files",
        ),
        SparkConfigEntry(
            config_key="spark.sql.files.maxPartitionBytes",
            config_value=f"{config.row_group_size_mb}m",
            description="Max bytes per partition when reading files",
        ),
        SparkConfigEntry(
            config_key="spark.sql.parquet.enableVectorizedReader",
            config_value="true",
            description="Enable vectorized Parquet reader for better performance",
        ),
    ]

    # Add Iceberg configs if needed
    if config.target_format in (TargetFormat.ICEBERG, TargetFormat.BOTH):
        configs.extend([
            SparkConfigEntry(
                config_key="spark.sql.catalog.spark_catalog",
                config_value="org.apache.iceberg.spark.SparkSessionCatalog",
                description="Iceberg catalog implementation",
            ),
            SparkConfigEntry(
                config_key="spark.sql.catalog.spark_catalog.type",
                config_value="hive",
                description="Iceberg catalog type",
            ),
        ])

    output.spark_config = configs


# --- Helper Functions ---


def _select_bucket_column(
    model: LogicalModel, output: PhysicalModel, phys_entity: PhysicalEntity
) -> Optional[str]:
    """Select the best bucket column for an entity."""
    attrs = output.get_attributes_for_entity(phys_entity.physical_entity_name)

    # Find primary key columns that are not partition columns
    # Check against logical model since physical attributes don't track PK status
    pk_names = set()
    for source in phys_entity.source_entities:
        for la in model.get_primary_keys(source):
            pk_names.add(la.attribute_name)
            # Also check prefixed name (absorbed entities get prefixed)
            pk_names.add(f"{source}_{la.attribute_name}")

    pk_attrs = [
        a
        for a in attrs
        if a.attribute_name in pk_names and not a.is_partition_col
    ]

    # Prefer non-date PK columns for bucketing
    for attr in pk_attrs:
        if not is_date_like(attr.attribute_name):
            return attr.attribute_name

    # Fall back to first PK column
    if pk_attrs:
        return pk_attrs[0].attribute_name

    # Fall back to first non-partition column that looks like an ID
    for attr in attrs:
        if not attr.is_partition_col and is_id_like(attr.attribute_name):
            return attr.attribute_name

    return None


def _calculate_bucket_count(
    phys_entity: PhysicalEntity,
    config: Config,
    target_file_bytes: int,
    model: Optional[LogicalModel] = None,
) -> int:
    """Calculate optimal bucket count.

    Uses the best available size estimate:
    1. PhysicalEntity.estimated_size_bytes (if set directly)
    2. row_count * estimated_record_length_bytes (from source entity)
    3. row_count * DEFAULT_AVG_ROW_SIZE_BYTES (fallback heuristic)
    """
    row_count = phys_entity.estimated_row_count
    if row_count:
        # Estimate total size using best available data
        if phys_entity.estimated_size_bytes:
            total_bytes = phys_entity.estimated_size_bytes
        else:
            avg_row_bytes = _estimate_record_length(phys_entity, model)
            total_bytes = row_count * avg_row_bytes
            phys_entity.estimated_size_bytes = total_bytes

        raw_count = math.ceil(total_bytes / target_file_bytes)
        # Round up to nearest power of 2 for even distribution
        count = _next_power_of_2(raw_count)
        # Clamp to reasonable range
        count = max(defaults.MIN_BUCKET_COUNT, min(count, defaults.MAX_BUCKET_COUNT))
        # Don't exceed cluster parallelism
        count = min(count, config.cluster_parallelism)
        return count

    return config.default_bucket_count


def _estimate_record_length(
    phys_entity: PhysicalEntity,
    model: Optional[LogicalModel],
) -> int:
    """Estimate average record length in bytes.

    Checks source entities for estimated_record_length_bytes,
    falls back to DEFAULT_AVG_ROW_SIZE_BYTES.
    """
    if not model:
        return defaults.DEFAULT_AVG_ROW_SIZE_BYTES

    # Use the max record length from any source entity that specifies it
    max_record_len = 0
    for source_name in phys_entity.source_entities:
        entity = model.get_entity(source_name)
        if entity and entity.estimated_record_length_bytes:
            max_record_len = max(max_record_len, entity.estimated_record_length_bytes)

    if max_record_len > 0:
        return max_record_len

    return defaults.DEFAULT_AVG_ROW_SIZE_BYTES


def _next_power_of_2(n: int) -> int:
    """Round up to the next power of 2."""
    if n <= 0:
        return 1
    p = 1
    while p < n:
        p *= 2
    return p


def _select_secondary_sort(
    model: LogicalModel, output: PhysicalModel, phys_entity: PhysicalEntity
) -> Optional[str]:
    """Select secondary sort column from query patterns."""
    if not model.query_patterns:
        return None

    phys_attrs = {
        a.attribute_name
        for a in output.get_attributes_for_entity(phys_entity.physical_entity_name)
    }

    # Count filter + group-by usage
    col_counts: Counter = Counter()
    for pattern in model.query_patterns:
        if pattern.primary_entity in phys_entity.source_entities:
            for col in pattern.filter_attributes + pattern.group_by_attributes:
                if col in phys_attrs:
                    col_counts[col] += 1

    # Return most common that isn't already partition or bucket col
    for col, _count in col_counts.most_common():
        if col not in (phys_entity.partition_columns or []):
            if col != phys_entity.bucket_column:
                return col

    return None


def _select_encoding(
    phys_attr: PhysicalAttribute,
    logical_attr: Attribute,
    threshold: int,
) -> ParquetEncoding:
    """Select Parquet encoding for a column."""
    logical_type = logical_attr.logical_data_type.upper()

    # Boolean always RLE
    if logical_type in ("BOOLEAN", "BIT"):
        return ParquetEncoding.RLE

    # Dictionary encoding for low-cardinality columns
    if phys_attr.estimated_cardinality is not None:
        if phys_attr.estimated_cardinality <= threshold:
            return ParquetEncoding.DICTIONARY

    # String types default to dictionary (Parquet will fall back to PLAIN if needed)
    if logical_type in ("VARCHAR", "CHAR", "STRING", "TEXT", "NVARCHAR"):
        return ParquetEncoding.DICTIONARY

    # Integer types that are sorted -> delta encoding
    if logical_type in ("INTEGER", "INT", "BIGINT", "LONG") and phys_attr.is_sort_col:
        return ParquetEncoding.DELTA_BINARY_PACKED

    # Date/timestamp that are sorted -> delta encoding
    if logical_type in ("DATE", "TIMESTAMP", "DATETIME") and phys_attr.is_sort_col:
        return ParquetEncoding.DELTA_BINARY_PACKED

    return ParquetEncoding.PLAIN


def _type_rationale(
    phys_attr: PhysicalAttribute, logical_attr: Attribute, threshold: int
) -> str:
    """Build rationale for type/encoding selection."""
    parts = []
    if phys_attr.estimated_cardinality is not None:
        parts.append(f"cardinality={phys_attr.estimated_cardinality:,}")
        if phys_attr.estimated_cardinality <= threshold:
            parts.append(f"<= threshold={threshold:,} -> DICTIONARY")
    if phys_attr.is_sort_col:
        parts.append("sort_column=true -> DELTA candidate")
    if phys_attr.estimated_null_pct > 50:
        parts.append(f"null_pct={phys_attr.estimated_null_pct:.1f}% (sparse)")
    return "; ".join(parts) if parts else "Default mapping applied"


def _bucket_rationale(
    phys_entity: PhysicalEntity, count: int, target_file_bytes: int
) -> str:
    """Build rationale for bucket count calculation."""
    parts = []
    if phys_entity.estimated_row_count:
        parts.append(f"estimated_rows={phys_entity.estimated_row_count:,}")
    if phys_entity.estimated_size_bytes:
        size_mb = phys_entity.estimated_size_bytes / (1024 * 1024)
        parts.append(f"estimated_size={size_mb:,.0f}MB")
    target_mb = target_file_bytes / (1024 * 1024)
    parts.append(f"target_file_size={target_mb:,.0f}MB")
    parts.append(f"raw_count rounded to power_of_2={count}")
    return "; ".join(parts)


def _mark_bucket_attribute(
    output: PhysicalModel, entity_name: str, bucket_col: str
) -> None:
    """Mark an attribute as the bucket column."""
    for attr in output.physical_attributes:
        if (
            attr.physical_entity_name == entity_name
            and attr.attribute_name == bucket_col
        ):
            attr.is_bucket_col = True


