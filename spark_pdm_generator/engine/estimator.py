"""Stats estimator: fills in missing statistics from schema metadata.

Calculates record length from attribute types/sizes and applies heuristic
encoding defaults when no data distribution stats are available.

Designed to be disposable -- bypass this module when comprehensive input
statistics become available.
"""

from spark_pdm_generator.models.logical import (
    EntityType,
    LogicalModel,
)
from spark_pdm_generator.models.physical import (
    PhysicalModel,
    TransformationRule,
    WarningLevel,
)

# Storage bytes per normalized logical type
TYPE_STORAGE_BYTES: dict[str, int] = {
    "VARCHAR": 0,       # uses max_length
    "CHAR": 0,          # uses max_length
    "STRING": 0,        # uses max_length
    "TEXT": 0,          # uses max_length
    "NVARCHAR": 0,      # uses max_length
    "CLOB": 0,          # uses max_length
    "DECIMAL": 16,
    "NUMERIC": 16,
    "INTEGER": 4,
    "INT": 4,
    "SMALLINT": 2,
    "TINYINT": 1,
    "BIGINT": 8,
    "LONG": 8,
    "FLOAT": 4,
    "REAL": 4,
    "DOUBLE": 8,
    "DATE": 4,
    "TIMESTAMP": 8,
    "DATETIME": 8,
    "TIMESTAMP_NTZ": 8,
    "BOOLEAN": 1,
    "BIT": 1,
    "BINARY": 0,        # uses max_length
    "VARBINARY": 0,     # uses max_length
    "BLOB": 0,          # uses max_length
}

# Default storage bytes when type is unknown or length-based type has no length
DEFAULT_FIELD_BYTES = 256

# Heuristic row count defaults by entity type (post-classification)
ROW_COUNT_DEFAULTS: dict[EntityType, int] = {
    EntityType.FACT: 100_000_000,
    EntityType.DIMENSION: 100_000,
    EntityType.BRIDGE: 10_000_000,
    EntityType.REFERENCE: 1_000,
    EntityType.AGGREGATE: 1_000_000,
    EntityType.UNKNOWN: 100_000,
}


def estimate_record_lengths(model: LogicalModel, output: PhysicalModel) -> None:
    """Calculate estimated_record_length_bytes for entities that lack it.

    For each entity without an estimated_record_length_bytes, sums the
    max storage bytes of each attribute based on type and max_length.
    """
    for entity in model.entities:
        if entity.estimated_record_length_bytes is not None:
            continue

        attrs = model.get_attributes_for_entity(entity.entity_name)
        if not attrs:
            continue

        total_bytes = 0
        for attr in attrs:
            logical_type = attr.logical_data_type.upper()
            fixed_bytes = TYPE_STORAGE_BYTES.get(logical_type)

            if fixed_bytes is None:
                # Unknown type
                total_bytes += DEFAULT_FIELD_BYTES
            elif fixed_bytes == 0:
                # Length-dependent type (VARCHAR, BINARY, etc.)
                total_bytes += attr.max_length if attr.max_length else DEFAULT_FIELD_BYTES
            else:
                total_bytes += fixed_bytes

        entity.estimated_record_length_bytes = total_bytes

        output.add_log_entry(
            rule=TransformationRule.ESTIMATE_STATS,
            source_entity=entity.entity_name,
            target_entity=entity.entity_name,
            description=(
                f"Estimated record length = {total_bytes:,} bytes "
                f"({len(attrs)} attributes)"
            ),
            rationale="Calculated from attribute types and max lengths (schema-based estimate)",
        )


def fill_missing_row_counts(model: LogicalModel, output: PhysicalModel) -> None:
    """Fill in missing row counts using heuristic defaults by entity type.

    Should be called AFTER entity classification so entity_type is set.
    Only fills entities that have no estimated_row_count.
    """
    for entity in model.entities:
        if entity.estimated_row_count is not None:
            continue

        default_count = ROW_COUNT_DEFAULTS.get(entity.entity_type, 100_000)
        entity.estimated_row_count = default_count

        output.add_log_entry(
            rule=TransformationRule.ESTIMATE_STATS,
            source_entity=entity.entity_name,
            target_entity=entity.entity_name,
            description=(
                f"Estimated row count = {default_count:,} "
                f"(heuristic default for {entity.entity_type.value})"
            ),
            rationale=(
                f"No row count provided; using default for entity type "
                f"{entity.entity_type.value}"
            ),
        )

        output.add_warning(
            level=WarningLevel.INFO,
            entity=entity.entity_name,
            message=(
                f"Row count estimated as {default_count:,} based on "
                f"entity type {entity.entity_type.value}"
            ),
            recommendation=(
                "Provide actual row count in Entities sheet RowCount column "
                "for more accurate bucketing and denormalization decisions."
            ),
        )
