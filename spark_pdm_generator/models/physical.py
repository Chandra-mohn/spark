"""Pydantic models for the physical output data model."""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class PhysicalEntityType(str, Enum):
    FACT_TABLE = "FACT_TABLE"
    DIMENSION_TABLE = "DIMENSION_TABLE"
    BRIDGE_TABLE = "BRIDGE_TABLE"
    AGGREGATE_TABLE = "AGGREGATE_TABLE"
    DOMAIN_GROUP = "DOMAIN_GROUP"


class ParquetEncoding(str, Enum):
    DICTIONARY = "DICTIONARY"
    PLAIN = "PLAIN"
    DELTA_BINARY_PACKED = "DELTA_BINARY_PACKED"
    DELTA_LENGTH_BYTE_ARRAY = "DELTA_LENGTH_BYTE_ARRAY"
    RLE = "RLE"


class JoinStrategy(str, Enum):
    BUCKET_JOIN = "BUCKET_JOIN"
    BROADCAST_JOIN = "BROADCAST_JOIN"
    SORT_MERGE_JOIN = "SORT_MERGE_JOIN"


class JoinCost(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class SortOrder(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class WarningLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class TransformationRule(str, Enum):
    DENORMALIZE_1_TO_1 = "DENORMALIZE_1_TO_1"
    DENORMALIZE_COMPOSITION = "DENORMALIZE_COMPOSITION"
    DENORMALIZE_SMALL_DIM = "DENORMALIZE_SMALL_DIM"
    FLATTEN_HIERARCHY = "FLATTEN_HIERARCHY"
    VERTICAL_SPLIT = "VERTICAL_SPLIT"
    TYPE_OPTIMIZE = "TYPE_OPTIMIZE"
    ENCODING_SELECT = "ENCODING_SELECT"
    PARTITION_SELECT = "PARTITION_SELECT"
    BUCKET_CALCULATE = "BUCKET_CALCULATE"
    SORT_KEY_SELECT = "SORT_KEY_SELECT"
    ENTITY_CLASSIFY = "ENTITY_CLASSIFY"
    OVERRIDE_APPLIED = "OVERRIDE_APPLIED"
    KEEP_SEPARATE = "KEEP_SEPARATE"
    ESTIMATE_STATS = "ESTIMATE_STATS"


# --- Output Models ---


class SortColumn(BaseModel):
    """A column used for sorting with its order."""

    column_name: str
    order: SortOrder = SortOrder.ASC


class PhysicalEntity(BaseModel):
    """A physical table in the output model."""

    physical_entity_name: str
    source_entities: list[str] = Field(default_factory=list)
    entity_type: PhysicalEntityType
    grain_description: str = ""
    domain: str = ""
    partition_columns: list[str] = Field(default_factory=list)
    bucket_column: Optional[str] = None
    bucket_count: Optional[int] = None
    sort_columns: list[SortColumn] = Field(default_factory=list)
    estimated_row_count: Optional[int] = None
    estimated_size_bytes: Optional[int] = None
    estimated_file_count: Optional[int] = None
    row_group_size_mb: int = 256
    compression_codec: str = "ZSTD"
    denormalization_notes: str = ""
    storage_format: str = "PARQUET"


class PhysicalAttribute(BaseModel):
    """A column in a physical table."""

    physical_entity_name: str
    attribute_name: str
    source_entity: str = ""
    source_attribute: str = ""
    parquet_type: str = ""
    logical_type: str = ""
    encoding: ParquetEncoding = ParquetEncoding.PLAIN
    nullable: bool = True
    is_partition_col: bool = False
    is_bucket_col: bool = False
    is_sort_col: bool = False
    sort_order: Optional[SortOrder] = None
    estimated_cardinality: Optional[int] = None
    estimated_null_pct: float = 0.0
    notes: str = ""


class PhysicalRelationship(BaseModel):
    """A join relationship between physical tables."""

    parent_physical_entity: str
    child_physical_entity: str
    join_type: JoinStrategy = JoinStrategy.SORT_MERGE_JOIN
    join_columns: list[str] = Field(default_factory=list)
    co_partitioned: bool = False
    co_bucketed: bool = False
    estimated_join_cost: JoinCost = JoinCost.MEDIUM
    notes: str = ""


class TransformationLogEntry(BaseModel):
    """A single decision in the transformation log."""

    log_id: int
    rule_applied: TransformationRule
    source_entity: str = ""
    target_entity: str = ""
    description: str = ""
    rationale: str = ""


class Warning(BaseModel):
    """A warning or issue detected during transformation."""

    level: WarningLevel
    entity: str = ""
    attribute: str = ""
    message: str = ""
    recommendation: str = ""


class SparkConfigEntry(BaseModel):
    """A recommended Spark configuration setting."""

    config_key: str
    config_value: str
    description: str = ""


class PhysicalModel(BaseModel):
    """Complete physical model -- the output of the engine."""

    physical_entities: list[PhysicalEntity] = Field(default_factory=list)
    physical_attributes: list[PhysicalAttribute] = Field(default_factory=list)
    physical_relationships: list[PhysicalRelationship] = Field(default_factory=list)
    transformation_log: list[TransformationLogEntry] = Field(default_factory=list)
    warnings: list[Warning] = Field(default_factory=list)
    spark_config: list[SparkConfigEntry] = Field(default_factory=list)

    _log_counter: int = 0

    def add_log_entry(
        self,
        rule: TransformationRule,
        source_entity: str = "",
        target_entity: str = "",
        description: str = "",
        rationale: str = "",
    ) -> TransformationLogEntry:
        """Add a new transformation log entry with auto-incrementing ID."""
        self._log_counter += 1
        entry = TransformationLogEntry(
            log_id=self._log_counter,
            rule_applied=rule,
            source_entity=source_entity,
            target_entity=target_entity,
            description=description,
            rationale=rationale,
        )
        self.transformation_log.append(entry)
        return entry

    def add_warning(
        self,
        level: WarningLevel,
        message: str,
        entity: str = "",
        attribute: str = "",
        recommendation: str = "",
    ) -> Warning:
        """Add a warning."""
        warning = Warning(
            level=level,
            entity=entity,
            attribute=attribute,
            message=message,
            recommendation=recommendation,
        )
        self.warnings.append(warning)
        return warning

    def get_attributes_for_entity(
        self, physical_entity_name: str
    ) -> list[PhysicalAttribute]:
        """Get all attributes for a physical entity."""
        return [
            a
            for a in self.physical_attributes
            if a.physical_entity_name == physical_entity_name
        ]

    def get_entity(self, name: str) -> Optional[PhysicalEntity]:
        """Lookup physical entity by name."""
        for e in self.physical_entities:
            if e.physical_entity_name == name:
                return e
        return None
