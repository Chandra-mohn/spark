"""Pydantic models for the logical/OLTP input data model."""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class EntityType(str, Enum):
    FACT = "FACT"
    DIMENSION = "DIMENSION"
    BRIDGE = "BRIDGE"
    REFERENCE = "REFERENCE"
    AGGREGATE = "AGGREGATE"
    UNKNOWN = "UNKNOWN"


class GrowthRate(str, Enum):
    STATIC = "STATIC"
    SLOW = "SLOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"


class UpdateFrequency(str, Enum):
    STATIC = "STATIC"
    DAILY = "DAILY"
    HOURLY = "HOURLY"
    REAL_TIME = "REAL_TIME"


class Cardinality(str, Enum):
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_MANY = "M:N"


class SkewIndicator(str, Enum):
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"


class QueryFrequency(str, Enum):
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    AD_HOC = "AD_HOC"


class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ModelType(str, Enum):
    OLTP_3NF = "OLTP_3NF"
    STAR_SCHEMA = "STAR_SCHEMA"
    SNOWFLAKE = "SNOWFLAKE"
    DATA_VAULT = "DATA_VAULT"
    LDM_FLAT = "LDM_FLAT"


class TargetFormat(str, Enum):
    PARQUET = "PARQUET"
    ICEBERG = "ICEBERG"
    BOTH = "BOTH"


class DenormalizationMode(str, Enum):
    AUTO = "AUTO"
    AGGRESSIVE = "AGGRESSIVE"
    CONSERVATIVE = "CONSERVATIVE"


class Compression(str, Enum):
    ZSTD = "ZSTD"
    SNAPPY = "SNAPPY"
    LZ4 = "LZ4"
    GZIP = "GZIP"


# --- Input Sheet Models ---


class Entity(BaseModel):
    """Represents an entity from the entities input sheet."""

    entity_name: str
    entity_type: EntityType = EntityType.UNKNOWN
    description: str = ""
    grain_description: str = ""
    domain: str = "general"
    estimated_row_count: Optional[int] = None
    estimated_record_length_bytes: Optional[int] = None
    growth_rate: GrowthRate = GrowthRate.MODERATE
    update_frequency: UpdateFrequency = UpdateFrequency.DAILY


class Attribute(BaseModel):
    """Represents an attribute from the attributes input sheet."""

    entity_name: str
    attribute_name: str
    logical_data_type: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    max_length: Optional[int] = None
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    fk_references: Optional[str] = None
    description: str = ""
    domain_group: Optional[str] = None


class Relationship(BaseModel):
    """Represents a relationship from the relationships input sheet."""

    parent_entity: str
    child_entity: str
    cardinality: Cardinality
    parent_key_columns: list[str]
    child_key_columns: list[str]
    is_identifying: bool = False
    description: str = ""

    @model_validator(mode="after")
    def _validate_key_column_counts(self) -> "Relationship":
        if (
            self.parent_key_columns
            and self.child_key_columns
            and len(self.parent_key_columns) != len(self.child_key_columns)
        ):
            raise ValueError(
                f"parent_key_columns ({len(self.parent_key_columns)}) and "
                f"child_key_columns ({len(self.child_key_columns)}) must have "
                f"the same length for relationship "
                f"{self.parent_entity} -> {self.child_entity}"
            )
        return self


class DataDistribution(BaseModel):
    """Represents a row from the data_distribution input sheet."""

    entity_name: str
    attribute_name: str
    distinct_count: Optional[int] = None
    null_percentage: float = 0.0
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    avg_length: Optional[float] = None
    skew_indicator: SkewIndicator = SkewIndicator.LOW


class QueryPattern(BaseModel):
    """Represents a row from the query_patterns input sheet."""

    pattern_name: str
    primary_entity: str
    filter_attributes: list[str] = Field(default_factory=list)
    group_by_attributes: list[str] = Field(default_factory=list)
    join_entities: list[str] = Field(default_factory=list)
    accessed_attributes: list[str] = Field(default_factory=list)
    frequency: QueryFrequency = QueryFrequency.DAILY
    priority: Priority = Priority.MEDIUM


class RuleOverride(BaseModel):
    """Represents a row from the rules_override input sheet."""

    rule_id: int
    override_type: str
    target: str
    instruction: str


class Config(BaseModel):
    """Configuration parameters from the config input sheet."""

    model_config = {"protected_namespaces": ()}

    model_type: ModelType = ModelType.OLTP_3NF
    target_format: TargetFormat = TargetFormat.PARQUET
    compression: Compression = Compression.ZSTD
    denormalization_mode: DenormalizationMode = DenormalizationMode.AUTO
    cluster_parallelism: int = 2048
    target_file_size_mb: int = 512
    column_threshold_for_vertical_split: int = 500
    small_dim_row_threshold: int = 100_000
    dictionary_encoding_cardinality_threshold: int = 10_000
    max_partition_cardinality: int = 10_000
    row_group_size_mb: int = 256
    default_bucket_count: int = 2048

    @field_validator("cluster_parallelism", "target_file_size_mb",
                     "row_group_size_mb", "default_bucket_count")
    @classmethod
    def _must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"must be > 0, got {v}")
        return v

    @model_validator(mode="after")
    def _validate_cross_field(self) -> "Config":
        if self.default_bucket_count > self.cluster_parallelism:
            raise ValueError(
                f"default_bucket_count ({self.default_bucket_count}) "
                f"must be <= cluster_parallelism ({self.cluster_parallelism})"
            )
        return self


class LogicalModel(BaseModel):
    """Complete parsed logical model -- the unified input to the engine."""

    entities: list[Entity]
    attributes: list[Attribute]
    relationships: list[Relationship]
    data_distributions: list[DataDistribution] = Field(default_factory=list)
    query_patterns: list[QueryPattern] = Field(default_factory=list)
    rule_overrides: list[RuleOverride] = Field(default_factory=list)
    config: Config = Field(default_factory=Config)

    def get_entity(self, name: str) -> Optional[Entity]:
        """Lookup entity by name."""
        for e in self.entities:
            if e.entity_name == name:
                return e
        return None

    def get_attributes_for_entity(self, entity_name: str) -> list[Attribute]:
        """Get all attributes belonging to an entity."""
        return [a for a in self.attributes if a.entity_name == entity_name]

    def get_primary_keys(self, entity_name: str) -> list[Attribute]:
        """Get primary key attributes for an entity."""
        return [
            a
            for a in self.attributes
            if a.entity_name == entity_name and a.is_primary_key
        ]

    def get_distribution(
        self, entity_name: str, attribute_name: str
    ) -> Optional[DataDistribution]:
        """Lookup distribution stats for a specific attribute."""
        for d in self.data_distributions:
            if d.entity_name == entity_name and d.attribute_name == attribute_name:
                return d
        return None

    def get_entity_row_count(self, entity_name: str) -> Optional[int]:
        """Get estimated row count for an entity."""
        entity = self.get_entity(entity_name)
        if entity and entity.estimated_row_count is not None:
            return entity.estimated_row_count
        return None

    def get_relationships_for_child(self, child_entity: str) -> list[Relationship]:
        """Get all relationships where the given entity is the child."""
        return [r for r in self.relationships if r.child_entity == child_entity]

    def get_relationships_for_parent(self, parent_entity: str) -> list[Relationship]:
        """Get all relationships where the given entity is the parent."""
        return [r for r in self.relationships if r.parent_entity == parent_entity]
