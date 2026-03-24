"""Shared parser utility functions used by both ExcelParser and LiteParser."""

from typing import Any, Optional

from spark_pdm_generator.models.logical import (
    Compression,
    Config,
    DenormalizationMode,
    ModelType,
    TargetFormat,
)


def parse_enum(value: Any, enum_class: type, default: Any) -> Any:
    """Parse a value into an enum, falling back to default."""
    if value is None:
        return default
    str_val = str(value).strip().upper()
    try:
        return enum_class(str_val)
    except ValueError:
        for member in enum_class:
            if member.name == str_val or member.value == str_val:
                return member
        return default


def parse_int(value: Any) -> Optional[int]:
    """Parse a value to int, returning None if not possible."""
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def parse_float(value: Any, default: Optional[float]) -> Optional[float]:
    """Parse a value to float."""
    if value is None:
        return default
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return default


def parse_string_list(value: str) -> list[str]:
    """Parse a comma-separated string into a list of trimmed strings."""
    if not value or value.strip() == "":
        return []
    return [s.strip() for s in value.split(",") if s.strip()]


def build_config_from_dict(d: dict[str, Any]) -> Config:
    """Build a Config from a key-value dict, applying type coercion."""
    kwargs: dict[str, Any] = {}

    str_enum_fields = {
        "model_type": ModelType,
        "target_format": TargetFormat,
        "compression": Compression,
        "denormalization_mode": DenormalizationMode,
    }
    int_fields = [
        "cluster_parallelism",
        "target_file_size_mb",
        "column_threshold_for_vertical_split",
        "small_dim_row_threshold",
        "dictionary_encoding_cardinality_threshold",
        "max_partition_cardinality",
        "row_group_size_mb",
        "default_bucket_count",
    ]

    for key, enum_class in str_enum_fields.items():
        if key in d:
            kwargs[key] = parse_enum(d[key], enum_class, None)

    for key in int_fields:
        if key in d:
            val = parse_int(d[key])
            if val is not None:
                kwargs[key] = val

    return Config(**{k: v for k, v in kwargs.items() if v is not None})
