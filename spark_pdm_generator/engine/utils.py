"""Shared utility functions for the engine modules."""

import re
from typing import Optional

from spark_pdm_generator.models.logical import Attribute, LogicalModel, RuleOverride
from spark_pdm_generator.rules import defaults


class OverrideRegistry:
    """Pre-parsed override lookup -- built once in pipeline, shared by all phases."""

    # Override types handled by apply_remaining_overrides
    _KNOWN_REMAINING_TYPES = {
        defaults.OVERRIDE_FORCE_COMPRESSION,
        defaults.OVERRIDE_FORCE_ENCODING,
        defaults.OVERRIDE_FORCE_ROW_GROUP_SIZE,
    }

    def __init__(self, rule_overrides: list[RuleOverride]) -> None:
        self.blocked_denorm: set[str] = set()
        self.force_domain_groups: dict[str, str] = {}
        self.forced_partitions: dict[str, list[str]] = {}
        self.forced_bucket_count: Optional[int] = None
        self.bucket_count_error: Optional[str] = None
        self.remaining: list[RuleOverride] = []
        self.unknown_overrides: list[RuleOverride] = []

        for ov in rule_overrides:
            otype = ov.override_type.upper()
            if otype == defaults.OVERRIDE_BLOCK_DENORMALIZE:
                self.blocked_denorm.add(ov.target.strip())
            elif otype == defaults.OVERRIDE_FORCE_DOMAIN_GROUP:
                for attr in (a.strip() for a in ov.target.split(",")):
                    self.force_domain_groups[attr] = ov.instruction.strip()
            elif otype == defaults.OVERRIDE_FORCE_PARTITION_COL:
                target = ov.target.strip()
                cols = [c.strip() for c in ov.instruction.split(",")]
                self.forced_partitions[target] = cols
            elif otype == defaults.OVERRIDE_FORCE_BUCKET_COUNT:
                try:
                    self.forced_bucket_count = int(ov.instruction.strip())
                except (ValueError, TypeError):
                    self.bucket_count_error = (
                        f"Invalid FORCE_BUCKET_COUNT value: '{ov.instruction}'. "
                        f"Expected an integer. Override ignored."
                    )
            elif otype in self._KNOWN_REMAINING_TYPES:
                self.remaining.append(ov)
            else:
                # Track truly unknown override types separately
                self.unknown_overrides.append(ov)
                self.remaining.append(ov)


def is_date_like(name: str) -> bool:
    """Check if a column name suggests a date/timestamp.

    Uses comprehensive pattern matching for common date column naming
    conventions across domains.
    """
    lower = name.lower()
    date_patterns = [
        "date", "report_date", "reporting_date", "created_at", "updated_at",
        "timestamp", "event_date", "transaction_date", "effective_date",
        "as_of_date", "snapshot_date", "process_date", "batch_date",
    ]
    for pattern in date_patterns:
        if pattern in lower:
            return True
    return lower.endswith("_dt") or lower.endswith("_date") or lower.endswith("_ts")


def is_id_like(name: str) -> bool:
    """Check if a column name suggests an ID/key column."""
    lower = name.lower()
    return lower.endswith("_id") or lower.endswith("_key") or lower == "id"


def sanitize_name(name: str) -> str:
    """Sanitize a name for use as a SQL identifier.

    Replaces spaces and hyphens with underscores, collapses multiple
    underscores, and strips leading/trailing underscores.
    Raises ValueError if the result is empty.
    """
    if not name or not name.strip():
        raise ValueError("Cannot sanitize empty or whitespace-only name")
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def find_logical_attribute(
    model: LogicalModel, entity_name: str, attr_name: str
) -> Optional[Attribute]:
    """Find a logical attribute by entity and attribute name."""
    for attr in model.attributes:
        if attr.entity_name == entity_name and attr.attribute_name == attr_name:
            return attr
    return None
