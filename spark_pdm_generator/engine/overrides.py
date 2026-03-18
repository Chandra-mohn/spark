"""Override handler: applies rules_override entries that weren't handled inline.

Most overrides (BLOCK_DENORMALIZE, FORCE_DOMAIN_GROUP, FORCE_PARTITION_COL,
FORCE_BUCKET_COUNT) are parsed once by OverrideRegistry and consumed by
their respective engine phases.  This module handles the remaining types.
"""

from spark_pdm_generator.engine.utils import OverrideRegistry
from spark_pdm_generator.models.physical import (
    ParquetEncoding,
    PhysicalModel,
    TransformationRule,
    WarningLevel,
)
from spark_pdm_generator.rules import defaults


def apply_remaining_overrides(output: PhysicalModel, overrides: OverrideRegistry) -> None:
    """Apply override types that weren't handled by inline engine phases.

    Currently supported additional overrides:
    - FORCE_COMPRESSION: Override compression codec for a specific entity
    - FORCE_ENCODING: Override encoding for a specific attribute
    - FORCE_ROW_GROUP_SIZE: Override row group size for a specific entity
    """
    for override in overrides.remaining:
        otype = override.override_type.upper()

        if otype == defaults.OVERRIDE_FORCE_COMPRESSION:
            _apply_force_compression(override.target, override.instruction, output)
        elif otype == defaults.OVERRIDE_FORCE_ENCODING:
            _apply_force_encoding(override.target, override.instruction, output)
        elif otype == defaults.OVERRIDE_FORCE_ROW_GROUP_SIZE:
            _apply_force_row_group_size(override.target, override.instruction, output)
        else:
            output.add_warning(
                level=WarningLevel.WARNING,
                message=f"Unknown override type '{otype}' (rule_id={override.rule_id}). Ignored.",
                recommendation=f"Supported types: BLOCK_DENORMALIZE, FORCE_DOMAIN_GROUP, "
                f"FORCE_PARTITION_COL, FORCE_BUCKET_COUNT, FORCE_COMPRESSION, "
                f"FORCE_ENCODING, FORCE_ROW_GROUP_SIZE",
            )


def _apply_force_compression(target: str, instruction: str, output: PhysicalModel) -> None:
    """Override compression for a physical entity."""
    entity = output.get_entity(target)
    if entity:
        entity.compression_codec = instruction.upper()
        output.add_log_entry(
            rule=TransformationRule.OVERRIDE_APPLIED,
            source_entity=target,
            target_entity=target,
            description=f"Compression set to {instruction.upper()}",
            rationale="User override via FORCE_COMPRESSION",
        )
    else:
        output.add_warning(
            level=WarningLevel.WARNING,
            entity=target,
            message=f"FORCE_COMPRESSION target entity '{target}' not found in physical model.",
        )


def _apply_force_encoding(target: str, instruction: str, output: PhysicalModel) -> None:
    """Override encoding for specific attributes.

    Target format: 'entity_name.attribute_name' or just 'attribute_name' (all entities).
    """
    try:
        encoding = ParquetEncoding(instruction.strip().upper())
    except ValueError:
        output.add_warning(
            level=WarningLevel.WARNING,
            entity=target,
            message=(
                f"FORCE_ENCODING instruction '{instruction}' is not a valid encoding. "
                f"Valid values: {', '.join(e.value for e in ParquetEncoding)}"
            ),
        )
        return

    if "." in target:
        entity_name, attr_name = target.split(".", 1)
        for attr in output.physical_attributes:
            if (
                attr.physical_entity_name == entity_name
                and attr.attribute_name == attr_name
            ):
                attr.encoding = encoding
                output.add_log_entry(
                    rule=TransformationRule.OVERRIDE_APPLIED,
                    source_entity=entity_name,
                    target_entity=entity_name,
                    description=f"Encoding for '{attr_name}' set to {encoding.value}",
                    rationale="User override via FORCE_ENCODING",
                )
    else:
        for attr in output.physical_attributes:
            if attr.attribute_name == target:
                attr.encoding = encoding


def _apply_force_row_group_size(
    target: str, instruction: str, output: PhysicalModel
) -> None:
    """Override row group size for a physical entity."""
    entity = output.get_entity(target)
    if entity:
        try:
            entity.row_group_size_mb = int(instruction)
            output.add_log_entry(
                rule=TransformationRule.OVERRIDE_APPLIED,
                source_entity=target,
                target_entity=target,
                description=f"Row group size set to {instruction}MB",
                rationale="User override via FORCE_ROW_GROUP_SIZE",
            )
        except ValueError:
            output.add_warning(
                level=WarningLevel.WARNING,
                entity=target,
                message=f"FORCE_ROW_GROUP_SIZE instruction '{instruction}' is not a valid integer.",
            )
