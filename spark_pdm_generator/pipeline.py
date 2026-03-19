"""Main pipeline: orchestrates all transformation phases."""

from pathlib import Path

from spark_pdm_generator.emitters.ddl_builder import emit_ddl_files
from spark_pdm_generator.emitters.diagram_emitter import emit_diagram
from spark_pdm_generator.emitters.etl_builder import emit_etl_files
from spark_pdm_generator.emitters.excel_emitter import emit_excel
from spark_pdm_generator.engine.classifier import classify_entities, validate_classifications
from spark_pdm_generator.engine.denormalizer import (
    apply_denormalization,
    build_denormalization_plan,
)
from spark_pdm_generator.engine.optimizer import (
    apply_type_mapping,
    build_physical_relationships,
    calculate_bucketing,
    generate_spark_config,
    select_sort_keys,
)
from spark_pdm_generator.engine.overrides import apply_remaining_overrides
from spark_pdm_generator.engine.partitioner import (
    apply_vertical_split,
    select_partition_columns,
)
from spark_pdm_generator.engine.utils import OverrideRegistry
from spark_pdm_generator.models.graph import ERGraph
from spark_pdm_generator.models.logical import LogicalModel, TargetFormat
from spark_pdm_generator.models.physical import PhysicalModel


def run_pipeline(
    model: LogicalModel,
    output_excel_path: Path,
    output_dir: Path,
) -> PhysicalModel:
    """Execute the full transformation pipeline.

    Phases:
    1. Build ER graph
    2. Classify entities
    3. Denormalize (plan + apply)
    4. Vertical split wide entities
    5. Select partition columns
    6. Calculate bucketing
    7. Select sort keys
    8. Map types and encoding
    9. Build physical relationships
    10. Apply remaining overrides
    11. Generate Spark config
    12. Emit outputs

    Args:
        model: Parsed logical model (input).
        output_excel_path: Where to write the output Excel workbook.
        output_dir: Directory for DDL and ETL files.

    Returns:
        The populated PhysicalModel.
    """
    if not model.entities:
        raise ValueError("LogicalModel has no entities. Cannot run pipeline.")

    output = PhysicalModel()
    overrides = OverrideRegistry(model.rule_overrides)

    # Phase 1: Build ER graph
    graph = ERGraph.from_logical_model(model)

    # Phase 2: Classify entities
    classify_entities(model, graph, output)
    validate_classifications(model, graph, output)

    # Phase 3: Denormalize
    plan = build_denormalization_plan(model, graph, output, overrides)
    apply_denormalization(model, graph, plan, output)

    # Phase 4: Vertical split
    apply_vertical_split(model, output, overrides)

    # Phase 5: Select partition columns
    select_partition_columns(model, output, overrides)

    # Phase 6: Calculate bucketing
    calculate_bucketing(model, output, overrides)

    # Phase 7: Select sort keys
    select_sort_keys(model, output)

    # Phase 8: Map types and encoding
    apply_type_mapping(model, output)

    # Phase 9: Build physical relationships
    build_physical_relationships(model, output)

    # Phase 10: Apply remaining overrides
    apply_remaining_overrides(output, overrides)

    # Phase 11: Generate Spark config
    generate_spark_config(model, output)

    # Phase 12: Emit outputs
    output_dir.mkdir(parents=True, exist_ok=True)
    emit_excel(output, output_excel_path)

    include_iceberg = model.config.target_format in (
        TargetFormat.ICEBERG,
        TargetFormat.BOTH,
    )
    emit_ddl_files(output, output_dir, include_iceberg=include_iceberg)
    emit_etl_files(output, output_dir, logical_model=model)

    # Diagram
    diagram_path = output_dir / "physical_model.svg"
    domain_name = model.entities[0].domain if model.entities else ""
    emit_diagram(
        output, diagram_path,
        domain_name=domain_name,
        denormalization_mode=model.config.denormalization_mode.value,
    )

    return output
