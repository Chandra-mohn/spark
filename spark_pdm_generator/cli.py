"""CLI entry point for spark-pdm-generator."""

from pathlib import Path

import typer

from spark_pdm_generator.models.logical import LogicalModel
from spark_pdm_generator.models.physical import PhysicalModel
from spark_pdm_generator.parsers.column_mapper import (
    create_default_mapping,
    load_column_mapping,
)
from spark_pdm_generator.parsers.excel_parser import ExcelParser, ParseError
from spark_pdm_generator.parsers.inspector import (
    generate_and_save_mapping,
    inspect_workbook,
)
from spark_pdm_generator.parsers.lite_parser import LiteParser, LiteParseError
from spark_pdm_generator.pipeline import run_pipeline
from spark_pdm_generator.pipeline_lite import run_lite_pipeline

def _report_attribute_coverage(
    model: LogicalModel, result: PhysicalModel
) -> None:
    """Compare logical attributes against physical attributes and report gaps."""
    # Build set of (entity, attribute) from logical model
    logical_attrs = {
        (a.entity_name, a.attribute_name) for a in model.attributes
    }

    # Build set of (source_entity, source_attribute) from physical model
    # Skip duplicated FK columns (they have source_attribute set)
    physical_sources = set()
    for pa in result.physical_attributes:
        if pa.source_entity and pa.source_attribute:
            physical_sources.add((pa.source_entity, pa.source_attribute))

    mapped = logical_attrs & physical_sources
    unmapped = logical_attrs - physical_sources
    total = len(logical_attrs)
    mapped_count = len(mapped)

    if total == 0:
        typer.echo("\n  Attribute coverage: no logical attributes to check")
        return

    pct = (mapped_count / total) * 100
    typer.echo(f"\n  Attribute coverage: {mapped_count}/{total} logical attributes mapped ({pct:.0f}%)")

    if unmapped:
        typer.echo(f"  Unmapped attributes: {len(unmapped)}")
        for entity, attr in sorted(unmapped):
            typer.echo(f"    {entity}.{attr}")


app = typer.Typer(
    name="spark-pdm",
    help="Spark Physical Data Model Generator -- transforms logical/OLTP models into Spark/Parquet-optimized physical models.",
)


@app.command()
def generate(
    input_file: Path = typer.Argument(
        ...,
        help="Path to the input Excel workbook.",
        exists=True,
        readable=True,
    ),
    output_file: Path = typer.Option(
        "output/physical_model.xlsx",
        "--output", "-o",
        help="Path for the output Excel workbook.",
    ),
    output_dir: Path = typer.Option(
        "output",
        "--output-dir", "-d",
        help="Directory for DDL and ETL output files.",
    ),
    mapping_file: Path = typer.Option(
        None,
        "--mapping", "-m",
        help="Path to column mapping JSON file. If omitted, uses default internal names.",
    ),
) -> None:
    """Generate a Spark/Parquet physical data model from a logical model."""
    # Load column mapping
    if mapping_file:
        if not mapping_file.exists():
            typer.echo(
                f"ERROR: Mapping file '{mapping_file}' not found.",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.echo(f"Loading column mapping from: {mapping_file}")
        try:
            mapping = load_column_mapping(mapping_file)
        except (ValueError, Exception) as e:
            typer.echo(f"ERROR: Failed to load mapping file: {e}", err=True)
            raise typer.Exit(code=1)
    else:
        mapping = create_default_mapping()

    # Parse input
    typer.echo(f"Parsing input workbook: {input_file}")
    parser = ExcelParser(mapping=mapping)
    try:
        model = parser.parse(input_file)
    except ParseError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)

    if parser.warnings:
        for w in parser.warnings:
            typer.echo(f"  Warning: {w}")

    # Summary
    typer.echo(f"  Entities:      {len(model.entities)}")
    typer.echo(f"  Attributes:    {len(model.attributes)}")
    typer.echo(f"  Relationships: {len(model.relationships)}")
    typer.echo(f"  Distributions: {len(model.data_distributions)}")
    typer.echo(f"  Query patterns:{len(model.query_patterns)}")
    typer.echo(f"  Overrides:     {len(model.rule_overrides)}")

    # Show active config
    cfg = model.config
    typer.echo(f"\n  Active configuration:")
    typer.echo(f"    target_format:                        {cfg.target_format.value}")
    typer.echo(f"    denormalization_mode:                  {cfg.denormalization_mode.value}")
    typer.echo(f"    compression:                          {cfg.compression.value}")
    typer.echo(f"    cluster_parallelism:                  {cfg.cluster_parallelism}")
    typer.echo(f"    target_file_size_mb:                  {cfg.target_file_size_mb}")
    typer.echo(f"    column_threshold_for_vertical_split:  {cfg.column_threshold_for_vertical_split}")
    typer.echo(f"    small_dim_row_threshold:              {cfg.small_dim_row_threshold}")
    typer.echo(f"    row_group_size_mb:                    {cfg.row_group_size_mb}")
    typer.echo(f"    default_bucket_count:                 {cfg.default_bucket_count}")
    typer.echo(f"    max_partition_cardinality:            {cfg.max_partition_cardinality}")
    typer.echo(f"    dictionary_encoding_cardinality_threshold: {cfg.dictionary_encoding_cardinality_threshold}")

    # Run pipeline
    typer.echo("\nRunning transformation pipeline...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = run_pipeline(model, output_file, output_dir)
    except Exception as e:
        typer.echo(f"ERROR: Pipeline failed: {e}", err=True)
        raise typer.Exit(code=1)

    # Report results
    typer.echo(f"\nResults:")
    typer.echo(f"  Physical entities:      {len(result.physical_entities)}")
    typer.echo(f"  Physical attributes:    {len(result.physical_attributes)}")
    typer.echo(f"  Physical relationships: {len(result.physical_relationships)}")
    typer.echo(f"  Transformation log:     {len(result.transformation_log)} decisions")
    typer.echo(f"  Warnings:               {len(result.warnings)}")

    _report_attribute_coverage(model, result)

    typer.echo(f"\nOutput written to:")
    typer.echo(f"  Excel:   {output_file}")
    typer.echo(f"  DDL:     {output_dir}/ddl/")
    typer.echo(f"  ETL SQL: {output_dir}/etl/sql/")
    typer.echo(f"  ETL Py:  {output_dir}/etl/pyspark/")
    typer.echo(f"  Diagram: {output_dir}/physical_model.drawio")


@app.command("generate-lite")
def generate_lite(
    input_file: Path = typer.Argument(
        ...,
        help="Path to the lite input Excel workbook (Entities, Attributes, Relationships sheets).",
        exists=True,
        readable=True,
    ),
    output_file: Path = typer.Option(
        "output/physical_model.xlsx",
        "--output", "-o",
        help="Path for the output Excel workbook.",
    ),
    output_dir: Path = typer.Option(
        "output",
        "--output-dir", "-d",
        help="Directory for DDL output files.",
    ),
    flip_composition: bool = typer.Option(
        False,
        "--flip-composition",
        help="Swap parent/child on composition relationships. Use when the input "
        "data has the subdocument as ParentEntity and the collection as ChildEntity.",
    ),
) -> None:
    """Generate a Spark/Parquet PDM from a minimal 3-sheet workbook.

    Reads Entities (with optional RowCount), Attributes, and Relationships
    sheets. Estimates record lengths from schema metadata and applies
    heuristic defaults for missing statistics.
    """
    if flip_composition:
        typer.echo("  --flip-composition: will swap parent/child on composition relationships")
    typer.echo(f"Parsing lite input workbook: {input_file}")
    parser = LiteParser(flip_composition=flip_composition)
    try:
        model = parser.parse(input_file)
    except LiteParseError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)

    if parser.warnings:
        for w in parser.warnings:
            typer.echo(f"  Warning: {w}")

    # Summary
    typer.echo(f"  Entities:      {len(model.entities)}")
    typer.echo(f"  Attributes:    {len(model.attributes)}")
    typer.echo(f"  Relationships: {len(model.relationships)}")

    # Show active config
    cfg = model.config
    typer.echo(f"\n  Active configuration:")
    typer.echo(f"    target_format:                        {cfg.target_format.value}")
    typer.echo(f"    denormalization_mode:                  {cfg.denormalization_mode.value}")
    typer.echo(f"    compression:                          {cfg.compression.value}")
    typer.echo(f"    cluster_parallelism:                  {cfg.cluster_parallelism}")
    typer.echo(f"    target_file_size_mb:                  {cfg.target_file_size_mb}")
    typer.echo(f"    column_threshold_for_vertical_split:  {cfg.column_threshold_for_vertical_split}")
    typer.echo(f"    small_dim_row_threshold:              {cfg.small_dim_row_threshold}")
    typer.echo(f"    row_group_size_mb:                    {cfg.row_group_size_mb}")
    typer.echo(f"    default_bucket_count:                 {cfg.default_bucket_count}")
    typer.echo(f"    max_partition_cardinality:            {cfg.max_partition_cardinality}")
    typer.echo(f"    dictionary_encoding_cardinality_threshold: {cfg.dictionary_encoding_cardinality_threshold}")

    # Run lite pipeline
    typer.echo("\nRunning lite transformation pipeline...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = run_lite_pipeline(model, output_file, output_dir)
    except Exception as e:
        typer.echo(f"ERROR: Lite pipeline failed: {e}", err=True)
        raise typer.Exit(code=1)

    # Report results
    typer.echo(f"\nResults:")
    typer.echo(f"  Physical entities:      {len(result.physical_entities)}")
    typer.echo(f"  Physical attributes:    {len(result.physical_attributes)}")
    typer.echo(f"  Physical relationships: {len(result.physical_relationships)}")
    typer.echo(f"  Transformation log:     {len(result.transformation_log)} decisions")
    typer.echo(f"  Warnings:               {len(result.warnings)}")

    _report_attribute_coverage(model, result)

    typer.echo(f"\nOutput written to:")
    typer.echo(f"  Excel:   {output_file}")
    typer.echo(f"  DDL:     {output_dir}/ddl/")
    typer.echo(f"  ETL SQL: {output_dir}/etl/sql/")
    typer.echo(f"  ETL Py:  {output_dir}/etl/pyspark/")
    typer.echo(f"  Diagram: {output_dir}/physical_model.drawio")


@app.command("inspect")
def inspect_cmd(
    input_file: Path = typer.Argument(
        ...,
        help="Path to the Excel workbook to inspect.",
        exists=True,
        readable=True,
    ),
    output_mapping: Path = typer.Option(
        "column_map.json",
        "--output", "-o",
        help="Path for the generated mapping template JSON.",
    ),
) -> None:
    """Inspect a workbook and generate a column mapping template."""
    typer.echo(f"Inspecting workbook: {input_file}")
    sheet_headers = generate_and_save_mapping(input_file, output_mapping)

    typer.echo(f"\nFound {len(sheet_headers)} sheets:\n")
    for sheet_name, headers in sheet_headers.items():
        typer.echo(f'  Sheet "{sheet_name}":')
        for i, header in enumerate(headers, 1):
            typer.echo(f"    [{i}] {header}")
        typer.echo("")

    typer.echo(f"Generated mapping template: {output_mapping}")
    typer.echo("Review and adjust the mappings, then run:")
    typer.echo(f"  spark-pdm generate {input_file} --mapping {output_mapping}")


if __name__ == "__main__":
    app()
