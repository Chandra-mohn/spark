"""DDL builder: generates CREATE TABLE statements for Parquet and Iceberg."""

from pathlib import Path

from spark_pdm_generator.models.physical import (
    PhysicalAttribute,
    PhysicalEntity,
    PhysicalModel,
)


def _q(name: str) -> str:
    """Quote a SQL identifier with backticks if it contains special chars."""
    if " " in name or "-" in name or "." in name:
        return f"`{name}`"
    return name


class DDLBuilder:
    """Builds Spark SQL DDL statements using f-strings."""

    def build_create_table_parquet(
        self,
        entity: PhysicalEntity,
        attributes: list[PhysicalAttribute],
    ) -> str:
        """Generate a CREATE TABLE statement for Parquet format."""
        table_name = _q(entity.physical_entity_name)

        # Separate partition columns from regular columns
        partition_cols = set(entity.partition_columns)
        regular_attrs = [a for a in attributes if a.attribute_name not in partition_cols]
        partition_attrs = [a for a in attributes if a.attribute_name in partition_cols]

        # Column definitions
        col_defs = []
        for attr in regular_attrs:
            spark_type = _to_spark_sql_type(attr)
            null_str = "" if attr.nullable else " NOT NULL"
            col_defs.append(f"    {_q(attr.attribute_name)} {spark_type}{null_str}")

        columns_sql = ",\n".join(col_defs)

        # Build the DDL
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns_sql}\n)"

        # USING clause
        ddl += "\nUSING PARQUET"

        # Partitioning
        if partition_attrs:
            part_defs = []
            for pa in partition_attrs:
                spark_type = _to_spark_sql_type(pa)
                part_defs.append(f"{_q(pa.attribute_name)} {spark_type}")
            ddl += f"\nPARTITIONED BY ({', '.join(part_defs)})"

        # Clustering (bucketing + sorting)
        if entity.bucket_column and entity.bucket_count:
            sort_clause = ""
            if entity.sort_columns:
                sort_cols = ", ".join(
                    _q(sc.column_name) for sc in entity.sort_columns
                )
                sort_clause = f"\nSORTED BY ({sort_cols})"
            ddl += (
                f"\nCLUSTERED BY ({_q(entity.bucket_column)}) "
                f"{sort_clause}"
                f"\nINTO {entity.bucket_count} BUCKETS"
            )

        # Table properties
        props = {
            "parquet.compression": entity.compression_codec.lower(),
            "parquet.block.size": str(entity.row_group_size_mb * 1024 * 1024),
        }
        props_str = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        ddl += f"\nTBLPROPERTIES ({props_str})"

        return ddl

    def build_create_table_iceberg(
        self,
        entity: PhysicalEntity,
        attributes: list[PhysicalAttribute],
    ) -> str:
        """Generate a CREATE TABLE statement for Iceberg format."""
        table_name = _q(entity.physical_entity_name)

        # All columns including partition columns
        col_defs = []
        for attr in attributes:
            spark_type = _to_spark_sql_type(attr)
            null_str = "" if attr.nullable else " NOT NULL"
            col_defs.append(f"    {_q(attr.attribute_name)} {spark_type}{null_str}")

        columns_sql = ",\n".join(col_defs)

        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns_sql}\n)"
        ddl += "\nUSING ICEBERG"

        # Iceberg partitioning (uses transform expressions)
        if entity.partition_columns:
            part_exprs = []
            for pc in entity.partition_columns:
                # Iceberg supports identity, year, month, day, hour transforms
                attr = _find_attr(attributes, pc)
                if attr and "DATE" in attr.logical_type.upper():
                    part_exprs.append(f"days({pc})")
                elif attr and "TIMESTAMP" in attr.logical_type.upper():
                    part_exprs.append(f"days({pc})")
                else:
                    part_exprs.append(pc)
            ddl += f"\nPARTITIONED BY ({', '.join(part_exprs)})"

        # Iceberg sort order
        if entity.sort_columns:
            sort_exprs = []
            for sc in entity.sort_columns:
                sort_exprs.append(f"{sc.column_name} {sc.order.value}")
            # Note: Iceberg uses WRITE ORDERED BY or sort-order in table properties

        # Table properties
        props = {
            "write.parquet.compression-codec": entity.compression_codec.lower(),
            "write.parquet.row-group-size-bytes": str(
                entity.row_group_size_mb * 1024 * 1024
            ),
            "format-version": "2",
        }
        if entity.sort_columns:
            props["write.distribution-mode"] = "range"

        props_str = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        ddl += f"\nTBLPROPERTIES ({props_str})"

        return ddl


def emit_ddl_files(
    output: PhysicalModel, output_dir: Path, include_iceberg: bool = False
) -> None:
    """Write DDL files to the output directory."""
    builder = DDLBuilder()

    # Parquet DDLs
    parquet_dir = output_dir / "ddl" / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    for entity in output.physical_entities:
        attrs = output.get_attributes_for_entity(entity.physical_entity_name)
        ddl = builder.build_create_table_parquet(entity, attrs)
        filepath = parquet_dir / f"{entity.physical_entity_name}.sql"
        filepath.write_text(ddl + ";\n", encoding="utf-8")

    # Iceberg DDLs
    if include_iceberg:
        iceberg_dir = output_dir / "ddl" / "iceberg"
        iceberg_dir.mkdir(parents=True, exist_ok=True)

        for entity in output.physical_entities:
            attrs = output.get_attributes_for_entity(entity.physical_entity_name)
            ddl = builder.build_create_table_iceberg(entity, attrs)
            filepath = iceberg_dir / f"{entity.physical_entity_name}.sql"
            filepath.write_text(ddl + ";\n", encoding="utf-8")


def _to_spark_sql_type(attr: PhysicalAttribute) -> str:
    """Convert physical attribute to Spark SQL type string."""
    logical = attr.logical_type.upper()

    if logical.startswith("DECIMAL"):
        # Ensure bare DECIMAL gets default precision/scale
        if logical == "DECIMAL":
            return "DECIMAL(18,2)"
        return logical
    if logical == "STRING":
        return "STRING"
    if logical == "INT(32, TRUE)":
        return "INT"
    if logical == "INT(16, TRUE)":
        return "SMALLINT"
    if logical == "INT(8, TRUE)":
        return "TINYINT"
    if logical == "INT(64, TRUE)":
        return "BIGINT"
    if logical == "FLOAT":
        return "FLOAT"
    if logical == "DOUBLE":
        return "DOUBLE"
    if logical == "DATE":
        return "DATE"
    if logical in ("TIMESTAMP", "TIMESTAMP_MICROS"):
        return "TIMESTAMP"
    if logical == "BOOLEAN":
        return "BOOLEAN"
    if logical == "BINARY":
        return "BINARY"

    return "STRING"


def _find_attr(
    attributes: list[PhysicalAttribute], name: str
) -> PhysicalAttribute | None:
    """Find attribute by name."""
    for a in attributes:
        if a.attribute_name == name:
            return a
    return None
