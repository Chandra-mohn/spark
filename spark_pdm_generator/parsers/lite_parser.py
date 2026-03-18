"""Lite parser: reads a simplified 3-sheet workbook (Entities, Attributes, Relationships).

This parser handles input workbooks with minimal metadata -- entity names,
attributes with datatypes/sizes, primary keys, and relationships. It maps
the non-standard sheet/column names to the standard LogicalModel.

Designed to be disposable -- replace with the full ExcelParser when
comprehensive input data becomes available.
"""

import re
from pathlib import Path
from typing import Any, Optional

import openpyxl
from openpyxl.worksheet.worksheet import Worksheet

from spark_pdm_generator.models.logical import (
    Attribute,
    Cardinality,
    Compression,
    Config,
    DenormalizationMode,
    Entity,
    LogicalModel,
    ModelType,
    Relationship,
    TargetFormat,
)


class LiteParseError(Exception):
    """Raised when the lite input workbook has structural issues."""

    pass


# Datatype mapping: input Datatype -> (normalized_logical_type, default_max_length_bytes)
DATATYPE_MAP: dict[str, tuple[str, int]] = {
    "CHARACTERS": ("VARCHAR", 0),       # length from Length column
    "TEXT": ("VARCHAR", 0),             # length from Length column
    "NUMBER": ("DECIMAL", 16),          # precision from Length column
    "DECIMAL": ("DECIMAL", 16),         # precision from Length column
    "INTEGER": ("INTEGER", 4),
    "LONG INTEGER": ("BIGINT", 8),
    "DATE": ("DATE", 4),
    "TIMESTAMP": ("TIMESTAMP", 8),
    "BOOLEAN": ("BOOLEAN", 1),
    "BINARY": ("BINARY", 0),           # length from Length column
}

# Storage size in bytes for each normalized logical type
STORAGE_BYTES: dict[str, int] = {
    "VARCHAR": 0,       # uses max_length
    "DECIMAL": 16,      # Parquet fixed-length byte array
    "INTEGER": 4,
    "BIGINT": 8,
    "DATE": 4,
    "TIMESTAMP": 8,
    "BOOLEAN": 1,
    "BINARY": 0,        # uses max_length
}


class LiteParser:
    """Reads a simplified 3-sheet Excel workbook and produces a LogicalModel."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def parse(self, workbook_path: Path) -> LogicalModel:
        """Parse the lite input workbook into a LogicalModel.

        Expected sheets: Entities, Attributes, Relationships

        Args:
            workbook_path: Path to the Excel file.

        Returns:
            A populated LogicalModel.

        Raises:
            LiteParseError: If required sheets or fields are missing.
        """
        wb = openpyxl.load_workbook(workbook_path, read_only=True, data_only=True)

        try:
            entities = self._parse_entities(wb)
            attributes = self._parse_attributes(wb)
            relationships = self._parse_relationships(wb, attributes)
            config = self._parse_config(wb)
        finally:
            wb.close()

        if self.errors:
            raise LiteParseError(
                "Parsing errors:\n" + "\n".join(f"  - {e}" for e in self.errors)
            )

        return LogicalModel(
            entities=entities,
            attributes=attributes,
            relationships=relationships,
            config=config,
        )

    # ------------------------------------------------------------------
    # Entity parsing
    # ------------------------------------------------------------------

    def _parse_entities(self, wb: openpyxl.Workbook) -> list[Entity]:
        """Parse the Entities sheet."""
        ws = self._get_sheet(wb, "Entities", required=True)
        if not ws:
            return []

        rows = self._read_sheet_rows(ws)
        entities = []

        for i, row in enumerate(rows, start=2):
            entity_name = _str(row.get("EntityName"))
            if not entity_name:
                continue

            row_count = _parse_int(row.get("RowCount"))

            entity = Entity(
                entity_name=entity_name,
                description=_str(row.get("Comment", "")),
                estimated_row_count=row_count,
                domain=_str(row.get("CollectionName", "general")) or "general",
            )
            entities.append(entity)

        if not entities:
            self.errors.append("Entities sheet is empty or has no valid rows.")

        return entities

    # ------------------------------------------------------------------
    # Attribute parsing
    # ------------------------------------------------------------------

    def _parse_attributes(self, wb: openpyxl.Workbook) -> list[Attribute]:
        """Parse the Attributes sheet."""
        ws = self._get_sheet(wb, "Attributes", required=True)
        if not ws:
            return []

        rows = self._read_sheet_rows(ws)
        attributes = []

        for i, row in enumerate(rows, start=2):
            entity_name = _str(row.get("EntityName"))
            attr_code = _str(row.get("Code"))
            raw_datatype = _str(row.get("Datatype", "")).strip().upper()
            length = _parse_int(row.get("Length"))

            if not entity_name or not attr_code:
                continue

            if not raw_datatype:
                self.warnings.append(
                    f"Attributes row {i}: missing Datatype for "
                    f"'{entity_name}.{attr_code}', defaulting to VARCHAR"
                )
                raw_datatype = "CHARACTERS"

            # Map datatype
            type_info = DATATYPE_MAP.get(raw_datatype)
            if not type_info:
                self.warnings.append(
                    f"Attributes row {i}: unknown Datatype '{raw_datatype}' "
                    f"for '{entity_name}.{attr_code}', defaulting to VARCHAR"
                )
                type_info = ("VARCHAR", 0)

            logical_type, default_bytes = type_info

            # Determine precision/scale for numeric types
            precision: Optional[int] = None
            scale: Optional[int] = None
            max_length: Optional[int] = None

            if logical_type == "DECIMAL":
                precision = length if length else 18
                # Number -> scale=0 (whole number), Decimal -> scale=2 (financial)
                if raw_datatype == "NUMBER":
                    scale = 0
                else:
                    scale = 2
            elif logical_type in ("VARCHAR", "BINARY"):
                max_length = length if length else 256
            # INTEGER, BIGINT, DATE, TIMESTAMP, BOOLEAN -- no length needed

            # Parse flags
            is_pk = _str(row.get("PrimaryIdentifierFlag", "")).strip().upper() == "Y"
            is_fk = _str(row.get("ForeignIdentifierFlag", "")).strip().upper() == "Y"
            nullable = _str(row.get("MandatoryFlag", "")).strip().upper() != "Y"
            fk_parent = _str(row.get("ForeignIdentifierParentEntityName"))

            attribute = Attribute(
                entity_name=entity_name,
                attribute_name=attr_code,
                logical_data_type=logical_type,
                precision=precision,
                scale=scale,
                max_length=max_length,
                nullable=nullable,
                is_primary_key=is_pk,
                is_foreign_key=is_fk,
                fk_references=fk_parent if fk_parent and fk_parent != "N/A" else None,
                description=_str(row.get("Comment", "")),
            )
            attributes.append(attribute)

        return attributes

    # ------------------------------------------------------------------
    # Relationship parsing
    # ------------------------------------------------------------------

    def _parse_relationships(
        self, wb: openpyxl.Workbook, attributes: list[Attribute]
    ) -> list[Relationship]:
        """Parse the Relationships sheet.

        JoinAttributes format: "ParentEntity.Attr Name = ChildEntity.Attr Name"
        Uses display names (EntityName, Attribute Name) which must be mapped
        back to Code via the attributes list.
        """
        ws = self._get_sheet(wb, "Relationships", required=True)
        if not ws:
            return []

        # Build lookup: (EntityName, Attribute Name) -> Attribute Code
        # The "Name" column in attributes has the display name
        rows = self._read_sheet_rows(ws)
        relationships = []

        for i, row in enumerate(rows, start=2):
            parent_entity = _str(row.get("ParentEntity"))
            child_entity = _str(row.get("ChildEntity"))
            rel_type = _str(row.get("RelationshipType", "1:n")).strip()
            join_attrs = _str(row.get("JoinAttributes", ""))
            stereotype = _str(row.get("Stereotype", "")).strip().lower()

            if not parent_entity or not child_entity:
                continue

            # Parse cardinality
            cardinality = _parse_cardinality(rel_type)

            # Parse join attributes: "Entity.AttrName = Entity.AttrName"
            parent_cols, child_cols = _parse_join_attributes(join_attrs)

            if not parent_cols or not child_cols:
                self.warnings.append(
                    f"Relationships row {i}: could not parse JoinAttributes "
                    f"'{join_attrs}' for {parent_entity} -> {child_entity}"
                )

            is_composition = stereotype == "composition"

            description = ""
            if is_composition:
                description = "composition: embedded array in source MongoDB document"

            rel = Relationship(
                parent_entity=parent_entity,
                child_entity=child_entity,
                cardinality=cardinality,
                parent_key_columns=parent_cols,
                child_key_columns=child_cols,
                is_identifying=is_composition,
                description=description,
            )
            relationships.append(rel)

        return relationships

    # ------------------------------------------------------------------
    # Config parsing
    # ------------------------------------------------------------------

    def _parse_config(self, wb: openpyxl.Workbook) -> Config:
        """Parse the optional config sheet (key-value pairs).

        Falls back to lite defaults if no config sheet is present.
        """
        ws = self._get_sheet(wb, "config", required=False)
        if not ws:
            return Config()

        rows = self._read_sheet_rows(ws)
        config_dict: dict[str, Any] = {}

        for row in rows:
            # Normalize header keys to lowercase for case-insensitive matching
            lc_row = {str(k).lower(): v for k, v in row.items()}
            key = _str(lc_row.get("key") or lc_row.get("config_key"))
            value = lc_row.get("value") or lc_row.get("config_value")
            if key and value is not None:
                config_dict[key.strip()] = value
            elif key:
                self.warnings.append(
                    f"Config key '{key}' has no value -- skipped"
                )

        if config_dict:
            self.warnings.append(
                f"Config sheet: read {len(config_dict)} settings: "
                + ", ".join(f"{k}={v}" for k, v in config_dict.items())
            )
        else:
            self.warnings.append(
                "Config sheet found but no valid key-value pairs read. "
                "Expected headers: 'key' and 'value'"
            )

        return _build_config_from_dict(config_dict)

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _get_sheet(
        self, wb: openpyxl.Workbook, sheet_name: str, required: bool = True
    ) -> Optional[Worksheet]:
        """Get a worksheet by name (case-insensitive search)."""
        for name in wb.sheetnames:
            if name.lower() == sheet_name.lower():
                return wb[name]
        if required:
            self.errors.append(
                f"Required sheet '{sheet_name}' not found. "
                f"Available sheets: {wb.sheetnames}"
            )
        return None

    @staticmethod
    def _read_sheet_rows(ws: Worksheet) -> list[dict[str, Any]]:
        """Read all data rows from a worksheet as dicts keyed by header name."""
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []

        headers = [str(h).strip() if h else "" for h in rows[0]]
        result = []

        for row in rows[1:]:
            if all(cell is None for cell in row):
                continue
            row_dict: dict[str, Any] = {}
            for col_idx, header in enumerate(headers):
                if header and col_idx < len(row):
                    row_dict[header] = row[col_idx]
            result.append(row_dict)

        return result


# --- Helper Functions ---


def _str(value: Any) -> str:
    """Convert to string, returning empty string for None."""
    if value is None:
        return ""
    return str(value).strip()


def _parse_int(value: Any) -> Optional[int]:
    """Parse a value to int, returning None if not possible."""
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def _parse_cardinality(value: str) -> Cardinality:
    """Parse relationship type string to Cardinality enum."""
    normalized = value.strip().upper().replace(" ", "")
    if normalized in ("1:1", "1-1", "ONE_TO_ONE"):
        return Cardinality.ONE_TO_ONE
    if normalized in ("1:N", "1-N", "1:MANY", "ONE_TO_MANY"):
        return Cardinality.ONE_TO_MANY
    if normalized in ("M:N", "M-N", "MANY_TO_MANY", "N:M"):
        return Cardinality.MANY_TO_MANY
    return Cardinality.ONE_TO_MANY


def _parse_join_attributes(join_str: str) -> tuple[list[str], list[str]]:
    """Parse JoinAttributes string into parent and child column lists.

    Format: "ParentEntity.AttrName = ChildEntity.AttrName"
    Single join only (no AND clause needed per requirements).

    Returns:
        Tuple of (parent_key_columns, child_key_columns) using the
        attribute Code (the part after the dot).
    """
    if not join_str or not join_str.strip():
        return [], []

    join_str = join_str.strip()

    # Split on '='
    parts = join_str.split("=")
    if len(parts) != 2:
        return [], []

    left = parts[0].strip()
    right = parts[1].strip()

    # Each side is "EntityName.Attribute Name"
    # Split on first '.' only (attribute names could contain dots, but unlikely)
    left_attr = _extract_attr_from_join_part(left)
    right_attr = _extract_attr_from_join_part(right)

    if not left_attr or not right_attr:
        return [], []

    return [left_attr], [right_attr]


def _extract_attr_from_join_part(part: str) -> str:
    """Extract attribute name from 'EntityName.AttributeName' format.

    The attribute name in JoinAttributes uses the display Name (e.g., "Account Uuid")
    which corresponds to the Code column (e.g., "ACCOUNT_UUID").

    We convert the display name to the Code format:
    - Convert to uppercase
    - Replace spaces with underscores
    """
    dot_pos = part.find(".")
    if dot_pos < 0:
        return ""
    attr_display_name = part[dot_pos + 1:].strip()
    # Convert display name to code format: uppercase, spaces -> underscores
    return attr_display_name.upper().replace(" ", "_")


def _parse_enum(value: Any, enum_class: type, default: Any) -> Any:
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


def _build_config_from_dict(d: dict[str, Any]) -> Config:
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
            kwargs[key] = _parse_enum(d[key], enum_class, None)

    for key in int_fields:
        if key in d:
            val = _parse_int(d[key])
            if val is not None:
                kwargs[key] = val

    return Config(**{k: v for k, v in kwargs.items() if v is not None})
