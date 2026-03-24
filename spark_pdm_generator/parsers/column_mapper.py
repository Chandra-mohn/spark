"""Column mapper: translates user's Excel column names to internal field names."""

import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class SheetMapping(BaseModel):
    """Mapping of internal field names to user's column headers for one sheet."""

    field_map: dict[str, Optional[str]] = Field(default_factory=dict)

    def get_user_column(self, internal_field: str) -> Optional[str]:
        """Get the user's column name for an internal field.

        Returns None if the field is not mapped (absent in user's workbook).
        Returns the mapped column name otherwise.
        """
        return self.field_map.get(internal_field)

    def get_internal_field(self, user_column: str) -> Optional[str]:
        """Reverse lookup: get internal field name from user's column name."""
        for internal, user in self.field_map.items():
            if user is not None and user.strip().lower() == user_column.strip().lower():
                return internal
        return None


class ColumnMapping(BaseModel):
    """Complete column mapping configuration."""

    sheet_names: dict[str, str] = Field(default_factory=dict)
    entities: SheetMapping = Field(default_factory=SheetMapping)
    attributes: SheetMapping = Field(default_factory=SheetMapping)
    relationships: SheetMapping = Field(default_factory=SheetMapping)

    def get_sheet_name(self, internal_name: str) -> str:
        """Get the user's sheet tab name for an internal sheet name.

        Falls back to the internal name if not mapped.
        """
        return self.sheet_names.get(internal_name, internal_name)

    def get_sheet_mapping(self, internal_sheet_name: str) -> Optional[SheetMapping]:
        """Get the SheetMapping for a given internal sheet name."""
        mapping_map = {
            "entities": self.entities,
            "attributes": self.attributes,
            "relationships": self.relationships,
        }
        return mapping_map.get(internal_sheet_name)


def load_column_mapping(mapping_path: Path) -> ColumnMapping:
    """Load column mapping from a JSON file."""
    try:
        with open(mapping_path, "r") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in mapping file '{mapping_path}': {e}") from e

    sheet_names = raw.get("sheet_names", {})
    mapping = ColumnMapping(sheet_names=sheet_names)

    for sheet_key in ("entities", "attributes", "relationships"):
        if sheet_key in raw:
            sheet_mapping = SheetMapping(field_map=raw[sheet_key])
            setattr(mapping, sheet_key, sheet_mapping)

    return mapping


def create_default_mapping() -> ColumnMapping:
    """Create the default mapping where internal names match column headers."""
    return ColumnMapping(
        sheet_names={
            "entities": "entities",
            "attributes": "attributes",
            "relationships": "relationships",
        },
        entities=SheetMapping(
            field_map={
                "entity_name": "entity_name",
                "entity_type": "entity_type",
                "description": "description",
                "grain_description": "grain_description",
                "domain": "domain",
                "estimated_row_count": "estimated_row_count",
                "estimated_record_length_bytes": "estimated_record_length_bytes",
                "growth_rate": "growth_rate",
                "update_frequency": "update_frequency",
            }
        ),
        attributes=SheetMapping(
            field_map={
                "entity_name": "entity_name",
                "attribute_name": "attribute_name",
                "logical_data_type": "logical_data_type",
                "precision": "precision",
                "scale": "scale",
                "max_length": "max_length",
                "nullable": "nullable",
                "is_primary_key": "is_primary_key",
                "is_foreign_key": "is_foreign_key",
                "fk_references": "fk_references",
                "description": "description",
                "domain_group": "domain_group",
            }
        ),
        relationships=SheetMapping(
            field_map={
                "parent_entity": "parent_entity",
                "child_entity": "child_entity",
                "cardinality": "cardinality",
                "parent_key_columns": "parent_key_columns",
                "child_key_columns": "child_key_columns",
                "is_identifying": "is_identifying",
                "description": "description",
            }
        ),
    )


def generate_mapping_template(
    sheet_headers: dict[str, list[str]],
) -> dict:
    """Generate a best-guess mapping template from workbook sheet headers.

    Args:
        sheet_headers: dict mapping sheet tab names to their column header lists.

    Returns:
        A dict suitable for writing to JSON as a column_map.json file.
    """
    default = create_default_mapping()
    template: dict = {"sheet_names": {}, "entities": {}, "attributes": {}, "relationships": {}}

    # Fuzzy match internal sheet names to actual sheet tab names
    internal_sheets = ["entities", "attributes", "relationships"]
    actual_tabs = list(sheet_headers.keys())

    for internal_name in internal_sheets:
        best_match = _fuzzy_match_sheet(internal_name, actual_tabs)
        if best_match:
            template["sheet_names"][internal_name] = best_match

    # For each mapped sheet, fuzzy match column headers
    for internal_sheet in internal_sheets:
        actual_tab = template["sheet_names"].get(internal_sheet)
        if actual_tab and actual_tab in sheet_headers:
            actual_headers = sheet_headers[actual_tab]
            sheet_mapping = getattr(default, internal_sheet)
            for internal_field in sheet_mapping.field_map:
                best_match = _fuzzy_match_column(internal_field, actual_headers)
                template[internal_sheet][internal_field] = best_match

    return template


def _fuzzy_match_sheet(internal_name: str, actual_tabs: list[str]) -> Optional[str]:
    """Simple fuzzy matching for sheet tab names."""
    internal_lower = internal_name.lower()
    for tab in actual_tabs:
        if tab.lower() == internal_lower:
            return tab
        if internal_lower in tab.lower() or tab.lower() in internal_lower:
            return tab
    return None


def _fuzzy_match_column(
    internal_field: str, actual_headers: list[str]
) -> Optional[str]:
    """Simple fuzzy matching for column header names.

    Matches by: exact (case-insensitive), contains, word overlap.
    """
    internal_lower = internal_field.lower()
    internal_words = set(internal_lower.replace("_", " ").split())

    # Exact match (case-insensitive)
    for header in actual_headers:
        if header.strip().lower() == internal_lower:
            return header

    # Header contained in internal field (e.g., header="entity" matches internal="entity_name")
    # but NOT the reverse -- prevents "entity_name" matching "parent_entity_name"
    for header in actual_headers:
        header_lower = header.strip().lower()
        if header_lower in internal_lower:
            return header

    # Word overlap
    best_score = 0
    best_match = None
    for header in actual_headers:
        header_words = set(header.strip().lower().replace("_", " ").split())
        overlap = len(internal_words & header_words)
        if overlap > best_score:
            best_score = overlap
            best_match = header

    if best_score > 0:
        return best_match

    return None
