"""Draw.io diagram emitter: generates an editable .drawio physical model diagram.

Produces a .drawio (XML) file that can be opened in Draw.io / diagrams.net
for manual layout adjustment. Connectors use orthogonal auto-routing so
Draw.io handles edge paths automatically.
"""

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from xml.sax.saxutils import escape

from spark_pdm_generator.models.physical import (
    PhysicalEntity,
    PhysicalEntityType,
    PhysicalModel,
)

# --- Color palette by entity type ---

ENTITY_COLORS = {
    PhysicalEntityType.FACT_TABLE: {
        "header_fill": "#4472C4",
        "header_stroke": "#2F5496",
        "header_font": "#FFFFFF",
        "body_fill": "#FFFFFF",
        "body_stroke": "#2F5496",
    },
    PhysicalEntityType.DIMENSION_TABLE: {
        "header_fill": "#E2EFDA",
        "header_stroke": "#70AD47",
        "header_font": "#333333",
        "body_fill": "#FFFFFF",
        "body_stroke": "#70AD47",
    },
    PhysicalEntityType.BRIDGE_TABLE: {
        "header_fill": "#FFF2CC",
        "header_stroke": "#BF8F00",
        "header_font": "#333333",
        "body_fill": "#FFFFFF",
        "body_stroke": "#BF8F00",
    },
    PhysicalEntityType.AGGREGATE_TABLE: {
        "header_fill": "#F2DCDB",
        "header_stroke": "#C0504D",
        "header_font": "#333333",
        "body_fill": "#FFFFFF",
        "body_stroke": "#C0504D",
    },
    PhysicalEntityType.DOMAIN_GROUP: {
        "header_fill": "#D9E2F3",
        "header_stroke": "#4472C4",
        "header_font": "#333333",
        "body_fill": "#FFFFFF",
        "body_stroke": "#4472C4",
    },
}

DEFAULT_COLORS = {
    "header_fill": "#F2F2F2",
    "header_stroke": "#999999",
    "header_font": "#333333",
    "body_fill": "#FFFFFF",
    "body_stroke": "#999999",
}

# --- Layout constants ---

BOX_WIDTH = 340
HEADER_HEIGHT = 30
LINE_HEIGHT = 16
BOX_PADDING = 8
GRID_SPACING_X = 420
GRID_SPACING_Y = 80
MAX_COLS_PER_ROW = 4


def emit_drawio(
    output: PhysicalModel,
    output_path: Path,
    domain_name: str = "",
    denormalization_mode: str = "",
) -> None:
    """Generate a Draw.io diagram of the physical model.

    Args:
        output: The completed physical model.
        output_path: File path for the .drawio output.
        domain_name: Optional domain name for the title.
        denormalization_mode: Denormalization mode used.
    """
    entity_infos = _build_entity_infos(output)
    if not entity_infos:
        return

    positions = _compute_layout(entity_infos, output.physical_relationships)
    if not positions:
        return

    # Track cell IDs: id 0 = root, id 1 = default layer
    next_id = 2
    cells: list[str] = []

    # Map entity name -> cell ID for connector source/target
    entity_cell_ids: dict[str, int] = {}

    # Build entity cells (header group + body)
    for name, info in entity_infos.items():
        pos = positions.get(name)
        if not pos:
            continue

        body_lines = _build_body_lines(info)
        body_height = max(len(body_lines) * LINE_HEIGHT + 2 * BOX_PADDING, 40)
        total_height = HEADER_HEIGHT + body_height

        colors = ENTITY_COLORS.get(info["entity"].entity_type, DEFAULT_COLORS)
        badge = info["entity"].entity_type.value.replace("_TABLE", "").replace("_", " ")

        # Container group cell (parent for header + body)
        group_id = next_id
        next_id += 1
        entity_cell_ids[name] = group_id

        cells.append(
            f'        <mxCell id="{group_id}" value="" '
            f'style="group;container=1;collapsible=0;" '
            f'vertex="1" connectable="1" parent="1">\n'
            f'          <mxGeometry x="{pos["x"]}" y="{pos["y"]}" '
            f'width="{BOX_WIDTH}" height="{total_height}" as="geometry"/>\n'
            f'        </mxCell>'
        )

        # Header cell
        header_id = next_id
        next_id += 1
        header_text = escape(f"{info['entity'].physical_entity_name}  [{badge}]")
        cells.append(
            f'        <mxCell id="{header_id}" '
            f'value="{header_text}" '
            f'style="rounded=0;whiteSpace=wrap;html=1;'
            f'fillColor={colors["header_fill"]};'
            f'strokeColor={colors["header_stroke"]};'
            f'fontColor={colors["header_font"]};'
            f'fontSize=12;fontStyle=1;'
            f'align=left;verticalAlign=middle;spacingLeft=8;" '
            f'vertex="1" parent="{group_id}">\n'
            f'          <mxGeometry width="{BOX_WIDTH}" height="{HEADER_HEIGHT}" '
            f'as="geometry"/>\n'
            f'        </mxCell>'
        )

        # Body cell with multi-line content
        body_id = next_id
        next_id += 1
        body_html = _build_body_html(body_lines)
        cells.append(
            f'        <mxCell id="{body_id}" '
            f'value="{escape(body_html)}" '
            f'style="rounded=0;whiteSpace=wrap;html=1;overflow=fill;'
            f'fillColor={colors["body_fill"]};'
            f'strokeColor={colors["body_stroke"]};'
            f'fontSize=11;fontColor=#333333;'
            f'align=left;verticalAlign=top;spacingLeft=8;spacingTop=4;" '
            f'vertex="1" parent="{group_id}">\n'
            f'          <mxGeometry y="{HEADER_HEIGHT}" '
            f'width="{BOX_WIDTH}" height="{body_height}" as="geometry"/>\n'
            f'        </mxCell>'
        )

    # Build relationship connectors
    for rel in output.physical_relationships:
        parent_id = entity_cell_ids.get(rel.parent_physical_entity)
        child_id = entity_cell_ids.get(rel.child_physical_entity)
        if parent_id is None or child_id is None:
            continue

        edge_id = next_id
        next_id += 1

        join_cols = ", ".join(rel.join_columns) if rel.join_columns else ""
        join_type = rel.join_type.value.replace("_", " ").title()
        label = escape(f"{join_cols}") if join_cols else ""
        sublabel = escape(join_type)

        # Edge label: join columns + join type
        edge_value = label
        if sublabel:
            edge_value = f"{label}&lt;br&gt;&lt;font style=&quot;font-size: 9px&quot;&gt;{sublabel}&lt;/font&gt;" if label else sublabel

        cells.append(
            f'        <mxCell id="{edge_id}" '
            f'value="{edge_value}" '
            f'style="edgeStyle=orthogonalEdgeStyle;rounded=1;'
            f'orthogonalLoop=1;jettySize=auto;html=1;'
            f'endArrow=classic;endFill=1;'
            f'strokeColor=#333333;strokeWidth=1;'
            f'fontSize=10;fontColor=#333333;'
            f'labelBackgroundColor=#FFFFFF;" '
            f'edge="1" parent="1" '
            f'source="{parent_id}" target="{child_id}">\n'
            f'          <mxGeometry relative="1" as="geometry"/>\n'
            f'        </mxCell>'
        )

    # Compute canvas size
    max_x = max(p["x"] + BOX_WIDTH for p in positions.values()) + 80
    max_y = max(p["y"] + p["height"] for p in positions.values()) + 80

    # Assemble the full .drawio XML
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    title = escape(domain_name) if domain_name else "Physical Data Model"
    total_entities = len(entity_infos)
    total_attrs = sum(info["total_attrs"] for info in entity_infos.values())

    subtitle_parts = []
    if denormalization_mode:
        subtitle_parts.append(f"Denormalization: {denormalization_mode}")
    subtitle_parts.append(f"Entities: {total_entities}")
    subtitle_parts.append(f"Total Attributes: {total_attrs}")
    diagram_name = f"{title} - {' | '.join(subtitle_parts)}"

    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<mxfile host="spark-pdm-generator" modified="{ts}" type="device">',
        f'  <diagram id="pdm_diagram" name="{escape(diagram_name)}">',
        f'    <mxGraphModel dx="{max_x}" dy="{max_y}" '
        f'grid="1" gridSize="10" guides="1" tooltips="1" '
        f'connect="1" arrows="1" fold="1" page="0" '
        f'pageScale="1" pageWidth="{max_x}" pageHeight="{max_y}" '
        f'background="#FAFAFA">',
        '      <root>',
        '        <mxCell id="0"/>',
        '        <mxCell id="1" parent="0"/>',
    ]

    xml_parts.extend(cells)

    xml_parts.extend([
        '      </root>',
        '    </mxGraphModel>',
        '  </diagram>',
        '</mxfile>',
    ])

    xml_content = "\n".join(xml_parts)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(xml_content, encoding="utf-8")


# --- Data preparation ---


def _build_entity_infos(output: PhysicalModel) -> dict:
    """Build a dict of entity name -> info for rendering."""
    infos = {}
    for entity in output.physical_entities:
        attrs = output.get_attributes_for_entity(entity.physical_entity_name)

        source_counts: Counter = Counter()
        for a in attrs:
            source_counts[a.source_entity] += 1

        native_count = source_counts.get(entity.physical_entity_name, 0)
        native_count += source_counts.get("", 0)
        absorbed_count = len(attrs) - native_count

        absorbed_from = {
            src: cnt for src, cnt in source_counts.items()
            if src and src != entity.physical_entity_name
        }

        join_cols = set()
        for rel in output.physical_relationships:
            if rel.parent_physical_entity == entity.physical_entity_name:
                join_cols.update(rel.join_columns)
            if rel.child_physical_entity == entity.physical_entity_name:
                join_cols.update(rel.join_columns)

        sort_cols = [
            f"{sc.column_name} {sc.order.value}" for sc in entity.sort_columns
        ]

        size_display = ""
        if entity.estimated_size_bytes:
            size_gb = entity.estimated_size_bytes / (1024 ** 3)
            size_mb = entity.estimated_size_bytes / (1024 ** 2)
            if size_gb >= 1:
                size_display = f"{size_gb:.1f} GB"
            else:
                size_display = f"{size_mb:.0f} MB"

        row_display = ""
        if entity.estimated_row_count:
            if entity.estimated_row_count >= 1_000_000:
                row_display = f"{entity.estimated_row_count / 1_000_000:.1f}M rows"
            elif entity.estimated_row_count >= 1_000:
                row_display = f"{entity.estimated_row_count / 1_000:.0f}K rows"
            else:
                row_display = f"{entity.estimated_row_count} rows"

        infos[entity.physical_entity_name] = {
            "entity": entity,
            "total_attrs": len(attrs),
            "native_count": native_count,
            "absorbed_count": absorbed_count,
            "absorbed_from": absorbed_from,
            "join_columns": sorted(join_cols),
            "sort_columns": sort_cols,
            "size_display": size_display,
            "row_display": row_display,
        }

    return infos


# --- Layout ---


def _compute_layout(entity_infos: dict, relationships: list) -> dict:
    """Position entities using graph-aware BFS layering.

    Facts in row 0, BFS tiers below. Rows capped at MAX_COLS_PER_ROW.
    """
    if not entity_infos:
        return {}

    all_names = list(entity_infos.keys())

    # Build adjacency graph
    adj: dict[str, set[str]] = {n: set() for n in all_names}
    for rel in relationships:
        p = rel.parent_physical_entity
        c = rel.child_physical_entity
        if p in adj and c in adj:
            adj[p].add(c)
            adj[c].add(p)

    def _sort_key(name: str) -> tuple[int, int, str]:
        etype = entity_infos[name]["entity"].entity_type
        type_rank = 0 if etype == PhysicalEntityType.FACT_TABLE else 1
        return (type_rank, -len(adj[name]), name)

    # Facts always in layer 0
    facts = [
        n for n in all_names
        if entity_infos[n]["entity"].entity_type == PhysicalEntityType.FACT_TABLE
    ]
    facts.sort(key=_sort_key)
    assigned: set[str] = set(facts)

    # BFS from all facts simultaneously
    bfs_layers: list[list[str]] = []
    queue = list(facts)
    while queue:
        next_queue: list[str] = []
        layer: list[str] = []
        for node in queue:
            for neighbor in sorted(adj[node], key=_sort_key):
                if neighbor not in assigned:
                    assigned.add(neighbor)
                    next_queue.append(neighbor)
                    layer.append(neighbor)
        if layer:
            bfs_layers.append(layer)
        queue = next_queue

    # Disconnected entities
    disconnected = [n for n in all_names if n not in assigned]
    disconnected.sort(key=_sort_key)
    if disconnected:
        bfs_layers.append(disconnected)

    # Build rows: facts first, then BFS tiers, split at MAX_COLS_PER_ROW
    raw_rows: list[list[str]] = [facts] + bfs_layers
    rows: list[list[str]] = []
    for row in raw_rows:
        row.sort(key=_sort_key)
        for chunk_start in range(0, len(row), MAX_COLS_PER_ROW):
            rows.append(row[chunk_start:chunk_start + MAX_COLS_PER_ROW])

    # Position each row centered
    positions = {}
    y_offset = 20

    def _box_height(name: str) -> int:
        info = entity_infos[name]
        body_lines = _build_body_lines(info)
        body_h = max(len(body_lines) * LINE_HEIGHT + 2 * BOX_PADDING, 40)
        return HEADER_HEIGHT + body_h

    max_cols = max(len(row) for row in rows) if rows else 1
    total_canvas = max_cols * GRID_SPACING_X + 80

    for row in rows:
        row_width = len(row) * GRID_SPACING_X - (GRID_SPACING_X - BOX_WIDTH)
        start_x = max(40, (total_canvas - row_width) // 2)

        row_max_h = 0
        for i, name in enumerate(row):
            h = _box_height(name)
            positions[name] = {
                "x": start_x + i * GRID_SPACING_X,
                "y": y_offset,
                "height": h,
            }
            row_max_h = max(row_max_h, h)

        y_offset += row_max_h + GRID_SPACING_Y

    return positions


# --- Content builders ---


def _build_body_lines(info: dict) -> list[str]:
    """Build plain-text lines for the entity body."""
    entity = info["entity"]
    lines = []

    # Column stats
    lines.append(
        f"Columns: {info['total_attrs']}  "
        f"(native: {info['native_count']}, absorbed: {info['absorbed_count']})"
    )

    # Absorbed-from breakdown
    if info["absorbed_from"]:
        lines.append("Absorbed from:")
        for src, cnt in sorted(info["absorbed_from"].items()):
            lines.append(f"  {src} ({cnt} cols)")

    # Size / rows
    size_parts = []
    if info["row_display"]:
        size_parts.append(info["row_display"])
    if info["size_display"]:
        size_parts.append(info["size_display"])
    if size_parts:
        lines.append("  |  ".join(size_parts))

    # Separator
    lines.append("---")

    # Storage
    if entity.partition_columns:
        lines.append(f"Partition: {', '.join(entity.partition_columns)}")

    if entity.bucket_column:
        bucket_text = f"Bucket: {entity.bucket_column}"
        if entity.bucket_count:
            bucket_text += f" x {entity.bucket_count}"
        lines.append(bucket_text)

    if info["sort_columns"]:
        lines.append(f"Sort: {', '.join(info['sort_columns'])}")

    lines.append(f"{entity.compression_codec} / {entity.storage_format}")

    # Join columns
    if info["join_columns"]:
        lines.append("---")
        lines.append(f"Join: {', '.join(info['join_columns'])}")

    return lines


def _build_body_html(lines: list[str]) -> str:
    """Convert body lines to HTML for Draw.io cell value."""
    html_lines = []
    for line in lines:
        if line == "---":
            html_lines.append("<hr/>")
        else:
            html_lines.append(f"<div>{escape(line)}</div>")
    return "".join(html_lines)
