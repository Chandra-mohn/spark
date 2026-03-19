"""SVG diagram emitter: generates a visual physical model diagram."""

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from xml.sax.saxutils import escape

from spark_pdm_generator.models.physical import (
    PhysicalEntity,
    PhysicalEntityType,
    PhysicalModel,
)

# --- Color palette by entity type ---

ENTITY_COLORS = {
    PhysicalEntityType.FACT_TABLE: {"fill": "#4472C4", "stroke": "#2F5496", "text": "#FFFFFF"},
    PhysicalEntityType.DIMENSION_TABLE: {"fill": "#E2EFDA", "stroke": "#70AD47", "text": "#333333"},
    PhysicalEntityType.BRIDGE_TABLE: {"fill": "#FFF2CC", "stroke": "#BF8F00", "text": "#333333"},
    PhysicalEntityType.AGGREGATE_TABLE: {"fill": "#F2DCDB", "stroke": "#C0504D", "text": "#333333"},
    PhysicalEntityType.DOMAIN_GROUP: {"fill": "#D9E2F3", "stroke": "#4472C4", "text": "#333333"},
}

DEFAULT_COLORS = {"fill": "#F2F2F2", "stroke": "#999999", "text": "#333333"}

# --- Layout constants ---

BOX_WIDTH = 360
BOX_PADDING = 12
LINE_HEIGHT = 18
HEADER_HEIGHT = 32
SECTION_GAP = 6
FONT_FAMILY = "Consolas, 'Courier New', monospace"
FONT_SIZE = 12
HEADER_FONT_SIZE = 14
ARROW_COLOR = "#333333"
ABSORBED_ARROW_COLOR = "#999999"
BACKGROUND_COLOR = "#FAFAFA"
GRID_SPACING_X = 440
GRID_SPACING_Y = 100  # increased gap for edge routing channels
EDGE_CHANNEL_MARGIN = 20  # min distance from box edge for routed edges
EDGE_OFFSET_STEP = 12  # horizontal offset between parallel edges in same channel


def emit_diagram(
    output: PhysicalModel,
    output_path: Path,
    domain_name: str = "",
    denormalization_mode: str = "",
) -> None:
    """Generate an SVG diagram of the physical model.

    Args:
        output: The completed physical model.
        output_path: File path for the SVG output.
        domain_name: Optional domain name for the title.
        denormalization_mode: Denormalization mode used (AUTO/AGGRESSIVE/CONSERVATIVE).
    """
    # Build entity info dicts with computed stats
    entity_infos = _build_entity_infos(output)

    # Layout entities on a grid (facts center, dims around)
    positions = _compute_layout(entity_infos)

    # Compute SVG canvas size
    if not positions:
        return

    max_x = max(p["x"] + BOX_WIDTH for p in positions.values())
    max_y = max(p["y"] + p["height"] for p in positions.values())
    canvas_w = max_x + 80
    canvas_h = max_y + 120  # room for footer

    # Build SVG
    parts: list[str] = []
    parts.append(_svg_header(canvas_w, canvas_h))
    parts.append(_svg_defs())
    parts.append(f'<rect width="{canvas_w}" height="{canvas_h}" fill="{BACKGROUND_COLOR}"/>')

    # Title block
    parts.append(_svg_title(
        canvas_w, domain_name, denormalization_mode, entity_infos, output,
    ))

    # Draw edges first (behind boxes) using orthogonal routing
    # Group edges by channel (gap between rows) to offset parallel edges
    channel_counts: dict[tuple[int, int], int] = {}  # (y_top, y_bot) -> count
    edge_data = []
    for rel in output.physical_relationships:
        parent_pos = positions.get(rel.parent_physical_entity)
        child_pos = positions.get(rel.child_physical_entity)
        if parent_pos and child_pos:
            label = ", ".join(rel.join_columns) if rel.join_columns else ""
            join_info = rel.join_type.value.replace("_", " ").title()
            # Determine channel: gap between parent bottom and child top
            p_bot = parent_pos["y"] + parent_pos["height"]
            c_top = child_pos["y"]
            channel_key = (min(p_bot, c_top), max(p_bot, c_top))
            idx = channel_counts.get(channel_key, 0)
            channel_counts[channel_key] = idx + 1
            edge_data.append((parent_pos, child_pos, label, join_info, idx))

    for parent_pos, child_pos, label, join_info, edge_idx in edge_data:
        parts.append(_svg_edge_orthogonal(
            parent_pos, child_pos, positions,
            label=label,
            sublabel=join_info,
            dashed=False,
            edge_index=edge_idx,
        ))

    # Draw absorbed-entity ghost edges
    for name, info in entity_infos.items():
        pos = positions.get(name)
        if not pos:
            continue
        for absorbed_name in info["absorbed_from"]:
            # Absorbed entities don't have their own box, so skip edge drawing
            # They are shown as annotations inside the absorbing entity's box
            pass

    # Draw entity boxes
    for name, info in entity_infos.items():
        pos = positions.get(name)
        if pos:
            parts.append(_svg_entity_box(pos["x"], pos["y"], info))

    # Footer
    parts.append(_svg_footer(canvas_w, canvas_h))

    parts.append("</svg>")

    svg_content = "\n".join(parts)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg_content, encoding="utf-8")


# --- Data preparation ---


def _build_entity_infos(output: PhysicalModel) -> dict:
    """Build a dict of entity name -> info for rendering."""
    infos = {}
    for entity in output.physical_entities:
        attrs = output.get_attributes_for_entity(entity.physical_entity_name)

        # Count native vs absorbed
        source_counts: Counter = Counter()
        for a in attrs:
            source_counts[a.source_entity] += 1

        native_count = source_counts.get(entity.physical_entity_name, 0)
        # Also count attrs where source_entity is empty as native
        native_count += source_counts.get("", 0)
        absorbed_count = len(attrs) - native_count

        # Absorbed-from breakdown: {source_entity: count} excluding self
        absorbed_from = {
            src: cnt for src, cnt in source_counts.items()
            if src and src != entity.physical_entity_name
        }

        # Join columns: gather from relationships
        join_cols = set()
        for rel in output.physical_relationships:
            if rel.parent_physical_entity == entity.physical_entity_name:
                join_cols.update(rel.join_columns)
            if rel.child_physical_entity == entity.physical_entity_name:
                join_cols.update(rel.join_columns)

        # Sort columns
        sort_cols = [
            f"{sc.column_name} {sc.order.value}" for sc in entity.sort_columns
        ]

        # Estimate size display
        size_display = ""
        if entity.estimated_size_bytes:
            size_gb = entity.estimated_size_bytes / (1024 ** 3)
            size_mb = entity.estimated_size_bytes / (1024 ** 2)
            if size_gb >= 1:
                size_display = f"{size_gb:.1f} GB"
            else:
                size_display = f"{size_mb:.0f} MB"

        # Row count display
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


def _compute_layout(entity_infos: dict) -> dict:
    """Position entities on a grid. Facts top-center, dims below."""
    if not entity_infos:
        return {}

    facts = []
    dims = []
    others = []

    for name, info in entity_infos.items():
        etype = info["entity"].entity_type
        if etype == PhysicalEntityType.FACT_TABLE:
            facts.append(name)
        elif etype == PhysicalEntityType.DIMENSION_TABLE:
            dims.append(name)
        else:
            others.append(name)

    positions = {}
    y_offset = 80  # below title

    def _box_height(name: str) -> int:
        info = entity_infos[name]
        lines = _count_content_lines(info)
        return HEADER_HEIGHT + BOX_PADDING + (lines * LINE_HEIGHT) + BOX_PADDING

    # Place facts in a row
    if facts:
        row_width = len(facts) * GRID_SPACING_X
        start_x = 40
        for i, name in enumerate(facts):
            h = _box_height(name)
            positions[name] = {"x": start_x + i * GRID_SPACING_X, "y": y_offset, "height": h}
        y_offset += max(_box_height(n) for n in facts) + GRID_SPACING_Y

    # Place dimensions in rows of 3
    _place_in_rows(dims, entity_infos, positions, y_offset, _box_height)
    if dims:
        placed_dims = [positions[n] for n in dims if n in positions]
        if placed_dims:
            y_offset = max(p["y"] + p["height"] for p in placed_dims) + GRID_SPACING_Y

    # Place others
    _place_in_rows(others, entity_infos, positions, y_offset, _box_height)

    return positions


def _place_in_rows(
    names: list[str],
    entity_infos: dict,
    positions: dict,
    y_offset: int,
    height_fn,
    cols: int = 3,
) -> None:
    """Place entities in a grid of the given column count."""
    start_x = 40
    row_y = y_offset
    row_max_h = 0

    for i, name in enumerate(names):
        col = i % cols
        if i > 0 and col == 0:
            row_y += row_max_h + GRID_SPACING_Y
            row_max_h = 0

        h = height_fn(name)
        positions[name] = {"x": start_x + col * GRID_SPACING_X, "y": row_y, "height": h}
        row_max_h = max(row_max_h, h)


def _count_content_lines(info: dict) -> int:
    """Count how many text lines this entity box will need."""
    entity = info["entity"]
    lines = 0

    # Stats line: "Columns: N (native: X, absorbed: Y)"
    lines += 1

    # Absorbed from breakdown (if any)
    if info["absorbed_from"]:
        lines += 1  # "Absorbed from:" header
        lines += len(info["absorbed_from"])

    # Size/rows line
    if info["size_display"] or info["row_display"]:
        lines += 1

    # Separator
    lines += 1

    # Partition
    if entity.partition_columns:
        lines += 1

    # Bucket
    if entity.bucket_column:
        lines += 1

    # Sort
    if info["sort_columns"]:
        lines += 1

    # Compression/format
    lines += 1

    # Separator + join columns
    if info["join_columns"]:
        lines += 1  # separator
        lines += 1  # "Join Columns:" header
        # Wrap join columns into lines of ~40 chars
        lines += _count_wrapped_lines(info["join_columns"], 40)

    return lines


def _count_wrapped_lines(items: list[str], max_chars: int) -> int:
    """Count lines needed to display items comma-separated with wrapping."""
    if not items:
        return 0
    current_len = 0
    line_count = 1
    for i, item in enumerate(items):
        addition = len(item) + (2 if i > 0 else 0)  # ", " prefix
        if current_len + addition > max_chars and current_len > 0:
            line_count += 1
            current_len = len(item)
        else:
            current_len += addition
    return line_count


# --- SVG rendering ---


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'font-family="{FONT_FAMILY}">'
    )


def _svg_defs() -> str:
    """Define reusable SVG elements (arrowhead marker)."""
    return """<defs>
  <marker id="arrowhead" markerWidth="10" markerHeight="7"
          refX="10" refY="3.5" orient="auto" fill="#333333">
    <polygon points="0 0, 10 3.5, 0 7"/>
  </marker>
  <marker id="arrowhead-absorbed" markerWidth="10" markerHeight="7"
          refX="10" refY="3.5" orient="auto" fill="#999999">
    <polygon points="0 0, 10 3.5, 0 7"/>
  </marker>
</defs>"""


def _svg_title(
    canvas_w: int,
    domain_name: str,
    denorm_mode: str,
    entity_infos: dict,
    output: PhysicalModel,
) -> str:
    """Render the title block at the top of the diagram."""
    total_entities = len(entity_infos)
    total_attrs = sum(info["total_attrs"] for info in entity_infos.values())

    title = escape(domain_name) if domain_name else "Physical Data Model"
    subtitle_parts = []
    if denorm_mode:
        subtitle_parts.append(f"Denormalization: {escape(denorm_mode)}")
    subtitle_parts.append(f"Entities: {total_entities}")
    subtitle_parts.append(f"Total Attributes: {total_attrs}")
    subtitle = "  |  ".join(subtitle_parts)

    return (
        f'<text x="{canvas_w // 2}" y="30" text-anchor="middle" '
        f'font-size="18" font-weight="bold" fill="#333333">{title}</text>\n'
        f'<text x="{canvas_w // 2}" y="52" text-anchor="middle" '
        f'font-size="{FONT_SIZE}" fill="#666666">{subtitle}</text>'
    )


def _svg_footer(canvas_w: int, canvas_h: int) -> str:
    """Render footer with timestamp and legend."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    y = canvas_h - 20

    # Legend items
    legend_items = [
        (PhysicalEntityType.FACT_TABLE, "Fact"),
        (PhysicalEntityType.DIMENSION_TABLE, "Dimension"),
        (PhysicalEntityType.BRIDGE_TABLE, "Bridge"),
    ]
    legend_parts = []
    lx = 40
    for etype, label in legend_items:
        colors = ENTITY_COLORS.get(etype, DEFAULT_COLORS)
        legend_parts.append(
            f'<rect x="{lx}" y="{y - 10}" width="14" height="14" '
            f'fill="{colors["fill"]}" stroke="{colors["stroke"]}" rx="2"/>'
            f'<text x="{lx + 20}" y="{y + 2}" font-size="11" fill="#666666">{label}</text>'
        )
        lx += len(label) * 8 + 40

    footer_ts = (
        f'<text x="{canvas_w - 40}" y="{y + 2}" text-anchor="end" '
        f'font-size="11" fill="#999999">Generated: {ts}</text>'
    )

    return "\n".join(legend_parts) + "\n" + footer_ts


def _svg_entity_box(x: int, y: int, info: dict) -> str:
    """Render a single entity box."""
    entity: PhysicalEntity = info["entity"]
    colors = ENTITY_COLORS.get(entity.entity_type, DEFAULT_COLORS)

    parts: list[str] = []
    height = HEADER_HEIGHT + BOX_PADDING + (_count_content_lines(info) * LINE_HEIGHT) + BOX_PADDING

    # Box background
    parts.append(
        f'<rect x="{x}" y="{y}" width="{BOX_WIDTH}" height="{height}" '
        f'fill="white" stroke="{colors["stroke"]}" stroke-width="1.5" rx="6"/>'
    )

    # Header bar
    parts.append(
        f'<rect x="{x}" y="{y}" width="{BOX_WIDTH}" height="{HEADER_HEIGHT}" '
        f'fill="{colors["fill"]}" stroke="{colors["stroke"]}" stroke-width="1.5" rx="6"/>'
    )
    # Square off bottom corners of header
    parts.append(
        f'<rect x="{x}" y="{y + HEADER_HEIGHT - 6}" width="{BOX_WIDTH}" height="6" '
        f'fill="{colors["fill"]}"/>'
    )

    # Header text: entity name + badge
    badge = entity.entity_type.value.replace("_TABLE", "").replace("_", " ")
    name_display = escape(entity.physical_entity_name)
    parts.append(
        f'<text x="{x + BOX_PADDING}" y="{y + 21}" '
        f'font-size="{HEADER_FONT_SIZE}" font-weight="bold" fill="{colors["text"]}">'
        f'{name_display}</text>'
    )
    parts.append(
        f'<text x="{x + BOX_WIDTH - BOX_PADDING}" y="{y + 21}" '
        f'text-anchor="end" font-size="11" fill="{colors["text"]}">[{badge}]</text>'
    )

    # Body content
    cy = y + HEADER_HEIGHT + BOX_PADDING + 14  # first text line baseline
    text_color = "#333333"
    muted_color = "#666666"

    def _line(text: str, color: str = text_color, bold: bool = False) -> str:
        nonlocal cy
        weight = ' font-weight="bold"' if bold else ""
        result = (
            f'<text x="{x + BOX_PADDING}" y="{cy}" '
            f'font-size="{FONT_SIZE}" fill="{color}"{weight}>{escape(text)}</text>'
        )
        cy += LINE_HEIGHT
        return result

    def _separator() -> str:
        nonlocal cy
        sep_y = cy - LINE_HEIGHT + 6
        result = (
            f'<line x1="{x + BOX_PADDING}" y1="{sep_y}" '
            f'x2="{x + BOX_WIDTH - BOX_PADDING}" y2="{sep_y}" '
            f'stroke="#DDDDDD" stroke-width="1"/>'
        )
        cy += 0  # separator doesn't add extra line
        return result

    # Column stats
    parts.append(_line(
        f"Columns: {info['total_attrs']}  "
        f"(native: {info['native_count']}, absorbed: {info['absorbed_count']})"
    ))

    # Absorbed-from breakdown
    if info["absorbed_from"]:
        parts.append(_line("Absorbed from:", muted_color))
        for src, cnt in sorted(info["absorbed_from"].items()):
            parts.append(_line(f"  {src} ({cnt} cols)", muted_color))

    # Size / rows
    size_parts = []
    if info["row_display"]:
        size_parts.append(info["row_display"])
    if info["size_display"]:
        size_parts.append(info["size_display"])
    if size_parts:
        parts.append(_line("  |  ".join(size_parts), muted_color))

    # Separator
    parts.append(_separator())

    # Storage section
    if entity.partition_columns:
        parts.append(_line(
            f"Partition: {', '.join(entity.partition_columns)}"
        ))

    if entity.bucket_column:
        bucket_text = f"Bucket: {entity.bucket_column}"
        if entity.bucket_count:
            bucket_text += f" x {entity.bucket_count}"
        parts.append(_line(bucket_text))

    if info["sort_columns"]:
        parts.append(_line(f"Sort: {', '.join(info['sort_columns'])}"))

    parts.append(_line(
        f"{entity.compression_codec} / {entity.storage_format}",
        muted_color,
    ))

    # Join columns section
    if info["join_columns"]:
        parts.append(_separator())
        parts.append(_line("Join Columns:", text_color, bold=True))
        # Wrap join columns
        current_line_parts: list[str] = []
        current_len = 0
        for col in info["join_columns"]:
            addition = len(col) + (2 if current_line_parts else 0)
            if current_len + addition > 40 and current_line_parts:
                parts.append(_line("  " + ", ".join(current_line_parts), muted_color))
                current_line_parts = [col]
                current_len = len(col)
            else:
                current_line_parts.append(col)
                current_len += addition
        if current_line_parts:
            parts.append(_line("  " + ", ".join(current_line_parts), muted_color))

    return "\n".join(parts)


def _svg_edge_orthogonal(
    parent_pos: dict,
    child_pos: dict,
    all_positions: dict,
    label: str = "",
    sublabel: str = "",
    dashed: bool = False,
    edge_index: int = 0,
) -> str:
    """Draw an orthogonal (right-angle) arrow between two entity boxes.

    Routes edges through the gaps between rows so they never cross
    through entity boxes.
    """
    color = ABSORBED_ARROW_COLOR if dashed else ARROW_COLOR
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    marker = "arrowhead-absorbed" if dashed else "arrowhead"

    # Horizontal offset for parallel edges in the same channel
    h_offset = edge_index * EDGE_OFFSET_STEP

    p_top = parent_pos["y"]
    p_bot = parent_pos["y"] + parent_pos["height"]
    p_cx = parent_pos["x"] + BOX_WIDTH // 2
    p_left = parent_pos["x"]
    p_right = parent_pos["x"] + BOX_WIDTH

    c_top = child_pos["y"]
    c_bot = child_pos["y"] + child_pos["height"]
    c_cx = child_pos["x"] + BOX_WIDTH // 2
    c_left = child_pos["x"]
    c_right = child_pos["x"] + BOX_WIDTH

    same_row = abs(p_top - c_top) < GRID_SPACING_Y // 2

    if same_row:
        # Same-row connection: route around via top or bottom
        waypoints = _route_same_row(
            parent_pos, child_pos, all_positions, h_offset,
        )
    elif p_top < c_top:
        # Parent is above child: exit bottom, route through gap, enter top
        mid_y = p_bot + (c_top - p_bot) // 2 + h_offset
        exit_x = p_cx + h_offset
        enter_x = c_cx + h_offset
        waypoints = [
            (exit_x, p_bot),      # exit parent bottom
            (exit_x, mid_y),      # down to channel
            (enter_x, mid_y),     # across to child alignment
            (enter_x, c_top),     # up to child top
        ]
    else:
        # Parent is below child: exit top, route through gap, enter bottom
        mid_y = c_bot + (p_top - c_bot) // 2 + h_offset
        exit_x = p_cx + h_offset
        enter_x = c_cx + h_offset
        waypoints = [
            (exit_x, p_top),      # exit parent top
            (exit_x, mid_y),      # up to channel
            (enter_x, mid_y),     # across to child alignment
            (enter_x, c_bot),     # down to child bottom
        ]

    # Build SVG path from waypoints
    path_d = f"M {waypoints[0][0]} {waypoints[0][1]}"
    for wx, wy in waypoints[1:]:
        path_d += f" L {wx} {wy}"

    parts = [
        f'<path d="{path_d}" fill="none" '
        f'stroke="{color}" stroke-width="1.5"{dash} '
        f'marker-end="url(#{marker})"/>'
    ]

    # Label at the horizontal segment midpoint
    if label and len(waypoints) >= 3:
        # Find the horizontal segment (where y is constant between two points)
        mx, my = _find_label_position(waypoints)
        parts.append(
            f'<rect x="{mx - 4}" y="{my - 12}" width="{len(label) * 7 + 8}" '
            f'height="16" fill="white" opacity="0.9" rx="3"/>'
        )
        parts.append(
            f'<text x="{mx}" y="{my}" font-size="10" fill="{color}">'
            f'{escape(label)}</text>'
        )
        if sublabel:
            parts.append(
                f'<text x="{mx}" y="{my + 14}" font-size="9" fill="#999999">'
                f'{escape(sublabel)}</text>'
            )

    return "\n".join(parts)


def _route_same_row(
    parent_pos: dict,
    child_pos: dict,
    all_positions: dict,
    h_offset: int,
) -> list[tuple[int, int]]:
    """Route an edge between two entities on the same row.

    Goes out the nearest side, then vertically above/below to clear
    all boxes, then horizontally, then back down/up into the target.
    """
    p_left = parent_pos["x"]
    p_right = parent_pos["x"] + BOX_WIDTH
    p_mid_y = parent_pos["y"] + parent_pos["height"] // 2
    c_left = child_pos["x"]
    c_right = child_pos["x"] + BOX_WIDTH
    c_mid_y = child_pos["y"] + child_pos["height"] // 2

    # Route above the row: use the minimum y of both boxes minus margin
    route_y = min(parent_pos["y"], child_pos["y"]) - EDGE_CHANNEL_MARGIN - abs(h_offset)

    if parent_pos["x"] < child_pos["x"]:
        # Parent is left of child -- go out right side, around top, in left side
        return [
            (p_right, p_mid_y),
            (p_right + EDGE_CHANNEL_MARGIN, p_mid_y),
            (p_right + EDGE_CHANNEL_MARGIN, route_y),
            (c_left - EDGE_CHANNEL_MARGIN, route_y),
            (c_left - EDGE_CHANNEL_MARGIN, c_mid_y),
            (c_left, c_mid_y),
        ]
    else:
        # Parent is right of child
        return [
            (p_left, p_mid_y),
            (p_left - EDGE_CHANNEL_MARGIN, p_mid_y),
            (p_left - EDGE_CHANNEL_MARGIN, route_y),
            (c_right + EDGE_CHANNEL_MARGIN, route_y),
            (c_right + EDGE_CHANNEL_MARGIN, c_mid_y),
            (c_right, c_mid_y),
        ]


def _find_label_position(waypoints: list[tuple[int, int]]) -> tuple[int, int]:
    """Find a good position for an edge label on the horizontal segment."""
    # Look for horizontal segments (same y between consecutive points)
    for i in range(len(waypoints) - 1):
        x1, y1 = waypoints[i]
        x2, y2 = waypoints[i + 1]
        if y1 == y2 and x1 != x2:
            return ((x1 + x2) // 2, y1 - 4)
    # Fallback: midpoint of the whole path
    mid_idx = len(waypoints) // 2
    return (waypoints[mid_idx][0], waypoints[mid_idx][1] - 4)
