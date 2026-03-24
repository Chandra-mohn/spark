"""SVG diagram emitter: generates a visual physical model diagram."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from xml.sax.saxutils import escape

from spark_pdm_generator.emitters.diagram_common import build_entity_infos
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
GRID_SPACING_Y = 80  # gap between rows for edge routing channels
PORT_PADDING = 30  # min distance from box corner for port placement
MAX_COLS_PER_ROW = 4  # max entities per row before wrapping


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
    entity_infos = build_entity_infos(output)

    # Layout entities using graph-aware placement (most connected in center)
    positions = _compute_layout(entity_infos, output.physical_relationships)

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

    # Draw entity boxes first (background)
    for name, info in entity_infos.items():
        pos = positions.get(name)
        if pos:
            parts.append(_svg_entity_box(pos["x"], pos["y"], info))

    # Collect valid edges and pre-compute port assignments
    edges = []
    for rel in output.physical_relationships:
        parent_pos = positions.get(rel.parent_physical_entity)
        child_pos = positions.get(rel.child_physical_entity)
        if parent_pos and child_pos:
            label = ", ".join(rel.join_columns) if rel.join_columns else ""
            join_info = rel.join_type.value.replace("_", " ").title()
            edges.append((parent_pos, child_pos, label, join_info))

    # Assign ports: for each box side, spread connection points evenly
    port_assignments = _assign_ports(edges)

    # Group edges by channel to stagger horizontal segments
    # Channel = (exit_y, enter_y) for bottom-top edges
    channel_counters: dict[tuple[int, int], int] = {}
    edge_channel_indices: list[int] = []
    edge_channel_totals: list[int] = []

    # First pass: count edges per channel
    channel_count_map: dict[tuple[int, int], int] = {}
    for i in range(len(edges)):
        exit_x, exit_y, enter_x, enter_y, side_pair = port_assignments[i]
        channel_key = (min(exit_y, enter_y), max(exit_y, enter_y))
        channel_count_map[channel_key] = channel_count_map.get(channel_key, 0) + 1

    # Second pass: assign stagger index per edge
    for i in range(len(edges)):
        exit_x, exit_y, enter_x, enter_y, side_pair = port_assignments[i]
        channel_key = (min(exit_y, enter_y), max(exit_y, enter_y))
        idx = channel_counters.get(channel_key, 0)
        channel_counters[channel_key] = idx + 1
        edge_channel_indices.append(idx)
        edge_channel_totals.append(channel_count_map[channel_key])

    # Draw edges on top of boxes with orthogonal routing
    for i, (parent_pos, child_pos, label, join_info) in enumerate(edges):
        exit_x, exit_y, enter_x, enter_y, side_pair = port_assignments[i]
        parts.append(_svg_edge_routed(
            exit_x, exit_y, enter_x, enter_y,
            side_pair=side_pair,
            label=label,
            sublabel=join_info,
            channel_index=edge_channel_indices[i],
            channel_total=edge_channel_totals[i],
        ))

    # Footer
    parts.append(_svg_footer(canvas_w, canvas_h))

    parts.append("</svg>")

    svg_content = "\n".join(parts)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg_content, encoding="utf-8")


# --- Data preparation ---



# _build_entity_infos moved to emitters/diagram_common.py


# --- Layout ---


def _compute_layout(entity_infos: dict, relationships: list) -> dict:
    """Position entities using graph-aware BFS layering.

    Facts always go in row 0 together. BFS from facts determines
    dimension tiers. Rows wider than MAX_COLS_PER_ROW are split.
    Each row is centered horizontally.
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

    # Separate facts from non-facts -- facts always go in layer 0
    facts = [
        n for n in all_names
        if entity_infos[n]["entity"].entity_type == PhysicalEntityType.FACT_TABLE
    ]
    facts.sort(key=_sort_key)

    assigned: set[str] = set(facts)

    # BFS from ALL facts simultaneously to find dimension tiers
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

    # Pick up any disconnected entities
    disconnected = [n for n in all_names if n not in assigned]
    disconnected.sort(key=_sort_key)
    if disconnected:
        bfs_layers.append(disconnected)

    # Build final row list: facts first, then BFS tiers
    raw_rows: list[list[str]] = [facts] + bfs_layers

    # Split rows that exceed MAX_COLS_PER_ROW
    rows: list[list[str]] = []
    for row in raw_rows:
        row.sort(key=_sort_key)
        for chunk_start in range(0, len(row), MAX_COLS_PER_ROW):
            rows.append(row[chunk_start:chunk_start + MAX_COLS_PER_ROW])

    # Position each row, centered horizontally
    positions = {}
    y_offset = 80  # below title

    def _box_height(name: str) -> int:
        info = entity_infos[name]
        lines = _count_content_lines(info)
        return HEADER_HEIGHT + BOX_PADDING + (lines * LINE_HEIGHT) + BOX_PADDING

    # Canvas width based on widest row (capped at MAX_COLS_PER_ROW)
    max_cols = max(len(row) for row in rows) if rows else 1
    total_canvas = max_cols * GRID_SPACING_X + 80  # 40px margin each side

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


def _assign_ports(
    edges: list[tuple[dict, dict, str, str]],
) -> list[tuple[int, int, int, int, str]]:
    """Assign connection ports on box edges for all edges.

    For each box side, distributes connection points evenly so edges
    don't bunch at the center.

    Returns list of (exit_x, exit_y, enter_x, enter_y, side_pair)
    where side_pair is "bottom-top", "right-left", etc.
    """
    # Count connections per (entity_pos_id, side) to know how many ports each side needs
    # Use (x, y) tuple as position identity
    def pos_id(pos: dict) -> tuple[int, int]:
        return (pos["x"], pos["y"])

    # For each edge, determine which sides to connect
    edge_sides = []
    for parent_pos, child_pos, _, _ in edges:
        p_top = parent_pos["y"]
        c_top = child_pos["y"]
        same_row = abs(p_top - c_top) < GRID_SPACING_Y // 2

        if same_row:
            if parent_pos["x"] < child_pos["x"]:
                edge_sides.append(("right", "left"))
            else:
                edge_sides.append(("left", "right"))
        elif p_top < c_top:
            edge_sides.append(("bottom", "top"))
        else:
            edge_sides.append(("top", "bottom"))

    # Count how many edges connect to each (pos_id, side)
    side_counts: dict[tuple[tuple[int, int], str], int] = {}
    side_indices: dict[tuple[tuple[int, int], str], int] = {}

    for i, (parent_pos, child_pos, _, _) in enumerate(edges):
        exit_side, enter_side = edge_sides[i]
        p_key = (pos_id(parent_pos), exit_side)
        c_key = (pos_id(child_pos), enter_side)
        side_counts[p_key] = side_counts.get(p_key, 0) + 1
        side_counts[c_key] = side_counts.get(c_key, 0) + 1

    # Assign port positions
    results = []
    for i, (parent_pos, child_pos, _, _) in enumerate(edges):
        exit_side, enter_side = edge_sides[i]
        p_key = (pos_id(parent_pos), exit_side)
        c_key = (pos_id(child_pos), enter_side)

        # Get this edge's index for each side
        p_idx = side_indices.get(p_key, 0)
        side_indices[p_key] = p_idx + 1
        c_idx = side_indices.get(c_key, 0)
        side_indices[c_key] = c_idx + 1

        p_total = side_counts[p_key]
        c_total = side_counts[c_key]

        exit_x, exit_y = _port_position(parent_pos, exit_side, p_idx, p_total)
        enter_x, enter_y = _port_position(child_pos, enter_side, c_idx, c_total)

        results.append((exit_x, exit_y, enter_x, enter_y,
                        f"{exit_side}-{enter_side}"))

    return results


def _port_position(
    pos: dict, side: str, index: int, total: int,
) -> tuple[int, int]:
    """Compute the (x, y) of the Nth port on a given side of a box.

    Distributes ports evenly along the side with padding from corners.
    """
    x, y, h = pos["x"], pos["y"], pos["height"]

    if side in ("top", "bottom"):
        # Spread along horizontal edge
        usable = BOX_WIDTH - 2 * PORT_PADDING
        if total <= 1:
            px = x + BOX_WIDTH // 2
        else:
            spacing = usable / (total - 1) if total > 1 else 0
            px = x + PORT_PADDING + int(index * spacing)
        py = y if side == "top" else y + h
        return (px, py)
    else:
        # Spread along vertical edge (left/right)
        usable = h - 2 * PORT_PADDING
        if total <= 1:
            py = y + h // 2
        else:
            spacing = usable / (total - 1) if total > 1 else 0
            py = y + PORT_PADDING + int(index * spacing)
        px = x if side == "left" else x + BOX_WIDTH
        return (px, py)


def _svg_edge_routed(
    exit_x: int,
    exit_y: int,
    enter_x: int,
    enter_y: int,
    side_pair: str,
    label: str = "",
    sublabel: str = "",
    channel_index: int = 0,
    channel_total: int = 1,
) -> str:
    """Draw an orthogonal edge between assigned port positions.

    Uses Z-shaped routing (exit -> horizontal channel -> enter)
    so edges stay in the gaps between entity rows. When multiple
    edges share the same channel, their horizontal segments are
    staggered vertically.
    """
    color = ARROW_COLOR
    marker = "arrowhead"

    # Stagger: spread horizontal segments across the channel gap
    STAGGER_STEP = 14
    if channel_total > 1:
        # Center the stagger around the midpoint
        stagger_offset = (channel_index - (channel_total - 1) / 2) * STAGGER_STEP
    else:
        stagger_offset = 0

    if side_pair == "bottom-top":
        mid_y = exit_y + (enter_y - exit_y) // 2 + int(stagger_offset)
        if exit_x == enter_x:
            waypoints = [(exit_x, exit_y), (enter_x, enter_y)]
        else:
            waypoints = [
                (exit_x, exit_y),
                (exit_x, mid_y),
                (enter_x, mid_y),
                (enter_x, enter_y),
            ]
    elif side_pair == "top-bottom":
        mid_y = enter_y + (exit_y - enter_y) // 2 + int(stagger_offset)
        if exit_x == enter_x:
            waypoints = [(exit_x, exit_y), (enter_x, enter_y)]
        else:
            waypoints = [
                (exit_x, exit_y),
                (exit_x, mid_y),
                (enter_x, mid_y),
                (enter_x, enter_y),
            ]
    elif side_pair in ("right-left", "left-right"):
        mid_x = exit_x + (enter_x - exit_x) // 2
        if exit_y == enter_y:
            waypoints = [(exit_x, exit_y), (enter_x, enter_y)]
        else:
            waypoints = [
                (exit_x, exit_y),
                (mid_x, exit_y),
                (mid_x, enter_y),
                (enter_x, enter_y),
            ]
    else:
        waypoints = [(exit_x, exit_y), (enter_x, enter_y)]

    # Build SVG path
    path_d = f"M {waypoints[0][0]} {waypoints[0][1]}"
    for wx, wy in waypoints[1:]:
        path_d += f" L {wx} {wy}"

    parts = [
        f'<path d="{path_d}" fill="none" '
        f'stroke="{color}" stroke-width="1.5" '
        f'marker-end="url(#{marker})"/>'
    ]

    # Label on the horizontal segment
    if label:
        mx, my = _find_label_position(waypoints)
        text_width = len(label) * 7 + 8
        parts.append(
            f'<rect x="{mx - text_width // 2}" y="{my - 12}" '
            f'width="{text_width}" height="16" '
            f'fill="white" opacity="0.9" rx="3"/>'
        )
        parts.append(
            f'<text x="{mx}" y="{my}" font-size="10" fill="{color}" '
            f'text-anchor="middle">{escape(label)}</text>'
        )
        if sublabel:
            sub_width = len(sublabel) * 6 + 8
            parts.append(
                f'<rect x="{mx - sub_width // 2}" y="{my + 2}" '
                f'width="{sub_width}" height="14" '
                f'fill="white" opacity="0.9" rx="3"/>'
            )
            parts.append(
                f'<text x="{mx}" y="{my + 13}" font-size="9" fill="#999999" '
                f'text-anchor="middle">{escape(sublabel)}</text>'
            )

    return "\n".join(parts)


def _find_label_position(waypoints: list[tuple[int, int]]) -> tuple[int, int]:
    """Find the midpoint of the longest segment for label placement."""
    best_len = 0
    best_mid = (waypoints[0][0], waypoints[0][1])
    for i in range(len(waypoints) - 1):
        x1, y1 = waypoints[i]
        x2, y2 = waypoints[i + 1]
        seg_len = abs(x2 - x1) + abs(y2 - y1)
        if seg_len > best_len:
            best_len = seg_len
            best_mid = ((x1 + x2) // 2, (y1 + y2) // 2 - 4)
    return best_mid
