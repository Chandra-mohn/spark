"""Shared utilities for diagram emitters (SVG and Draw.io)."""

from collections import Counter

from spark_pdm_generator.models.physical import PhysicalModel


def build_entity_infos(output: PhysicalModel) -> dict:
    """Build a dict of entity name -> info for rendering.

    Shared by both diagram_emitter (SVG) and drawio_emitter.
    """
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

        join_cols: set[str] = set()
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
