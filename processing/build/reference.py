"""Reference table loaders — attr_registry, gis_sources, ref_use_codes."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from .normalize import USE_CODES


def load_attr_registry(engine) -> int:
    """Load layer_attributes from attr_manifest.yaml.

    Each layer entry in the manifest generates one row per column. Dynamic
    layers (no columns) generate a single row with col_name=null.
    """
    manifest_path = Path(__file__).parent.parent / "attr_manifest.yaml"
    with open(manifest_path) as f:
        manifest = yaml.safe_load(f)

    rows = []
    loaded_at = datetime.now(timezone.utc).isoformat()

    for layer_name, layer in manifest["layers"].items():
        attr_key      = layer["attr_key"]
        layer_class   = layer["class"]
        node_type     = layer["node_type"]
        db_table      = layer.get("db_table")
        build_stage   = layer.get("build_stage")
        layer_gis_key = layer.get("gis_source_key")

        columns = layer.get("columns") or {}

        if columns:
            for col_name, col in columns.items():
                rows.append({
                    "attr_id":        f"{attr_key}.{col_name}",
                    "layer_name":     layer_name,
                    "layer_class":    layer_class,
                    "node_type":      node_type,
                    "db_table":       db_table,
                    "col_name":       col_name,
                    "gis_source_key": col.get("gis_source_key", layer_gis_key),
                    "build_stage":    build_stage,
                    "label":          col.get("label", col_name.replace("_", " ").title()),
                    "description":    col.get("description", ""),
                    "display_group":  col.get("display_group", ""),
                    "display_order":  col.get("display_order", 0),
                    "value_type":     col.get("value_type", "text"),
                    "filterable":     int(col.get("filterable", True)),
                    "_loaded_at":     loaded_at,
                })
        else:
            rows.append({
                "attr_id":        attr_key,
                "layer_name":     layer_name,
                "layer_class":    layer_class,
                "node_type":      node_type,
                "db_table":       db_table,
                "col_name":       None,
                "gis_source_key": layer_gis_key,
                "build_stage":    build_stage,
                "label":          layer.get("label", layer_name),
                "description":    layer.get("description", ""),
                "display_group":  layer.get("display_group", ""),
                "display_order":  layer.get("display_order", 0),
                "value_type":     layer.get("value_type"),
                "filterable":     int(layer.get("filterable", False)),
                "_loaded_at":     loaded_at,
            })

    df = pd.DataFrame(rows)
    df.to_sql("layer_attributes", engine, if_exists="replace", index=False)
    return len(df)


def load_gis_sources(engine) -> int:
    src_dir = Path(__file__).parent.parent.parent / "data" / "gis_sources"
    records = []
    for p in sorted(src_dir.glob("*.json")):
        records.append(json.loads(p.read_text()))
    if not records:
        print("  WARN — no JSON files found in data/gis_sources/")
        return 0
    df = pd.DataFrame(records)
    df.to_sql("gis_sources", engine, if_exists="replace", index=False)
    return len(df)


def load_ref_use_codes(engine) -> int:
    df = pd.DataFrame(
        [(code, desc, cls) for code, (desc, cls) in USE_CODES.items()],
        columns=["code", "description", "property_class"],
    )
    df.to_sql("ref_use_codes", engine, if_exists="replace", index=False)
    return len(df)
