"""Reference table loaders — schema_columns, gis_sources, ref_use_codes."""

import json
from pathlib import Path

import pandas as pd

from .normalize import USE_CODES


def load_schema_columns(engine) -> int:
    path = Path(__file__).parent.parent / "schema_columns.csv"
    df = pd.read_csv(path, dtype=str).fillna("")
    df.to_sql("schema_columns", engine, if_exists="replace", index=False)
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
