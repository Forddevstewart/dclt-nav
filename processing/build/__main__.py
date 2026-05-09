"""
Build CivicTwin/db/reference.db from CivicTwin source data.

All columns from every source are loaded as-is; no cherry-picking.
Schema is inferred by pandas from the data at load time.

Usage:
    python3 -m processing.build
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

from discovery.config import get_config

from .sources import (
    load_assessor, load_massgis, load_warrants, load_gis_top20,
    load_gis_layers, load_registry, load_ocr, load_for_sale, load_town_docs,
)
from .normalize import build_parcels
from .derived import (
    compute_coverage, compute_undeveloped_state_code,
    compute_farming_suitability, compute_acquisition_suitability,
    compute_parcel_acq_layers,
)
from .reference import load_attr_registry, load_gis_sources, load_ref_use_codes


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stage(engine, name: str, source, fn) -> int:
    print(f"\n[{name}]")
    n = fn(engine)
    with engine.begin() as con:
        con.execute(text(
            "INSERT INTO _pipeline_runs (stage, source_file, rows_loaded, run_at)"
            " VALUES (:s, :f, :n, :t)"
        ), {"s": name, "f": str(source) if source else "", "n": n, "t": _now_utc()})
    print(f"  → {n} rows")
    return n


def _load_link_candidates(engine) -> int:
    from processing.town_doc_candidates import load_parcel_link_candidates
    return load_parcel_link_candidates(engine)


def main() -> None:
    cfg = get_config()
    root = cfg.root

    assessor_files = cfg.collection_files("assessor")
    if not assessor_files:
        print("No assessor files in sources.yaml — cannot build.")
        sys.exit(1)
    assessor_path = assessor_files[0]["abs_path"]

    gis_files = cfg.collection_files("gis")
    massgis_path = gis_files[0]["abs_path"] if gis_files else root / "gis" / "dennis_parcels.geojson"

    warrants_path  = root / "ma-dennis" / "town_meeting_all_years.csv"
    ma_dennis_dir  = root / "ma-dennis"
    soil_path      = root / "gis" / "dennis_soil.csv"
    gis_dir        = root / "gis"
    registry_index = cfg.output_dir("registry") / "index"
    registry_docs  = cfg.output_dir("registry") / "documents"
    for_sale_path  = Path(__file__).parent.parent.parent / "HomeForSale.txt"

    db_path = cfg.db_path("raw")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    for label, path in [("Assessor", assessor_path), ("MassGIS", massgis_path)]:
        if not path.exists():
            print(f"MISSING required source: {label} — {path}")
            sys.exit(1)

    for label, path in [
        ("Warrants",       warrants_path),
        ("Town docs dir",  ma_dennis_dir),
        ("Soil CSV",       soil_path),
        ("GIS layers dir", gis_dir),
        ("Registry index", registry_index),
        ("Registry OCR",   registry_docs),
        ("For Sale",       for_sale_path),
    ]:
        print(f"  {label}: {'OK' if path.exists() else 'not found — stage will be skipped'}")

    if db_path.exists():
        db_path.unlink()
    print(f"\nBuilding {db_path}")

    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as con:
        con.execute(text("PRAGMA journal_mode=WAL"))
        con.execute(text("PRAGMA foreign_keys=ON"))
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS _pipeline_runs (
                run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                stage       TEXT,
                source_file TEXT,
                rows_loaded INTEGER,
                run_at      TEXT
            )
        """))

    stages = [
        ("load_assessor",           assessor_path,  lambda e: load_assessor(e, assessor_path)),
        ("load_massgis",            massgis_path,   lambda e: load_massgis(e, massgis_path)),
        ("load_warrants",           warrants_path,  lambda e: load_warrants(e, warrants_path)),
        ("layer_soils",             soil_path,      lambda e: load_gis_top20(e, soil_path)),
        ("parcels_gis",             gis_dir,        lambda e: load_gis_layers(e, gis_dir)),
        ("load_registry",           registry_index, lambda e: load_registry(e, registry_index)),
        ("load_ocr",                registry_docs,  lambda e: load_ocr(e, registry_docs)),
        ("load_for_sale",           for_sale_path,  lambda e: load_for_sale(e, for_sale_path)),
        ("load_town_docs",          ma_dennis_dir,  lambda e: load_town_docs(e, ma_dennis_dir)),
        ("build_parcels",           None,           lambda e: build_parcels(e)),
        ("coverage",                None,           lambda e: compute_coverage(e)),
        ("undeveloped_state_code",  None,           lambda e: compute_undeveloped_state_code(e)),
        ("farming_suitability",     None,           lambda e: compute_farming_suitability(e)),
        ("acquisition_suitability", None,           lambda e: compute_acquisition_suitability(e)),
        ("parcel_acq_layers",       None,           lambda e: compute_parcel_acq_layers(e)),
        ("link_candidates",         None,           lambda e: _load_link_candidates(e)),
        ("attr_registry",            None,           lambda e: load_attr_registry(e)),
        ("gis_sources",             None,           lambda e: load_gis_sources(e)),
        ("ref_use_codes",           None,           lambda e: load_ref_use_codes(e)),
    ]

    for name, source, fn in stages:
        try:
            _stage(engine, name, source, fn)
        except Exception as exc:
            print(f"\nERROR in {name}: {exc}")
            raise

    size_mb = db_path.stat().st_size / 1_000_000
    print(f"\nDone. {db_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
