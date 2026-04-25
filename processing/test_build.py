"""
Reconcile build.py source counts against raw.db loaded counts.

Run:
    python3 -m pytest -sv processing/test_build.py
    python3 processing/test_build.py
"""

import json
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from discovery.config import get_config
from processing.build import _LAYER_SPECS


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def cfg():
    return get_config()


@pytest.fixture(scope="module")
def engine(cfg):
    path = cfg.db_path("raw")
    assert path.exists(), (
        f"raw.db not found at {path}\n"
        f"Run: python3 -m processing.build"
    )
    return create_engine(f"sqlite:///{path}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _db_count(engine, table: str) -> int | None:
    try:
        with engine.connect() as con:
            return con.execute(text(f"SELECT COUNT(*) FROM [{table}]")).scalar()
    except Exception:
        return None


def _nonnull_count(engine, table: str, col: str) -> int | None:
    try:
        with engine.connect() as con:
            return con.execute(
                text(f"SELECT COUNT(*) FROM [{table}] WHERE [{col}] IS NOT NULL AND [{col}] != ''")
            ).scalar()
    except Exception:
        return None


def _csv_rows(path: Path) -> int:
    with path.open("rb") as f:
        return sum(1 for _ in f) - 1  # subtract header


def _fmt(n: int | None) -> str:
    return f"{n:,}" if n is not None else "—"


def _row(label, src, db_table, db_n, note=""):
    ok = "✓" if db_n is not None and db_n > 0 else "✗"
    print(f"  {ok}  {label:<40}  src={_fmt(src):<9}  db={_fmt(db_n):<9}  {note}")


# ── Test ──────────────────────────────────────────────────────────────────────

def test_build_counts(cfg, engine):
    root = cfg.root

    print()
    print("=" * 85)
    print("  Build reconciliation — source counts vs raw.db")
    print("=" * 85)

    # ── assessor ─────────────────────────────────────────────────────────────
    assessor_path = cfg.collection_files("assessor")[0]["abs_path"]
    assessor_src = len(pd.read_excel(assessor_path, sheet_name="BT_Extract"))
    assessor_db  = _db_count(engine, "assessor")
    _row("assessor  (BT_Extract sheet)", assessor_src, "assessor", assessor_db)
    assert assessor_db == assessor_src, f"assessor: src={assessor_src} db={assessor_db}"

    # ── massgis ───────────────────────────────────────────────────────────────
    gis_files  = cfg.collection_files("gis")
    massgis_path = gis_files[0]["abs_path"] if gis_files else root / "gis" / "dennis_parcels.geojson"
    massgis_src  = len(json.loads(massgis_path.read_text())["features"])
    massgis_db   = _db_count(engine, "massgis")
    _row("massgis   (dennis_parcels.geojson)", massgis_src, "massgis", massgis_db)
    assert massgis_db == massgis_src, f"massgis: src={massgis_src} db={massgis_db}"

    # ── warrants ──────────────────────────────────────────────────────────────
    warrants_path = root / "ma-dennis" / "town_meeting_all_years.csv"
    if warrants_path.exists():
        warrants_src = _csv_rows(warrants_path)
        warrants_db  = _db_count(engine, "warrants")
        _row("warrants  (town_meeting_all_years.csv)", warrants_src, "warrants", warrants_db)
        assert warrants_db == warrants_src, f"warrants: src={warrants_src} db={warrants_db}"
    else:
        _row("warrants  (town_meeting_all_years.csv)", None, "warrants", None, "not present — skipped")

    # ── layer_soils (from dennis_soil.csv, aggregated to unique map_par_id) ──
    soil_path = root / "gis" / "dennis_soil.csv"
    if soil_path.exists():
        soil_raw      = _csv_rows(soil_path)
        soil_unique   = pd.read_csv(soil_path, dtype=str, usecols=["MAP_PAR_ID"]).fillna("")["MAP_PAR_ID"].nunique()
        layer_soils_db = _db_count(engine, "layer_soils")
        _row(
            "soil      (dennis_soil.csv)",
            soil_raw, "layer_soils", layer_soils_db,
            f"raw rows={_fmt(soil_raw)}  unique map_par_id={_fmt(soil_unique)}",
        )
        assert layer_soils_db == soil_unique, (
            f"layer_soils: unique map_par_id={soil_unique} db={layer_soils_db}"
        )
    else:
        _row("soil      (dennis_soil.csv)", None, "layer_soils", None, "not present — skipped")

    # ── parcels_gis ───────────────────────────────────────────────────────────
    print()
    print(f"  {'GIS layers → parcels_gis'}")
    gis_dir = root / "gis"

    for spec in _LAYER_SPECS:
        path = gis_dir / spec["file"]
        src  = _csv_rows(path) if path.exists() else None
        key_col = list(spec["keep"].values())[0]
        nn = _nonnull_count(engine, "parcels_gis", key_col)
        label = f"  {spec['name']:<12}({spec['file']})"
        note  = f"non-null in parcels_gis: {_fmt(nn)}" if src is not None else "not present — skipped"
        _row(label, src, f"parcels_gis.{key_col}", nn, note)
        if src is not None:
            assert nn is not None and nn > 0, (
                f"{spec['name']}: {spec['file']} has {src} rows"
                f" but parcels_gis.{key_col} has {nn} non-null rows"
            )

    for fname, col, label in [
        ("dennis_structures.csv", "struct_count", "structures"),
        ("dennis_soil.csv",       "soil_map_unit", "soil"),
    ]:
        path = gis_dir / fname
        src  = _csv_rows(path) if path.exists() else None
        nn   = _nonnull_count(engine, "parcels_gis", col)
        _row(
            f"  {label:<12}({fname})",
            src, f"parcels_gis.{col}", nn,
            "aggregated" if src else "not present — skipped",
        )
        if src is not None:
            assert nn is not None and nn > 0, (
                f"{label}: {fname} has {src} rows"
                f" but parcels_gis.{col} has {nn} non-null rows"
            )

    parcels_gis_db = _db_count(engine, "parcels_gis")
    print(f"\n  {'parcels_gis total rows':<40}  {'':10}  db={_fmt(parcels_gis_db)}")
    assert parcels_gis_db is not None and parcels_gis_db > 0, "parcels_gis is empty or missing"

    # Every parcels_gis row must join to a parcel
    with engine.connect() as con:
        joined = con.execute(text(
            "SELECT COUNT(*) FROM parcels_gis g"
            " INNER JOIN layer_massgis p ON g.parcel_id = p.parcel_id"
        )).scalar()
        orphans = parcels_gis_db - joined
    print(f"  {'parcels_gis → layer_massgis join':<40}  {'':10}  matched={_fmt(joined)}  orphans={orphans}")
    assert orphans == 0, f"{orphans} parcels_gis rows have no matching parcel"

    # ── registry ──────────────────────────────────────────────────────────────
    print()
    registry_index = cfg.output_dir("registry") / "index"
    if registry_index.exists():
        total_docs  = sum(
            len(json.loads(p.read_text()))
            for p in registry_index.glob("*/documents.json")
        )
        registry_db = _db_count(engine, "registry_documents")
        _row("registry  (index/*/documents.json)", total_docs, "registry_documents", registry_db)
        assert registry_db == total_docs, f"registry: src={total_docs} db={registry_db}"
    else:
        _row("registry  (index/*/documents.json)", None, "registry_documents", None, "not present — skipped")

    print("=" * 85)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-sv"]))
