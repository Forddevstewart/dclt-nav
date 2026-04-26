"""
Reconcile build.py source counts against raw.db loaded counts.
Also checks for gaps between the priority queue, enumeration cache, and downloaded PDFs.

Run:
    python3 -m pytest -sv processing/test_build.py
    python3 processing/test_build.py
"""

import csv as csv_mod
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


def test_queue_enumeration_gaps(cfg, engine):
    """Priority queue parcels vs registry enumeration cache.

    Every parcel in target_queue.csv should have a fresh cache entry.
    Parcels that have a deed book/page reference but returned zero documents
    are flagged separately — these are likely Tier 1 lookup failures.
    """
    from discovery.registry.cache import get_cached_index

    queue_csv = cfg.output_dir("registry") / "queue" / "target_queue.csv"
    if not queue_csv.exists():
        pytest.skip("target_queue.csv not found — run discovery.registry.queue first")

    with queue_csv.open(newline="", encoding="utf-8") as f:
        queue = list(csv_mod.DictReader(f))

    total       = len(queue)
    with_docs   = 0
    empty       = 0   # enumerated but no documents found
    pending     = 0   # not in cache or stale
    t1_misses   = []  # had deed_book/page but returned 0 documents

    for row in queue:
        pid    = row["parcel_id"]
        cached = get_cached_index(pid)
        if cached is None:
            pending += 1
        elif not cached:
            empty += 1
            book = row.get("deed_book", "").strip()
            if book and book != "0":
                t1_misses.append((pid, book, row.get("deed_page", "").strip()))
        else:
            with_docs += 1

    print()
    print("=" * 85)
    print("  Queue → Enumeration reconciliation")
    print("=" * 85)
    print(f"  {'Total priority parcels in queue':<45}  {_fmt(total)}")
    print(f"  {'Enumerated — documents found':<45}  {_fmt(with_docs)}")
    print(f"  {'Enumerated — no results':<45}  {_fmt(empty)}")
    print(f"  {'Not yet enumerated (pending / stale)':<45}  {_fmt(pending)}")

    if t1_misses:
        print(f"\n  Parcels with deed book/page but 0 documents ({len(t1_misses)}):")
        for pid, book, page in t1_misses[:25]:
            print(f"    {pid:<20}  book={book}  page={page}")
        if len(t1_misses) > 25:
            print(f"    … and {len(t1_misses) - 25} more")

    print("=" * 85)

    assert pending == 0, (
        f"{pending} of {total} queue entries are not yet enumerated. "
        "Run: python3 -m discovery.registry.pipeline --override-robots"
    )
    assert not t1_misses, (
        f"{len(t1_misses)} parcels have a deed book/page but 0 indexed documents — "
        "possible Tier 1 lookup failures. Check registry_access.log."
    )


def test_enumeration_download_gaps(cfg, engine):
    """Enumerated registry documents vs downloaded PDF scans.

    Compares what was found during enumeration against what has been downloaded.
    Uses the same instrument-type filter as the download step.
    scan_cached reflects the state at the last processing.build run.
    """
    from discovery.registry.download import _is_approved

    rd_total = _db_count(engine, "registry_documents")
    if not rd_total:
        pytest.skip("registry_documents is empty — run pipeline first")

    with engine.connect() as con:
        rows = con.execute(text(
            "SELECT doc_type_code, instrument_type, book, page, scan_cached, parcel_id"
            " FROM registry_documents"
        )).fetchall()

    approved     = [r for r in rows if _is_approved({"doc_type_code": r[0], "instrument_type": r[1]})]
    downloaded   = [r for r in approved if r[4]]
    not_dl       = [r for r in approved if not r[4]]

    # Tally missing by instrument type
    missing_by_type: dict[str, int] = {}
    for r in not_dl:
        t = (r[1] or r[0] or "UNKNOWN").strip()
        missing_by_type[t] = missing_by_type.get(t, 0) + 1

    # Cross-check: any approved docs where the PDF exists on disk but scan_cached=0
    # (i.e., downloaded after the last build — build.py is stale)
    from discovery.registry.cache import scan_path as _scan_path
    stale_build = sum(
        1 for r in not_dl
        if r[2] and r[3] and _scan_path(r[2], r[3]).exists()
    )

    print()
    print("=" * 85)
    print("  Enumeration → Download reconciliation")
    print("=" * 85)
    print(f"  {'Total registry_documents rows':<45}  {_fmt(rd_total)}")
    print(f"  {'Approved instrument types':<45}  {_fmt(len(approved))}")
    print(f"  {'Downloaded (scan_cached = 1)':<45}  {_fmt(len(downloaded))}")
    print(f"  {'Not downloaded':<45}  {_fmt(len(not_dl))}")
    if stale_build:
        print(f"  {'  of which: on disk but build not run':<45}  {_fmt(stale_build)}"
              "  ← re-run processing.build")

    if missing_by_type:
        print(f"\n  Not downloaded by instrument type:")
        for t, c in sorted(missing_by_type.items(), key=lambda x: -x[1]):
            print(f"    {c:4d}  {t}")

    print("=" * 85)

    assert len(approved) > 0, (
        "No approved documents found in registry_documents — "
        "check instrument type filters in discovery.registry.download"
    )
    assert len(not_dl) == 0, (
        f"{len(not_dl)} approved documents have no PDF scan. "
        "Run: python3 -m discovery.registry.pipeline --override-robots --confirm"
    )


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-sv"]))
