"""
Processing — Publish reference.db from raw.db + transactional.db.

Reads:
  CivicTwin/db/raw.db           — built by processing/build.py
  CivicTwin/db/transactional.db — server data synced down (optional)

Writes:
  CivicTwin/db/reference.db     — deployed to server (read-only there)

Merge steps applied from transactional.db (each skipped if table absent):
  parcel_corrections   Field-level overrides on the parcels table

Usage:
    python3 -m processing.publish
"""

import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from discovery.config import get_config


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(engine, name: str) -> bool:
    with engine.connect() as con:
        result = con.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        )
        return result.fetchone() is not None


def _parcel_columns(engine) -> set[str]:
    with engine.connect() as con:
        rows = con.execute(text("PRAGMA table_info(parcels)")).fetchall()
    return {r[1] for r in rows}


# ── Merge steps ───────────────────────────────────────────────────────────────
# Each function receives (ref_engine, tx_engine) and returns a row count.
# Add new steps here as transactional.db grows.

def _apply_parcel_corrections(ref_engine, tx_engine) -> int:
    """Field-level overrides: UPDATE parcels SET <field> = value WHERE parcel_id = id."""
    if not _table_exists(tx_engine, "parcel_corrections"):
        return 0
    df = pd.read_sql("SELECT * FROM parcel_corrections", tx_engine)
    if df.empty:
        return 0
    valid_cols = _parcel_columns(ref_engine)
    count = 0
    with ref_engine.begin() as con:
        for _, row in df.iterrows():
            field = str(row["field"])
            if field not in valid_cols:
                print(f"  WARN skip unknown field: {field}")
                continue
            con.execute(
                text(f"UPDATE parcels SET {field} = :val WHERE parcel_id = :pid"),
                {"val": row["corrected_value"], "pid": row["parcel_id"]},
            )
            count += 1
    return count


MERGE_STEPS = [
    ("parcel_corrections", _apply_parcel_corrections),
    # Future steps: ("user_annotations", _apply_annotations), etc.
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = get_config()
    raw_path = cfg.db_path("raw")
    ref_path = cfg.db_path("reference")
    tx_path  = cfg.db_path("transactional")

    if not raw_path.exists():
        print(f"raw.db not found at {raw_path} — run processing.build first")
        sys.exit(1)

    has_tx = tx_path.exists()
    print(f"raw:           {raw_path}")
    print(f"transactional: {tx_path} ({'found' if has_tx else 'not found — skipping merge steps'})")
    print(f"reference:     {ref_path}")

    # Checkpoint raw.db WAL before copying
    raw_engine = create_engine(f"sqlite:///{raw_path}")
    with raw_engine.connect() as con:
        con.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
    raw_engine.dispose()

    if ref_path.exists():
        ref_path.unlink()
    shutil.copy2(raw_path, ref_path)
    print(f"\n  Copied raw.db → reference.db")

    ref_engine = create_engine(f"sqlite:///{ref_path}")

    if has_tx:
        tx_engine = create_engine(f"sqlite:///{tx_path}")
        for label, fn in MERGE_STEPS:
            print(f"\n[{label}]")
            n = fn(ref_engine, tx_engine)
            print(f"  → {n} rows merged")

    with ref_engine.begin() as con:
        con.execute(text("""
            INSERT INTO _pipeline_runs (stage, source_file, rows_loaded, run_at)
            VALUES ('publish', :src, 0, :t)
        """), {"src": str(tx_path) if has_tx else "", "t": now_utc()})

    size_mb = ref_path.stat().st_size / 1_000_000
    print(f"\nDone. {ref_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
