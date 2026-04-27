"""
One-off migration: write For Sale layer presence as Parcel-level system tags in dclt.db.

Parcels are matched to layer_for_sale using the same address join used at query time
in api.py. Confidence is 1.0 — presence in the listing file is authoritative.

Run once after migration 8 has been applied:

    python3 -m processing.migrate_for_sale_tags

Safe to re-run: existing taggings for source='for_sale' are deleted first.
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def run():
    dclt_path = ROOT / "data" / "dclt.db"
    if not dclt_path.exists():
        print(f"dclt.db not found at {dclt_path} — start the app first")
        sys.exit(1)

    from discovery.config import get_config
    ref_path = get_config().db_path("reference")
    if not Path(ref_path).exists():
        print(f"reference.db not found at {ref_path}")
        sys.exit(1)

    dclt = sqlite3.connect(dclt_path)
    dclt.row_factory = sqlite3.Row
    ref  = sqlite3.connect(ref_path)
    ref.row_factory = sqlite3.Row

    has_for_sale = ref.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='layer_for_sale'"
    ).fetchone()[0]
    if not has_for_sale:
        print("layer_for_sale not found in reference.db — run the pipeline first")
        ref.close(); dclt.close()
        sys.exit(0)

    tag_row = dclt.execute(
        "SELECT tag_id FROM tags WHERE name = 'For Sale' AND tag_type = 'system'"
    ).fetchone()
    if not tag_row:
        print("System tag 'For Sale' not found — apply migration 8 first")
        ref.close(); dclt.close()
        sys.exit(1)
    tag_id = tag_row["tag_id"]

    # Match parcels to listings using the same address logic as api.py
    matched = ref.execute("""
        SELECT DISTINCT p.parcel_id
        FROM parcels p
        JOIN layer_for_sale fs
        WHERE p.locno IS NOT NULL AND p.locno != ''
          AND p.locst IS NOT NULL AND p.locst != ''
          AND UPPER(fs.norm_address) LIKE
              printf('%d', CAST(p.locno AS REAL)) || ' ' || UPPER(p.locst) || '%'
    """).fetchall()
    ref.close()

    parcel_ids = [r["parcel_id"] for r in matched]

    # Clear previous for_sale taggings (bypass append-only trigger temporarily)
    dclt.execute("DROP TRIGGER IF EXISTS no_del_taggings")
    dclt.execute("DELETE FROM taggings WHERE source = 'for_sale'")
    dclt.execute("""
        CREATE TRIGGER IF NOT EXISTS no_del_taggings
            BEFORE DELETE ON taggings
            BEGIN SELECT RAISE(FAIL,'taggings is append-only'); END
    """)

    for parcel_id in parcel_ids:
        dclt.execute(
            "INSERT INTO taggings"
            " (tag_id, state, target_type, target_id, user_id, system, confidence, source)"
            " VALUES (?, NULL, 'parcel', ?, 0, 1, 1.0, 'for_sale')",
            (tag_id, parcel_id),
        )

    dclt.commit()
    dclt.close()
    print(f"Inserted {len(parcel_ids)} For Sale tagging rows")


if __name__ == "__main__":
    run()
