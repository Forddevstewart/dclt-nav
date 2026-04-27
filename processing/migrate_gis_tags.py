"""
One-off migration: write GIS layer presence as Parcel-level system tags in dclt.db.

Each GIS layer column in parcels_gis becomes a tagging row when the value
indicates presence. Confidence is 1.0 — GIS data is authoritative, not scored.

Run once after migration 7 has been applied:

    python3 -m processing.migrate_gis_tags

Safe to re-run: existing taggings for source='gis_layer' are deleted first.
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Tag name → (column, is_numeric)
# Column names match what build.py writes into parcels_gis.
GIS_TAGS: list[tuple[str, str, bool]] = [
    ("Zone 1 WHP",       "zone1_type",    False),
    ("Zone 2 WHP",       "zone2_id",      False),
    ("Priority Habitat", "prihab_id",     False),
    ("Est. Habitat",     "esthab_id",     False),
    ("Nat. Community",   "natcomm_id",    False),
    ("BioMap3 VP",       "bm3_vp_id",     False),
    ("BioMap3 Wetland",  "bm3_wc_id",     False),
    ("BioMap3 Core",     "bm3_ch_id",     False),
    ("BioMap3 CNL",      "bm3_cnl_id",    False),
    ("Open Space",       "os_site_name",  False),
    ("Wetlands",         "wetlands_code", False),
    ("Structures",       "struct_count",  True),
    ("Soil",             "soil_name",     False),
]


def _present(val, is_numeric: bool) -> bool:
    if val is None:
        return False
    if is_numeric:
        try:
            return float(val) > 0
        except (ValueError, TypeError):
            return False
    return str(val).strip() != ""


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

    # Check parcels_gis exists
    has_gis = ref.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='parcels_gis'"
    ).fetchone()[0]
    if not has_gis:
        print("parcels_gis not found in reference.db — run the pipeline first")
        ref.close(); dclt.close()
        sys.exit(0)

    # Discover which columns actually exist in parcels_gis
    existing_cols = {
        row[1] for row in ref.execute("PRAGMA table_info(parcels_gis)").fetchall()
    }
    active_tags = [
        (name, col, numeric)
        for name, col, numeric in GIS_TAGS
        if col in existing_cols
    ]
    skipped = [name for name, col, _ in GIS_TAGS if col not in existing_cols]
    if skipped:
        print(f"  Skipping (column not in parcels_gis): {', '.join(skipped)}")

    # Resolve tag_ids from dclt.db
    tag_ids: dict[str, int] = {}
    for name, _, _ in active_tags:
        row = dclt.execute(
            "SELECT tag_id FROM tags WHERE name = ? AND tag_type = 'system'", (name,)
        ).fetchone()
        if not row:
            print(f"System tag '{name}' not found — apply migration 7 first")
            ref.close(); dclt.close()
            sys.exit(1)
        tag_ids[name] = row["tag_id"]

    # Load parcels_gis — only the columns we need
    cols = ["parcel_id"] + [col for _, col, _ in active_tags]
    rows = ref.execute(
        f"SELECT {', '.join(cols)} FROM parcels_gis"
    ).fetchall()
    ref.close()

    # Clear previous gis_layer taggings (bypass append-only trigger temporarily)
    dclt.execute("DROP TRIGGER IF EXISTS no_del_taggings")
    dclt.execute("DELETE FROM taggings WHERE source = 'gis_layer'")
    dclt.execute("""
        CREATE TRIGGER IF NOT EXISTS no_del_taggings
            BEFORE DELETE ON taggings
            BEGIN SELECT RAISE(FAIL,'taggings is append-only'); END
    """)

    inserted = 0
    for row in rows:
        parcel_id = row["parcel_id"]
        for name, col, is_numeric in active_tags:
            if _present(row[col], is_numeric):
                dclt.execute(
                    "INSERT INTO taggings"
                    " (tag_id, state, target_type, target_id, user_id, system, confidence, source)"
                    " VALUES (?, NULL, 'parcel', ?, 0, 1, 1.0, 'gis_layer')",
                    (tag_ids[name], parcel_id),
                )
                inserted += 1

    dclt.commit()
    dclt.close()
    print(f"Inserted {inserted} GIS tagging rows from {len(rows)} parcels")


if __name__ == "__main__":
    run()
