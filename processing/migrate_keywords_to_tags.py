"""
One-off migration: copy registry_ocr keyword scores from reference.db
into dclt.db taggings as system tags.

Run once after migration 5 has been applied (i.e. after the app has
started and auto-migrated dclt.db):

    python3 -m processing.migrate_keywords_to_tags

Safe to re-run: existing taggings for source='ocr_keyword' are deleted
first (the append-only trigger is bypassed via a direct DELETE before
new rows are inserted within a transaction).
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

KW_COLS = {
    "kw_conservation_restriction":           "Conservation Restriction",
    "kw_article_97":                         "Article 97",
    "kw_deed_restriction":                   "Deed Restriction",
    "kw_chapter_61":                         "Chapter 61",
    "kw_agricultural_preservation_restriction": "Ag. Preservation Restriction",
    "kw_perpetual_restriction":              "Perpetual Restriction",
    "kw_ccr":                                "CC&R",
}


def run():
    dclt_path = ROOT / "data" / "dclt.db"
    if not dclt_path.exists():
        print(f"dclt.db not found at {dclt_path} — start the app first to create it")
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

    # Resolve tag_ids from dclt.db
    tag_ids = {}
    for col, name in KW_COLS.items():
        row = dclt.execute(
            "SELECT tag_id FROM tags WHERE name = ? AND tag_type = 'system'", (name,)
        ).fetchone()
        if not row:
            print(f"System tag '{name}' not found — run the app to apply migration 5 first")
            ref.close(); dclt.close()
            sys.exit(1)
        tag_ids[col] = row["tag_id"]

    # Check registry_ocr exists
    has_ocr = ref.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='registry_ocr'"
    ).fetchone()[0]
    if not has_ocr:
        print("registry_ocr table not found in reference.db — nothing to migrate")
        ref.close(); dclt.close()
        sys.exit(0)

    ocr_rows = ref.execute("SELECT book, page, " + ", ".join(KW_COLS) + " FROM registry_ocr").fetchall()
    ref.close()

    # Clear previous ocr_keyword taggings (bypass append-only trigger temporarily)
    dclt.execute("DROP TRIGGER IF EXISTS no_del_taggings")
    dclt.execute("DELETE FROM taggings WHERE source = 'ocr_keyword'")
    dclt.execute("""
        CREATE TRIGGER IF NOT EXISTS no_del_taggings
            BEFORE DELETE ON taggings
            BEGIN SELECT RAISE(FAIL,'taggings is append-only'); END
    """)

    inserted = 0
    for row in ocr_rows:
        target_id = f"{row['book']}/{row['page']}"
        for col, tag_id in tag_ids.items():
            score = row[col]
            if score is None or score <= 0:
                continue
            dclt.execute(
                "INSERT INTO taggings (tag_id, state, target_type, target_id, user_id, system, confidence, source)"
                " VALUES (?, NULL, 'document', ?, 0, 1, ?, 'ocr_keyword')",
                (tag_id, target_id, score),
            )
            inserted += 1

    dclt.commit()
    dclt.close()
    print(f"Inserted {inserted} keyword tagging rows from {len(ocr_rows)} OCR documents")


if __name__ == "__main__":
    run()
