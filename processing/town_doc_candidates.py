"""
Extract parcel references from town_docs and write candidate links to dclt.db.

Run after processing.build (which populates town_docs in reference.db).
Safe to re-run: uses INSERT OR IGNORE so adjudicated rows are never overwritten.

Two patterns:
  ocr_map_lot  — "Assessor's Map 294, Parcel 25"  →  parcel_id 294-25   confidence 0.95
  ocr_address  — "648 Setucket Road"              →  fuzzy address match  confidence 0.70–0.85

Usage:
    python3 -m processing.town_doc_candidates
"""

import re
import sqlite3
from pathlib import Path

from discovery.config import get_config

# ── Patterns ──────────────────────────────────────────────────────────────────

_MAP_LOT = re.compile(
    r"(?:Assessor'?s?\s+)?Map\s+(\d+)[,\s]+(?:Parcel|Lot)\s+(\d+)",
    re.IGNORECASE,
)

_ADDR = re.compile(
    r"\b(\d+)\s+([A-Z][A-Za-z ]{2,}?"
    r"(?:Road|Lane|Street|Drive|Avenue|Way|Circle|Court|Place|Path|Highway|Route"
    r"|Rd|St|Dr|Ave|Ln|Ct|Pl|Hwy|Rte))\b",
    re.IGNORECASE,
)

_ADDR_NORM = {
    r"\bRd\b":  "ROAD",    r"\bSt\b":  "STREET",
    r"\bAve\b": "AVENUE",  r"\bDr\b":  "DRIVE",
    r"\bLn\b":  "LANE",    r"\bCir\b": "CIRCLE",
    r"\bCt\b":  "COURT",   r"\bPl\b":  "PLACE",
    r"\bHwy\b": "HIGHWAY", r"\bRte\b": "ROUTE",
    r"\bExt\b": "EXTENSION",
}


def _norm_street(name: str) -> str:
    s = name.upper().strip()
    for pat, repl in _ADDR_NORM.items():
        s = re.sub(pat, repl, s, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", s)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_parcel_index(ref: sqlite3.Connection) -> tuple[dict, dict]:
    """
    Returns:
      map_lot_index  — {(map_str, lot_str): parcel_id}
      addr_index     — {(locno_str, norm_locst): parcel_id}
    """
    rows = ref.execute(
        "SELECT parcel_id, locno, locst FROM parcels WHERE locno IS NOT NULL AND locst IS NOT NULL"
    ).fetchall()

    map_lot_index: dict[tuple, str] = {}
    addr_index:    dict[tuple, str] = {}

    for r in rows:
        pid = r["parcel_id"]
        # map-lot key from parcel_id format "MAP-LOT"
        parts = pid.split("-")
        if len(parts) == 2:
            map_lot_index[(parts[0].lstrip("0") or "0", parts[1].lstrip("0") or "0")] = pid

        locno = str(r["locno"] or "").strip()
        locst = _norm_street(str(r["locst"] or ""))
        if locno and locst:
            addr_index[(locno, locst)] = pid

    return map_lot_index, addr_index


def _candidates_for_text(
    text: str,
    map_lot_index: dict,
    addr_index: dict,
) -> list[dict]:
    """Return list of {parcel_id, match_type, match_text, confidence} dicts."""
    seen: dict[str, dict] = {}  # parcel_id → best candidate so far

    def _add(pid: str, match_type: str, match_text: str, conf: float) -> None:
        if pid not in seen or seen[pid]["confidence"] < conf:
            seen[pid] = {
                "parcel_id":  pid,
                "match_type": match_type,
                "match_text": match_text[:200],
                "confidence": conf,
            }

    # Pass 1 — Map/Lot
    for m in _MAP_LOT.finditer(text):
        map_n = m.group(1).lstrip("0") or "0"
        lot_n = m.group(2).lstrip("0") or "0"
        pid   = map_lot_index.get((map_n, lot_n))
        if pid:
            _add(pid, "ocr_map_lot", m.group(0), 0.95)

    # Pass 2 — Addresses
    for m in _ADDR.finditer(text):
        locno = m.group(1)
        locst = _norm_street(m.group(2))
        pid   = addr_index.get((locno, locst))
        if pid:
            _add(pid, "ocr_address", m.group(0), 0.75)

    return list(seen.values())


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg      = get_config()
    ref_path = cfg.db_path("raw")          # reference.db (read)
    dclt_path = Path(__file__).parent.parent / "data" / "dclt.db"

    if not ref_path.exists():
        print(f"reference db not found: {ref_path} — run processing.build first")
        return

    ref  = sqlite3.connect(ref_path)
    ref.row_factory = sqlite3.Row
    dclt = sqlite3.connect(dclt_path)
    dclt.row_factory = sqlite3.Row

    # Ensure parcel_links table exists (migration may not have run yet)
    dclt.executescript("""
        CREATE TABLE IF NOT EXISTS parcel_links (
            link_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id       TEXT    NOT NULL,
            source_type  TEXT    NOT NULL,
            parcel_id    TEXT    NOT NULL,
            match_type   TEXT,
            match_text   TEXT,
            confidence   REAL,
            status       TEXT    NOT NULL DEFAULT 'candidate'
                             CHECK(status IN ('candidate','confirmed','rejected')),
            reviewed_by  INTEGER,
            reviewed_at  TEXT,
            created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(doc_id, parcel_id)
        );
        CREATE INDEX IF NOT EXISTS idx_parcel_links_doc
            ON parcel_links (doc_id);
        CREATE INDEX IF NOT EXISTS idx_parcel_links_parcel
            ON parcel_links (parcel_id, status);
    """)
    dclt.commit()

    has_town_docs = ref.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='town_docs'"
    ).fetchone()[0]
    if not has_town_docs:
        print("town_docs table not found in reference db — run processing.build first")
        ref.close(); dclt.close()
        return

    print("Building parcel index...")
    map_lot_index, addr_index = _build_parcel_index(ref)
    print(f"  {len(map_lot_index)} map/lot entries, {len(addr_index)} address entries")

    docs = ref.execute(
        "SELECT doc_id, source_type, full_text FROM town_docs WHERE full_text IS NOT NULL AND full_text != ''"
    ).fetchall()
    print(f"Processing {len(docs)} town docs...")

    inserted = skipped = 0
    for doc in docs:
        candidates = _candidates_for_text(
            doc["full_text"], map_lot_index, addr_index
        )
        for c in candidates:
            try:
                dclt.execute(
                    """INSERT OR IGNORE INTO parcel_links
                       (doc_id, source_type, parcel_id, match_type, match_text, confidence)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (doc["doc_id"], doc["source_type"],
                     c["parcel_id"], c["match_type"], c["match_text"], c["confidence"]),
                )
                if dclt.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  WARN {doc['doc_id']} → {c['parcel_id']}: {e}")

    dclt.commit()
    ref.close(); dclt.close()
    print(f"Done. {inserted} candidates inserted, {skipped} already existed.")


if __name__ == "__main__":
    main()
