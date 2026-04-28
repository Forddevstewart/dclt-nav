"""
Extract parcel references from town_docs and write candidate links to raw.db.

Run after processing.build (which populates town_docs) — or called automatically
as part of the build pipeline via load_parcel_link_candidates(engine).

Two patterns:
  ocr_map_lot  — "Assessor's Map 294, Parcel 25"  →  parcel_id 294-25   confidence 0.95
  ocr_address  — "648 Setucket Road"              →  fuzzy address match  confidence 0.70–0.85

Usage:
    python3 -m processing.town_doc_candidates
"""

import re
import sqlite3
from pathlib import Path

from sqlalchemy import text

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

def _build_parcel_index(conn: sqlite3.Connection) -> tuple[dict, dict]:
    rows = conn.execute(
        "SELECT parcel_id, locno, locst FROM parcels WHERE locno IS NOT NULL AND locst IS NOT NULL"
    ).fetchall()

    map_lot_index: dict[tuple, str] = {}
    addr_index:    dict[tuple, str] = {}

    for r in rows:
        pid = r["parcel_id"]
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
    seen: dict[str, dict] = {}

    def _add(pid: str, match_type: str, match_text: str, conf: float) -> None:
        if pid not in seen or seen[pid]["confidence"] < conf:
            seen[pid] = {
                "parcel_id":  pid,
                "match_type": match_type,
                "match_text": match_text[:200],
                "confidence": conf,
            }

    for m in _MAP_LOT.finditer(text):
        map_n = m.group(1).lstrip("0") or "0"
        lot_n = m.group(2).lstrip("0") or "0"
        pid   = map_lot_index.get((map_n, lot_n))
        if pid:
            _add(pid, "ocr_map_lot", m.group(0), 0.95)

    for m in _ADDR.finditer(text):
        locno = m.group(1)
        locst = _norm_street(m.group(2))
        pid   = addr_index.get((locno, locst))
        if pid:
            _add(pid, "ocr_address", m.group(0), 0.75)

    return list(seen.values())


# ── SQLAlchemy entry point (called from build.py) ────────────────────────────

def load_parcel_link_candidates(engine) -> int:
    """
    Populate parcel_link_candidates in raw.db from town_docs OCR text.
    Called as a build pipeline step after load_town_docs.
    """
    with engine.begin() as con:
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS parcel_link_candidates (
                doc_id       TEXT NOT NULL,
                parcel_id    TEXT NOT NULL,
                source_type  TEXT NOT NULL,
                match_type   TEXT,
                match_text   TEXT,
                confidence   REAL,
                PRIMARY KEY (doc_id, parcel_id)
            )
        """))
        con.execute(text("CREATE INDEX IF NOT EXISTS idx_plc_doc ON parcel_link_candidates (doc_id)"))
        con.execute(text("CREATE INDEX IF NOT EXISTS idx_plc_parcel ON parcel_link_candidates (parcel_id)"))
        con.execute(text("DELETE FROM parcel_link_candidates"))

    raw_path = str(engine.url).replace("sqlite:///", "")
    conn = sqlite3.connect(raw_path)
    conn.row_factory = sqlite3.Row

    map_lot_index, addr_index = _build_parcel_index(conn)

    docs = conn.execute(
        "SELECT doc_id, source_type, full_text FROM town_docs"
        " WHERE full_text IS NOT NULL AND full_text != ''"
    ).fetchall()

    inserted = 0
    for doc in docs:
        candidates = _candidates_for_text(doc["full_text"], map_lot_index, addr_index)
        for c in candidates:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO parcel_link_candidates"
                    " (doc_id, parcel_id, source_type, match_type, match_text, confidence)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (doc["doc_id"], c["parcel_id"], doc["source_type"],
                     c["match_type"], c["match_text"], c["confidence"]),
                )
                if conn.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
            except Exception as e:
                print(f"  WARN {doc['doc_id']} → {c['parcel_id']}: {e}")

    conn.commit()
    conn.close()
    return inserted


# ── Standalone entry point ────────────────────────────────────────────────────

def main() -> None:
    cfg      = get_config()
    raw_path = cfg.db_path("raw")

    if not raw_path.exists():
        print(f"raw.db not found: {raw_path} — run processing.build first")
        return

    from sqlalchemy import create_engine
    engine = create_engine(f"sqlite:///{raw_path}")
    n = load_parcel_link_candidates(engine)
    engine.dispose()
    print(f"Done. {n} candidates inserted.")


if __name__ == "__main__":
    main()
