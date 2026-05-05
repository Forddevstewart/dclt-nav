"""Dynamic Layer evaluators.

Dynamic Layers read at least one Tag fold as input. They are never materialized
to storage; they evaluate per request. If a layer becomes a performance problem,
the escalation path is an overnight cache job — the layer's classification
(Dynamic) does not change.

Rule: default and applicability rules on Tag dimensions must not read Dynamic
Layers (would create Tag → Dynamic → Tag cycle). These functions are only
invoked from API endpoints, never from dimensions.py.
"""

from .models import get_db, get_reference_db
from .dimensions import ARTICLE97_THRESHOLD
from .db_utils import FOLD as _FOLD, table_exists as _table_exists


def parcel_coverage_rollup() -> dict:
    """ParcelCoverageRollup: inventory-level percentages by CoverageDetermination state.

    Drives the cover-page hygiene journey. Denominator is all parcels; Unconfirmed
    includes parcels with no tagging event (the work queue).
    """
    from .dimensions import DIMENSIONS
    db  = get_db()
    ref = get_reference_db()

    total = ref.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
    ref.close()

    tag_row = db.execute(
        "SELECT tag_id FROM tags WHERE name = 'CoverageDetermination' AND deprecated_at IS NULL"
    ).fetchone()
    if not tag_row:
        db.close()
        return {"error": "CoverageDetermination dimension not found", "total": total, "by_state": []}

    tag_id = tag_row["tag_id"]
    states = DIMENSIONS["CoverageDetermination"]["states"]

    counts = db.execute(
        f"SELECT t1.state, COUNT(*) n FROM taggings t1"
        f" WHERE t1.target_type = 'parcel' AND t1.tag_id = ?"
        f"   AND t1.state IS NOT NULL AND {_FOLD}"
        f" GROUP BY t1.state",
        (tag_id,),
    ).fetchall()
    db.close()

    by_state = {r["state"]: r["n"] for r in counts}

    # Unconfirmed = total minus any non-Unconfirmed explicit fold, plus explicit Unconfirmed folds
    n_confirmed_states = sum(v for s, v in by_state.items() if s != "Unconfirmed")
    n_unconfirmed = total - n_confirmed_states

    result = []
    for s in states:
        n = n_unconfirmed if s == "Unconfirmed" else by_state.get(s, 0)
        result.append({
            "state": s,
            "n":     n,
            "pct":   round(n / total * 100, 1) if total else 0.0,
        })

    return {"total": total, "by_state": result}


def parcel_article97_rollup() -> dict:
    """ParcelArticle97Rollup: per-parcel Article97Determination fold counts.

    Denominator is applicable documents (kw_article_97 > threshold), not all
    documents — see CA 'Rollup denominators'. Not Applicable count is reported
    separately; it is never folded into the active denominator.
    """
    from collections import defaultdict
    db  = get_db()
    ref = get_reference_db()

    if not _table_exists(ref, "registry_ocr"):
        db.close(); ref.close()
        return {"parcels": [], "note": "registry_ocr not available"}

    tag_row = db.execute(
        "SELECT tag_id FROM tags WHERE name = 'Article97Determination' AND deprecated_at IS NULL"
    ).fetchone()
    if not tag_row:
        db.close(); ref.close()
        return {"parcels": [], "error": "Article97Determination dimension not found"}

    tag_id = tag_row["tag_id"]

    folds = db.execute(
        f"SELECT t1.target_id, t1.state FROM taggings t1"
        f" WHERE t1.target_type = 'document' AND t1.tag_id = ?"
        f"   AND t1.state IS NOT NULL AND {_FOLD}",
        (tag_id,),
    ).fetchall()
    db.close()

    fold_map = {r["target_id"]: r["state"] for r in folds}

    # Applicable = documents where kw_article_97 > threshold
    applicable = ref.execute(
        "SELECT rd.parcel_id, rd.book || '/' || rd.page AS doc_key"
        " FROM registry_documents rd"
        " JOIN registry_ocr ro ON ro.book = rd.book AND ro.page = rd.page"
        " WHERE ro.kw_article_97 > ?",
        (ARTICLE97_THRESHOLD,),
    ).fetchall()
    ref.close()

    parcel_docs: dict[str, list] = defaultdict(list)
    for r in applicable:
        if r["parcel_id"]:
            parcel_docs[r["parcel_id"]].append(r["doc_key"])

    result = []
    for parcel_id, docs in sorted(parcel_docs.items()):
        n_applicable  = len(docs)
        n_confirmed   = sum(1 for d in docs if fold_map.get(d) == "Confirmed")
        n_denied      = sum(1 for d in docs if fold_map.get(d) == "Denied")
        n_unconfirmed = n_applicable - n_confirmed - n_denied
        result.append({
            "parcel_id":     parcel_id,
            "n_applicable":  n_applicable,
            "n_confirmed":   n_confirmed,
            "n_denied":      n_denied,
            "n_unconfirmed": n_unconfirmed,
        })

    return {"parcels": result}
