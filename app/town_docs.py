from collections import defaultdict
from flask import Blueprint, jsonify, abort, send_file, redirect, request
from .models import get_reference_db, get_db
from .db_utils import table_exists

bp = Blueprint("town_docs", __name__, url_prefix="/api")


def _link_counts(cand_parcel_ids: set, adj_map: dict) -> tuple[int, int, int]:
    """Return (n_candidate, n_confirmed, n_rejected) for a single doc."""
    n_confirmed = sum(1 for s in adj_map.values() if s in ("confirmed", "user_manual"))
    n_rejected  = sum(1 for s in adj_map.values() if s == "rejected")
    n_candidate = len(cand_parcel_ids - set(adj_map.keys()))
    return n_candidate, n_confirmed, n_rejected


# ── Town docs overview ────────────────────────────────────────────────────────

@bp.route("/town-docs/overview")
def town_docs_overview():
    db = get_reference_db()
    if not table_exists(db, "town_docs"):
        db.close()
        return jsonify({"total": 0, "by_committee": [], "by_doc_type": []})

    total = db.execute("SELECT COUNT(*) FROM town_docs").fetchone()[0]
    by_committee = [
        dict(r) for r in db.execute(
            "SELECT committee, COUNT(*) n FROM town_docs"
            " GROUP BY committee ORDER BY n DESC"
        ).fetchall()
    ]
    by_doc_type = [
        dict(r) for r in db.execute(
            "SELECT doc_type, COUNT(*) n FROM town_docs"
            " GROUP BY doc_type ORDER BY n DESC"
        ).fetchall()
    ]
    db.close()
    return jsonify({"total": total, "by_committee": by_committee, "by_doc_type": by_doc_type})


# ── Town docs list ────────────────────────────────────────────────────────────

@bp.route("/town-docs")
def town_docs_list():
    """List town docs that have at least one candidate link, with candidate counts."""
    ref  = get_reference_db()
    dclt = get_db()

    if not table_exists(ref, "town_docs"):
        ref.close(); dclt.close()
        return jsonify([])

    has_candidates = table_exists(ref, "parcel_link_candidates")
    committee = request.args.get("committee", "")
    status    = request.args.get("status", "")

    where_td = "WHERE full_text IS NOT NULL AND full_text != ''"
    params: list = []
    if committee:
        where_td += " AND committee = ?"
        params.append(committee)

    rows_td = ref.execute(
        f"SELECT doc_id, source_type, committee, doc_type, meeting_date, page_count"
        f" FROM town_docs {where_td} ORDER BY meeting_date DESC NULLS LAST, committee",
        params,
    ).fetchall()

    if not has_candidates:
        ref.close(); dclt.close()
        result = [dict(r) | {"n_candidate": 0, "n_confirmed": 0, "n_rejected": 0}
                  for r in rows_td]
        return jsonify(result)

    doc_ids = [r["doc_id"] for r in rows_td]
    if not doc_ids:
        ref.close(); dclt.close()
        return jsonify([])

    placeholders = ",".join("?" * len(doc_ids))

    cand_by_doc: dict[str, set] = defaultdict(set)
    for r in ref.execute(
        f"SELECT doc_id, parcel_id FROM parcel_link_candidates WHERE doc_id IN ({placeholders})",
        doc_ids,
    ).fetchall():
        cand_by_doc[r["doc_id"]].add(r["parcel_id"])

    adj_by_doc: dict[str, dict] = defaultdict(dict)
    if table_exists(dclt, "parcel_link_adjudications"):
        for r in dclt.execute(
            f"SELECT doc_id, parcel_id, status FROM parcel_link_adjudications"
            f" WHERE seq IN ("
            f"  SELECT MAX(seq) FROM parcel_link_adjudications"
            f"  WHERE doc_id IN ({placeholders}) GROUP BY doc_id, parcel_id"
            f")",
            doc_ids,
        ).fetchall():
            adj_by_doc[r["doc_id"]][r["parcel_id"]] = r["status"]

    seen_key: dict[tuple, int] = {}
    result = []
    for r in rows_td:
        did = r["doc_id"]
        cands = cand_by_doc.get(did, set())
        adjs  = adj_by_doc.get(did, {})
        n_cand, n_conf, n_rej = _link_counts(cands, adjs)
        if n_cand + n_conf + n_rej == 0:
            continue

        key = (r["committee"], r["meeting_date"])
        if key in seen_key:
            idx = seen_key[key]
            existing = result[idx]
            merged_cands = existing["_cands"] | cands
            merged_adjs  = {**existing["_adjs"], **adjs}
            m_cand, m_conf, m_rej = _link_counts(merged_cands, merged_adjs)
            existing["_cands"]      = merged_cands
            existing["_adjs"]       = merged_adjs
            existing["n_candidate"] = m_cand
            existing["n_confirmed"] = m_conf
            existing["n_rejected"]  = m_rej
            if r["doc_type"] == "Updated":
                existing.update({k: r[k] for k in ("doc_id", "source_type", "doc_type", "page_count")})
        else:
            row = dict(r)
            row.update({"n_candidate": n_cand, "n_confirmed": n_conf, "n_rejected": n_rej,
                        "_cands": cands, "_adjs": adjs})
            seen_key[key] = len(result)
            result.append(row)

    out = []
    for row in result:
        row.pop("_cands"); row.pop("_adjs")
        if status == "candidate" and row["n_candidate"] == 0: continue
        if status == "confirmed" and row["n_confirmed"] == 0: continue
        if status == "rejected"  and row["n_rejected"]  == 0: continue
        out.append(row)

    ref.close(); dclt.close()
    return jsonify(out)


# ── Town doc detail ───────────────────────────────────────────────────────────

@bp.route("/town-docs/<path:doc_id>")
def town_doc_detail(doc_id):
    ref  = get_reference_db()
    dclt = get_db()

    doc = ref.execute(
        "SELECT * FROM town_docs WHERE doc_id = ? LIMIT 1", (doc_id,)
    ).fetchone()
    if not doc:
        ref.close(); dclt.close()
        abort(404)

    candidates = []
    if table_exists(ref, "parcel_link_candidates"):
        candidates = ref.execute(
            "SELECT parcel_id, source_type, match_type, match_text, confidence"
            " FROM parcel_link_candidates WHERE doc_id = ?"
            " ORDER BY confidence DESC, parcel_id",
            (doc_id,),
        ).fetchall()

    adj_map: dict[str, dict] = {}
    if table_exists(dclt, "parcel_link_adjudications"):
        for r in dclt.execute(
            "SELECT parcel_id, status, reviewed_by, reviewed_at"
            " FROM parcel_link_adjudications"
            " WHERE seq IN ("
            "  SELECT MAX(seq) FROM parcel_link_adjudications"
            "  WHERE doc_id = ? GROUP BY doc_id, parcel_id"
            ")",
            (doc_id,),
        ).fetchall():
            adj_map[r["parcel_id"]] = dict(r)

    cand_pids = {c["parcel_id"] for c in candidates}
    links = []
    for c in candidates:
        pid = c["parcel_id"]
        adj = adj_map.get(pid, {})
        links.append({
            "link_id":     doc_id + "|" + pid,
            "parcel_id":   pid,
            "source_type": c["source_type"],
            "match_type":  c["match_type"],
            "match_text":  c["match_text"],
            "confidence":  c["confidence"],
            "status":      adj.get("status", "candidate"),
            "reviewed_by": adj.get("reviewed_by"),
            "reviewed_at": adj.get("reviewed_at"),
        })

    for pid, adj in adj_map.items():
        if pid not in cand_pids and adj["status"] == "user_manual":
            links.append({
                "link_id":     doc_id + "|" + pid,
                "parcel_id":   pid,
                "source_type": adj.get("source_type"),
                "match_type":  "user_manual",
                "match_text":  None,
                "confidence":  1.0,
                "status":      "user_manual",
                "reviewed_by": adj.get("reviewed_by"),
                "reviewed_at": adj.get("reviewed_at"),
            })

    ref.close(); dclt.close()
    return jsonify({"doc": dict(doc), "links": links})


# ── Town doc PDF ──────────────────────────────────────────────────────────────

@bp.route("/town-docs/pdf/<path:doc_id>")
def town_doc_pdf(doc_id):
    ref = get_reference_db()
    try:
        row = ref.execute(
            "SELECT source_path, source_url FROM town_docs WHERE doc_id = ? LIMIT 1", (doc_id,)
        ).fetchone()
    except Exception:
        row = ref.execute(
            "SELECT source_path FROM town_docs WHERE doc_id = ? LIMIT 1", (doc_id,)
        ).fetchone()
    ref.close()
    if not row or not row["source_path"]:
        abort(404)
    from discovery.config import get_config
    pdf_path = get_config().root / "ma-dennis" / row["source_path"]
    if pdf_path.exists():
        return send_file(pdf_path, mimetype="application/pdf")
    source_url = (dict(row).get("source_url") or "").strip()
    if source_url:
        return redirect(source_url)
    abort(404)
