from flask import Blueprint, jsonify
from .models import get_reference_db, get_db
from .db_utils import FOLD

bp = Blueprint("reports", __name__, url_prefix="/api/reports")

_ASSESSOR_VERDICTS = {"ADB Add", "ADB Remove"}
_GIS_VERDICTS      = {"GIS Add",  "GIS Remove"}


@bp.route("/identity")
def identity_report():
    ref = get_reference_db()
    tx  = get_db()

    # ── Assessor: GISID refresh ───────────────────────────────────────────────
    gisid_refresh = [dict(r) for r in ref.execute("""
        SELECT p.parcel_id, p.site_addr, p.owner_name,
               p.parcel_adb_gisid      AS adb_gisid,
               p.parcel_gisid_status   AS drift_class,
               lm.loc_id               AS current_loc_id
        FROM   parcels p
        JOIN   layer_massgis lm ON lm.parcel_id = p.parcel_id
        WHERE  p.parcel_gisid_status IN ('drift-minor','drift-moderate','drift-major')
        ORDER  BY p.parcel_gisid_status DESC, p.parcel_id
    """).fetchall()]

    # ── GIS: corrections (absent + blank-map-par-id + unmatched-polygon) ──────
    corrections = [dict(r) for r in ref.execute("""
        SELECT p.parcel_id,
               p.site_addr,
               p.owner_name,
               p.parcel_massgis_status                          AS status,
               p.parcel_adb_gisid                               AS adb_gisid,
               lm.parcel_id                                     AS gis_map_par_id
        FROM   parcels p
        LEFT   JOIN layer_massgis lm ON lm.loc_id = p.parcel_adb_gisid
        WHERE  p.parcel_massgis_status IN ('absent', 'blank-map-par-id', 'unmatched-polygon')
        ORDER  BY p.parcel_massgis_status, p.parcel_id
    """).fetchall()]

    # ── IdentityResolution campaign verdicts ──────────────────────────────────
    tag_row = tx.execute(
        "SELECT tag_id FROM tags"
        " WHERE name = 'IdentityResolution' AND deprecated_at IS NULL LIMIT 1"
    ).fetchone()

    identity_verdicts = []
    if tag_row:
        tag_id = tag_row["tag_id"]
        rows = tx.execute(
            f"SELECT t1.target_id, t1.state FROM taggings t1"
            f" WHERE t1.target_type = 'parcel' AND t1.tag_id = ?"
            f"   AND t1.state IS NOT NULL AND t1.state != 'Unconfirmed'"
            f"   AND {FOLD}",
            (tag_id,),
        ).fetchall()
        parcel_ids = [r["target_id"] for r in rows]
        states     = {r["target_id"]: r["state"] for r in rows}

        if parcel_ids:
            placeholders = ",".join("?" * len(parcel_ids))
            parcels = {
                r["parcel_id"]: dict(r) for r in ref.execute(
                    f"SELECT parcel_id, site_addr, owner_name FROM parcels"
                    f" WHERE parcel_id IN ({placeholders})",
                    parcel_ids,
                ).fetchall()
            }
            for pid in parcel_ids:
                p = parcels.get(pid, {})
                identity_verdicts.append({
                    "parcel_id":  pid,
                    "site_addr":  p.get("site_addr"),
                    "owner_name": p.get("owner_name"),
                    "verdict":    states[pid],
                })

    ref.close()
    tx.close()

    return jsonify({
        "assessor": {
            "gisid_refresh":     gisid_refresh,
            "identity_verdicts": [v for v in identity_verdicts if v["verdict"] in _ASSESSOR_VERDICTS],
        },
        "gis": {
            "corrections":       corrections,
            "identity_verdicts": [v for v in identity_verdicts if v["verdict"] in _GIS_VERDICTS],
        },
    })
