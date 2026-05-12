"""Derived Layer computations — write materialized columns to the parcels table."""

from sqlalchemy import create_engine, text

from .normalize import FARMING_AG_USE_CODES, UNDEVELOPED_STATE_CODES

# ── Threshold constants ───────────────────────────────────────────────────────
# All band-boundary decisions collected here so calibration changes are one-line edits.

_AS_COVERAGE_LIKELY   = 0.25
_AS_COVERAGE_POSSIBLE = 0.60
_AS_ACRES_LARGE       = 2.0


def _table_exists(engine, name: str) -> bool:
    with engine.connect() as con:
        return con.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        ).fetchone() is not None


def _as_present(v) -> bool:
    return v is not None and str(v).strip() not in ("", "0", "nan")


def _as_conservation_value(row: dict) -> int:
    """Proxy for ParcelConservationValue (future Derived Layer). Returns 0/1/2."""
    if _as_present(row.get("bm3_ch_id")) or _as_present(row.get("bm3_cnl_id")):
        return 2
    if (
        _as_present(row.get("prihab_id"))
        or _as_present(row.get("bm3_wc_id"))
        or _as_present(row.get("bm3_vp_id"))
        or _as_present(row.get("zone2_id"))
        or _as_present(row.get("os_site_name"))
    ):
        return 1
    try:
        acres = float(row.get("billingacres") or 0)
        ratio = float(row["coverage_ratio"]) if row.get("coverage_ratio") is not None else 1.0
        if acres >= _AS_ACRES_LARGE and ratio < _AS_COVERAGE_LIKELY:
            return 1
    except (TypeError, ValueError):
        pass
    return 0


def _as_development_pressure(row: dict) -> int:
    """Proxy for ParcelDevelopmentPotential (future Derived Layer). Returns 0/1/2."""
    try:
        ratio = row.get("coverage_ratio")
        if ratio is None:
            return 0
        ratio = float(ratio)
        if ratio < _AS_COVERAGE_LIKELY:
            return 0
        if ratio < _AS_COVERAGE_POSSIBLE:
            return 1
        return 2
    except (TypeError, ValueError):
        return 0


# ── Compute functions ─────────────────────────────────────────────────────────

def compute_coverage(engine) -> int:
    """Write coverage_ratio and coverage_status to the parcels table."""
    if not _table_exists(engine, "parcels_gis"):
        print("  SKIP — parcels_gis not found")
        return 0

    with engine.begin() as con:
        con.execute(text("ALTER TABLE parcels ADD COLUMN coverage_ratio REAL"))
        con.execute(text("ALTER TABLE parcels ADD COLUMN coverage_status TEXT"))

        rows = con.execute(text("""
            SELECT p.parcel_id, p.billingacres, g.struct_total_sqft
            FROM parcels p
            LEFT JOIN parcels_gis g ON g.parcel_id = p.parcel_id
        """)).fetchall()

    updates = []
    for parcel_id, billing_acres, struct_sqft in rows:
        try:
            acres = float(billing_acres)
            if acres <= 0:
                acres = None
        except (TypeError, ValueError):
            acres = None

        try:
            sqft = float(struct_sqft)
        except (TypeError, ValueError):
            sqft = None

        if acres is None:
            ratio  = None
            status = "no_structure" if sqft is None else "no_acreage"
        elif sqft is None:
            ratio  = 0.0
            status = "no_structure"
        else:
            ratio = sqft / (acres * 43560.0)
            if ratio > 1.0:
                ratio  = 1.0
                status = "data_issue"
            else:
                status = "ok"

        updates.append({"r": ratio, "s": status, "pid": parcel_id})

    with engine.begin() as con:
        con.execute(
            text("UPDATE parcels SET coverage_ratio = :r, coverage_status = :s WHERE parcel_id = :pid"),
            updates,
        )

    n_ok    = sum(1 for u in updates if u["s"] == "ok")
    n_issue = sum(1 for u in updates if u["s"] == "data_issue")
    n_other = len(updates) - n_ok - n_issue
    print(f"    coverage: {n_ok} ok, {n_issue} data_issue, {n_other} indeterminate")
    return len(updates)


def compute_undeveloped_state_code(engine) -> int:
    """Write is_undeveloped_state_code (0/1) to the parcels table."""
    in_list = ", ".join(f"'{c}'" for c in sorted(UNDEVELOPED_STATE_CODES))
    with engine.begin() as con:
        con.execute(text(
            "ALTER TABLE parcels ADD COLUMN is_undeveloped_state_code INTEGER NOT NULL DEFAULT 0"
        ))
        con.execute(text(f"""
            UPDATE parcels
               SET is_undeveloped_state_code = CASE
                     WHEN use_code_norm IN ({in_list}) THEN 1
                     ELSE 0
                   END
        """))
    with engine.connect() as con:
        n = con.execute(text(
            "SELECT COUNT(*) FROM parcels WHERE is_undeveloped_state_code = 1"
        )).scalar()
    print(f"    undeveloped_state_code: {n} parcels flagged")
    return n


def compute_farming_suitability(engine) -> int:
    """Write farming_suitability {Not suitable, Possible, Likely} to the parcels table."""
    if not _table_exists(engine, "layer_soils"):
        print("  SKIP — layer_soils not found (farming_suitability not computed)")
        return 0

    with engine.begin() as con:
        con.execute(text("ALTER TABLE parcels ADD COLUMN farming_suitability TEXT"))

    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT p.parcel_id, p.use_code_norm,
                   s.prime, s.statewide, s."unique"
            FROM parcels p
            LEFT JOIN layer_soils s ON s.parcel_id = p.parcel_id
        """)).fetchall()

    updates = []
    for parcel_id, use_code, prime, statewide, unique_ in rows:
        if prime:
            state = "Likely"
        elif statewide or unique_ or use_code in FARMING_AG_USE_CODES:
            state = "Possible"
        else:
            state = "Not suitable"
        updates.append({"state": state, "pid": parcel_id})

    with engine.begin() as con:
        con.execute(
            text("UPDATE parcels SET farming_suitability = :state WHERE parcel_id = :pid"),
            updates,
        )

    counts: dict[str, int] = {}
    for u in updates:
        counts[u["state"]] = counts.get(u["state"], 0) + 1
    summary = ", ".join(f"{counts[s]} {s}" for s in ["Likely", "Possible", "Not suitable"] if s in counts)
    print(f"    farming_suitability: {summary}")
    return len(updates)


def compute_acquisition_suitability(engine) -> int:
    """Write acquisition_suitability {Not suitable, Possible, Likely} to the parcels table."""
    if not _table_exists(engine, "parcels_gis"):
        print("  SKIP — parcels_gis not found (acquisition_suitability not computed)")
        return 0

    with engine.begin() as con:
        con.execute(text("ALTER TABLE parcels ADD COLUMN acquisition_suitability TEXT"))

    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT
                p.parcel_id,
                p.billingacres,
                p.coverage_ratio,
                p.coverage_status,
                p.join_status,
                p.parcel_class,
                g.bm3_ch_id,    g.bm3_ch_acres,
                g.bm3_cnl_id,   g.bm3_cnl_acres,
                g.bm3_wc_id,    g.bm3_wc_acres,
                g.bm3_vp_id,    g.bm3_vp_acres,
                g.prihab_id,
                g.zone2_id,     g.zone2_acres,
                g.os_site_name, g.os_acres
            FROM parcels p
            LEFT JOIN parcels_gis g ON g.parcel_id = p.parcel_id
        """)).fetchall()

    updates = []
    for row in rows:
        r = row._mapping
        cv = _as_conservation_value(r)
        dp = _as_development_pressure(r)
        identity_ok = r.get("join_status") == "BOTH" or r.get("parcel_class") == "special-feature"

        if cv == 2 and dp == 0 and identity_ok:
            state = "Likely"
        elif cv >= 1:
            state = "Possible"
        else:
            state = "Not suitable"

        updates.append({"state": state, "pid": r["parcel_id"]})

    with engine.begin() as con:
        con.execute(
            text("UPDATE parcels SET acquisition_suitability = :state WHERE parcel_id = :pid"),
            updates,
        )

    counts: dict[str, int] = {}
    for u in updates:
        counts[u["state"]] = counts.get(u["state"], 0) + 1
    summary = ", ".join(
        f"{counts[s]} {s}" for s in ["Likely", "Possible", "Not suitable"] if s in counts
    )
    print(f"    acquisition_suitability: {summary}")
    return len(updates)


def compute_parcel_acq_layers(engine) -> int:
    """Write CA-named acquisition suitability input columns to the parcels table."""
    if not _table_exists(engine, "parcels_gis"):
        print("  SKIP — parcels_gis not found (parcel_acq_layers not computed)")
        return 0

    with engine.begin() as con:
        for col, dtype in [
            ("os_acres",        "REAL"),
            ("bm3_core_acres",  "REAL"),
            ("bm3_cnl_acres",   "REAL"),
            ("bm3_local_acres", "REAL"),
            ("phrs_present",    "INTEGER"),
            ("zone2_acres",     "REAL"),
            ("vp_present",      "INTEGER"),
        ]:
            con.execute(text(f"ALTER TABLE parcels ADD COLUMN {col} {dtype}"))

        def _ac(col: str) -> str:
            return f"CAST(NULLIF(TRIM(g.{col}), '') AS REAL)"

        con.execute(text(f"""
            UPDATE parcels SET
                os_acres        = (SELECT {_ac('os_acres')}
                                   FROM parcels_gis g WHERE g.parcel_id = parcels.parcel_id),
                bm3_core_acres  = (SELECT {_ac('bm3_ch_acres')}
                                   FROM parcels_gis g WHERE g.parcel_id = parcels.parcel_id),
                bm3_cnl_acres   = (SELECT {_ac('bm3_cnl_acres')}
                                   FROM parcels_gis g WHERE g.parcel_id = parcels.parcel_id),
                bm3_local_acres = (SELECT {_ac('bm3_wc_acres')}
                                   FROM parcels_gis g WHERE g.parcel_id = parcels.parcel_id),
                zone2_acres     = (SELECT {_ac('zone2_acres')}
                                   FROM parcels_gis g WHERE g.parcel_id = parcels.parcel_id),
                phrs_present    = (SELECT CASE WHEN g.prihab_id IS NOT NULL AND g.prihab_id != ''
                                               THEN 1 ELSE 0 END
                                   FROM parcels_gis g WHERE g.parcel_id = parcels.parcel_id),
                vp_present      = (SELECT CASE WHEN g.bm3_vp_id IS NOT NULL AND g.bm3_vp_id != ''
                                               THEN 1 ELSE 0 END
                                   FROM parcels_gis g WHERE g.parcel_id = parcels.parcel_id)
        """))

    counts: dict[str, int] = {}
    with engine.connect() as con:
        for col, pred in [
            ("os_acres",        "IS NOT NULL"),
            ("bm3_core_acres",  "IS NOT NULL"),
            ("bm3_cnl_acres",   "IS NOT NULL"),
            ("bm3_local_acres", "IS NOT NULL"),
            ("zone2_acres",     "IS NOT NULL"),
            ("phrs_present",    "= 1"),
            ("vp_present",      "= 1"),
        ]:
            n = con.execute(text(f"SELECT COUNT(*) FROM parcels WHERE {col} {pred}")).scalar()
            counts[col] = n

    summary = ", ".join(f"{n} {col}" for col, n in counts.items() if n)
    print(f"    parcel_acq_layers: {summary or '(no overlaps found)'}")
    return sum(counts.values())
