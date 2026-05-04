"""Dimension registry — all Tag dimensions, versioned with code.

Each dimension is defined by six CA properties: name, node_type, state space,
default rule, applicability rule, allowed transitions. Dimensions are small
commits, not runtime configuration — adding one here is the authoritative act.

Default and applicability rules read External or Derived Layers only (via
ref_db). They never read Dynamic Layers; that would create a cycle.
"""

ARTICLE97_THRESHOLD = 0.4

_JOIN_STATUS_TO_IDENTITY_STATE = {
    "BOTH":          "OK",
    "ASSESSOR_ONLY": "ADB-only",
    "MASSGIS_ONLY":  "GIS-only",
}

DIMENSIONS: dict[str, dict] = {
    "CoverageDetermination": {
        "node_type":     "parcel",
        "states":        ["Unconfirmed", "Undeveloped", "Underdeveloped", "Developed"],
        "default":       "Unconfirmed",
        "applicability": "all",
        "transitions":   None,          # any-to-any
        "display_order": 300,
    },
    "IdentityResolution": {
        "node_type":     "parcel",
        "states":        ["Unconfirmed", "ADB Add", "ADB Remove", "GIS Add", "GIS Remove"],
        "default":       "Unconfirmed",
        "applicability": "identity_state_not_ok",
        "transitions": {                # constrained by IdentityState (Derived Layer)
            "GIS-only":  {"ADB Add", "GIS Remove"},
            "ADB-only":  {"GIS Add", "ADB Remove"},
        },
        "display_order": 310,
    },
    "Article97Determination": {
        "node_type":     "document",
        "states":        ["Unconfirmed", "Confirmed", "Denied"],
        "default":       "Unconfirmed",
        "applicability": "article97_keyword_hit",
        "transitions":   None,          # any-to-any
        "display_order": 320,
    },
    "FarmingDetermination": {
        "node_type":     "parcel",
        "states":        ["Unconfirmed", "Not Suitable", "Possible", "Suitable"],
        "default":       "Unconfirmed",
        "applicability": "all",
        "transitions":   None,          # any-to-any
        "display_order": 330,
    },
    "AcquisitionDetermination": {
        "node_type":     "parcel",
        "states":        ["Unconfirmed", "Pursue", "Watch", "Pass"],
        "default":       "Unconfirmed",
        "applicability": "acquisition_suitability_active",
        "transitions":   None,          # any-to-any
        "display_order": 340,
    },
}


def check_applicability(
    dim_name: str, target_type: str, target_id: str, ref_db
) -> tuple[bool, str]:
    """Return (applicable, reason). ref_db must be an open reference.db connection."""
    dim = DIMENSIONS.get(dim_name)
    if not dim:
        return True, ""

    rule = dim["applicability"]

    if rule == "all":
        return True, ""

    if rule == "identity_state_not_ok":
        if target_type != "parcel":
            return False, "IdentityResolution applies to parcels only"
        row = ref_db.execute(
            "SELECT join_status FROM parcels WHERE parcel_id = ? LIMIT 1", (target_id,)
        ).fetchone()
        if not row:
            return False, "parcel not found in reference data"
        if row["join_status"] == "BOTH":
            return (
                False,
                "IdentityResolution not applicable — parcel is present in both "
                "Assessor database and GIS (IdentityState = OK)",
            )
        return True, ""

    if rule == "article97_keyword_hit":
        if target_type != "document":
            return False, "Article97Determination applies to documents only"
        try:
            book, page = target_id.split("/", 1)
        except ValueError:
            return False, "invalid document target_id — expected book/page"
        row = ref_db.execute(
            "SELECT kw_article_97 FROM registry_ocr WHERE book = ? AND page = ? LIMIT 1",
            (book, page),
        ).fetchone()
        score = float(row["kw_article_97"]) if row and row["kw_article_97"] is not None else 0.0
        if score < ARTICLE97_THRESHOLD:
            return (
                False,
                f"Article97Determination not applicable — keyword score {score:.2f} "
                f"is below threshold {ARTICLE97_THRESHOLD}",
            )
        return True, ""

    if rule == "acquisition_suitability_active":
        if target_type != "parcel":
            return False, "AcquisitionDetermination applies to parcels only"
        try:
            row = ref_db.execute(
                "SELECT acquisition_suitability FROM parcels WHERE parcel_id = ? LIMIT 1",
                (target_id,),
            ).fetchone()
        except Exception:
            return (
                False,
                "AcquisitionDetermination requires ParcelAcquisitionSuitability "
                "(rebuild reference.db to materialise the layer)",
            )
        if not row:
            return False, "parcel not found in reference data"
        suitability = row["acquisition_suitability"]
        if suitability not in ("Possible", "Likely"):
            return (
                False,
                f"AcquisitionDetermination not applicable — ParcelAcquisitionSuitability "
                f"is '{suitability}' (requires Possible or Likely)",
            )
        return True, ""

    return True, ""


def check_transition(
    dim_name: str, target_type: str, target_id: str, new_state: str, ref_db
) -> tuple[bool, str]:
    """Return (allowed, reason). ref_db must be an open reference.db connection."""
    dim = DIMENSIONS.get(dim_name)
    if not dim or not dim.get("transitions"):
        return True, ""

    if dim_name == "IdentityResolution":
        row = ref_db.execute(
            "SELECT join_status FROM parcels WHERE parcel_id = ? LIMIT 1", (target_id,)
        ).fetchone()
        if not row:
            return True, ""
        identity_state = _JOIN_STATUS_TO_IDENTITY_STATE.get(row["join_status"], "OK")
        allowed = dim["transitions"].get(identity_state)
        if allowed is not None and new_state not in allowed:
            return (
                False,
                f"State '{new_state}' is not allowed when IdentityState is "
                f"'{identity_state}' — permitted states: {sorted(allowed)}",
            )

    return True, ""
