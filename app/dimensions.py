"""Dimension registry — all Tag dimensions, versioned with code.

Each dimension is defined by the CA properties: name, node_type, state space,
default rule, applicability rule, allowed transitions. Adding a dimension here
is the authoritative act. Default and applicability rules read External or
Derived Layers only (via ref_db). They never read Dynamic Layers; that would
create a cycle.
"""

from __future__ import annotations
from dataclasses import dataclass, field

ARTICLE97_THRESHOLD = 0.4

JOIN_STATUS_TO_IDENTITY_STATE = {
    "BOTH":          "OK",
    "ASSESSOR_ONLY": "ADB-only",
    "MASSGIS_ONLY":  "GIS-only",
}


@dataclass
class Dimension:
    name: str
    node_type: str
    states: list[str]
    default: str
    display_order: int
    transitions: dict[str, set[str]] | None = None

    def is_applicable(self, target_type: str, target_id: str, ref_db) -> tuple[bool, str]:
        return True, ""

    def is_transition_allowed(self, target_id: str, new_state: str, ref_db) -> tuple[bool, str]:
        return True, ""


class _CoverageDetermination(Dimension):
    pass  # applicability = all, transitions = any-to-any


class _FarmingDetermination(Dimension):
    pass  # applicability = all, transitions = any-to-any


class _IdentityResolution(Dimension):
    def is_applicable(self, target_type: str, target_id: str, ref_db) -> tuple[bool, str]:
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

    def is_transition_allowed(self, target_id: str, new_state: str, ref_db) -> tuple[bool, str]:
        if not self.transitions:
            return True, ""
        row = ref_db.execute(
            "SELECT join_status FROM parcels WHERE parcel_id = ? LIMIT 1", (target_id,)
        ).fetchone()
        if not row:
            return True, ""
        identity_state = JOIN_STATUS_TO_IDENTITY_STATE.get(row["join_status"], "OK")
        allowed = self.transitions.get(identity_state)
        if allowed is not None and new_state not in allowed:
            return (
                False,
                f"State '{new_state}' is not allowed when IdentityState is "
                f"'{identity_state}' — permitted states: {sorted(allowed)}",
            )
        return True, ""


class _Article97Determination(Dimension):
    def is_applicable(self, target_type: str, target_id: str, ref_db) -> tuple[bool, str]:
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


class _AcquisitionDetermination(Dimension):
    def is_applicable(self, target_type: str, target_id: str, ref_db) -> tuple[bool, str]:
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


DIMENSIONS: dict[str, Dimension] = {
    "CoverageDetermination": _CoverageDetermination(
        name="CoverageDetermination",
        node_type="parcel",
        states=["Unconfirmed", "Undeveloped", "Underdeveloped", "Developed"],
        default="Unconfirmed",
        display_order=300,
    ),
    "IdentityResolution": _IdentityResolution(
        name="IdentityResolution",
        node_type="parcel",
        states=["Unconfirmed", "ADB Add", "ADB Remove", "GIS Add", "GIS Remove"],
        default="Unconfirmed",
        display_order=310,
        transitions={
            "GIS-only":  {"ADB Add", "GIS Remove"},
            "ADB-only":  {"GIS Add", "ADB Remove"},
        },
    ),
    "Article97Determination": _Article97Determination(
        name="Article97Determination",
        node_type="document",
        states=["Unconfirmed", "Confirmed", "Denied"],
        default="Unconfirmed",
        display_order=320,
    ),
    "FarmingDetermination": _FarmingDetermination(
        name="FarmingDetermination",
        node_type="parcel",
        states=["Unconfirmed", "Not Suitable", "Possible", "Suitable"],
        default="Unconfirmed",
        display_order=330,
    ),
    "AcquisitionDetermination": _AcquisitionDetermination(
        name="AcquisitionDetermination",
        node_type="parcel",
        states=["Unconfirmed", "Pursue", "Watch", "Pass"],
        default="Unconfirmed",
        display_order=340,
    ),
}


# ── Module-level API (called from tags.py) ────────────────────────────────────

def check_applicability(
    dim_name: str, target_type: str, target_id: str, ref_db
) -> tuple[bool, str]:
    dim = DIMENSIONS.get(dim_name)
    if not dim:
        return True, ""
    return dim.is_applicable(target_type, target_id, ref_db)


def check_transition(
    dim_name: str, target_type: str, target_id: str, new_state: str, ref_db
) -> tuple[bool, str]:
    dim = DIMENSIONS.get(dim_name)
    if not dim:
        return True, ""
    return dim.is_transition_allowed(target_id, new_state, ref_db)
