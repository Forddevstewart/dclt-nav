"""Parcel backbone construction and owner/use-code normalization."""

import math
import re
import pandas as pd

from discovery.keywords import KW_KEYS

# ── Use code reference ────────────────────────────────────────────────────────

USE_CODES: dict[str, tuple[str, str]] = {
    "1010": ("Residential - Single Family",             "Residential"),
    "1020": ("Residential - Condominium Unit",          "Residential"),
    "1021": ("Residential - Condo (Ch. 61A)",           "Residential"),
    "1023": ("Residential - Condo (Ch. 61B)",           "Residential"),
    "1030": ("Residential - Multi-Family (2-3 units)",  "Residential"),
    "1040": ("Residential - Multi-Family (4+ units)",   "Residential"),
    "1050": ("Residential - Apartment Complex",         "Residential"),
    "1060": ("Residential - Mobile Home",               "Residential"),
    "1090": ("Residential - Accessory Use",             "Residential"),
    "1110": ("Residential - Accessory Land",            "Residential"),
    "1300": ("Residential - Manufactured Housing",      "Residential"),
    "1320": ("Residential - Condo Common Area",         "Residential"),
    "0130": ("Open Space - Ch. 61B Recreational",       "Agricultural / Open Space"),
    "0131": ("Open Space - Ch. 61B (Partial)",          "Agricultural / Open Space"),
    "0170": ("Open Space - Other",                      "Agricultural / Open Space"),
    "0310": ("Recreational - Ch. 61B",                  "Agricultural / Open Space"),
    "0370": ("Open Space - Other Exempt",               "Agricultural / Open Space"),
    "2010": ("Agricultural - Farming / Crop Land",      "Agricultural / Open Space"),
    "2020": ("Agricultural - Cranberry Bog",            "Agricultural / Open Space"),
    "6010": ("Forest - Ch. 61",                         "Agricultural / Open Space"),
    "6020": ("Agricultural - Ch. 61A",                  "Agricultural / Open Space"),
    "7160": ("Municipal - Conservation",                "Municipal"),
    "7170": ("Municipal - Recreation",                  "Municipal"),
    "9300": ("Municipal - Improved",                    "Municipal"),
    "9320": ("Municipal Vacant - Conservation",         "Municipal"),
    "9380": ("Municipal Vacant - District",             "Municipal"),
    "9390": ("Municipal Improved - District",           "Municipal"),
    "9460": ("Exempt - Recreational Non-profit",        "Exempt / Non-profit"),
    "9500": ("Municipal - Underwater / Tidal Land",     "Municipal"),
    "9510": ("Municipal - Open Water",                  "Municipal"),
    "9520": ("Municipal - Tidal Wetland",               "Municipal"),
    "9530": ("Municipal - Non-tidal Wetland",           "Municipal"),
    "9540": ("Municipal - Pond",                        "Municipal"),
    "9560": ("Municipal - Coastal Wetland",             "Municipal"),
    "9570": ("Municipal - Freshwater Wetland",          "Municipal"),
    "9580": ("Municipal - Floodplain",                  "Municipal"),
    "9590": ("Municipal - Other Open Water",            "Municipal"),
    "9820": ("Exempt - Conservation Trust",             "Exempt / Non-profit"),
}

EXEMPT_USE = {"9460", "9820"}

FARMING_AG_USE_CODES = {"2010", "2020", "6020"}

UNDEVELOPED_STATE_CODES = {
    "1300", "1320", "2010", "2020",
    "3900", "3910", "3920",
    "4400", "4420",
    "9300", "9500",
}

_MUNICIPAL_OWNERS = re.compile(
    r"\b(TOWN OF|COMMONWEALTH|SELECTMEN|SELECTBOARD|BOARD OF|"
    r"DEPARTMENT OF|WATER DISTRICT|FIRE DISTRICT|HOUSING AUTHORITY|"
    r"AFFORDABLE HOUSING|CONSERVATION COMMISSION)\b",
    re.IGNORECASE,
)
_CONSERVATION_OWNERS = re.compile(
    r"\b(CONSERVATION TRUST|LAND TRUST|NATURE CONSERVANCY|AUDUBON)\b",
    re.IGNORECASE,
)


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names, replace spaces and slashes with underscores."""
    df.columns = [
        re.sub(r"[\s/]+", "_", c.lower().strip()) for c in df.columns
    ]
    return df


def owner_category(name: str) -> str:
    if not name:
        return ""
    if _MUNICIPAL_OWNERS.search(name):
        n = name.upper()
        if "WATER DISTRICT" in n:    return "Water District"
        if "FIRE DISTRICT" in n:     return "Fire District"
        if "HOUSING AUTHORITY" in n or "AFFORDABLE HOUSING" in n:
            return "Housing Authority"
        if "CONSERVATION COMMISSION" in n: return "Conservation Commission"
        if "COMMONWEALTH" in n:      return "Commonwealth"
        return "Town of Dennis"
    if _CONSERVATION_OWNERS.search(name):
        return "Conservation Land Trust" if "LAND TRUST" in name.upper() else "Conservation Trust"
    return ""


def build_parcels(engine) -> int:
    assessor = pd.read_sql("SELECT * FROM assessor", engine)
    massgis  = pd.read_sql("SELECT * FROM massgis",  engine)

    assessor["parcel_id"] = (
        assessor["map"].str.strip() + "-" + assessor["parcel"].str.strip()
    )
    ext = assessor["extension"].astype(str).str.strip()
    assessor["unit_key"] = assessor["parcel_id"].where(
        ext.isin(["", "0"]),
        assessor["parcel_id"] + "-" + ext,
    )
    assessor.to_sql("layer_assessor", engine, if_exists="replace", index=False)

    unit_counts = (
        assessor.loc[~ext.isin(["", "0"]), ["parcel_id"]]
        .groupby("parcel_id").size()
        .rename("condo_units").reset_index()
    )

    parents = assessor[ext == "0"].copy()
    parents["backbone_source"] = "parent"

    has_parent = set(parents["parcel_id"])
    orphans = assessor[~ext.isin(["", "0"]) & ~assessor["parcel_id"].isin(has_parent)].copy()

    if not orphans.empty:
        def _ext_order(e):
            try: return int(e)
            except (ValueError, TypeError): return 999
        orphans["_ext_order"] = orphans["extension"].apply(_ext_order)
        synthesized = (
            orphans.sort_values("_ext_order")
            .drop_duplicates(subset=["parcel_id"], keep="first")
            .drop(columns=["_ext_order"])
        )
        synthesized["backbone_source"] = "synthesized"
        backbone = pd.concat([parents, synthesized], ignore_index=True)
    else:
        backbone = parents

    backbone = backbone.merge(unit_counts, on="parcel_id", how="left")
    backbone["condo_units"] = backbone["condo_units"].fillna(0).astype(int)

    def _parcel_class(gisid) -> str:
        g = str(gisid or "").strip()
        if not g or g == "nan":
            return "no-gisid"
        if g.startswith("X_"):
            return "special-feature"
        return "standard"

    backbone["parcel_class"] = backbone["gisid"].apply(_parcel_class)

    massgis = massgis.rename(columns={"map_par_id": "parcel_id"})
    massgis["_lot_num"] = pd.to_numeric(massgis["lot_size"], errors="coerce").fillna(0)
    layer_massgis = (
        massgis.sort_values("_lot_num", ascending=False)
        .drop_duplicates(subset=["parcel_id"], keep="first")
        .drop(columns=["_lot_num"])
    )
    layer_massgis.to_sql("layer_massgis", engine, if_exists="replace", index=False)

    parcels = backbone.merge(
        layer_massgis, on="parcel_id", how="outer", suffixes=("", "_gis"), indicator=True,
    )
    parcels["join_status"] = parcels["_merge"].map({
        "both":       "BOTH",
        "left_only":  "ASSESSOR_ONLY",
        "right_only": "MASSGIS_ONLY",
    })
    parcels = parcels.drop(columns=["_merge"])

    _gis_loc_ids     = set(layer_massgis["loc_id"].dropna())
    # Raw massgis preserves all loc_ids including blank-map_par_id rows
    # that layer_massgis deduplication would collapse to a single entry.
    _gis_loc_ids_raw = set(massgis["loc_id"].dropna())
    # parcel_id → current loc_id for distance calculation on drift parcels
    _gis_par_to_loc  = dict(zip(
        layer_massgis["parcel_id"].fillna(""),
        layer_massgis["loc_id"].fillna(""),
    ))

    def _parse_f(fid: str):
        try:
            _, x, y = fid.split("_")
            return float(x), float(y)
        except Exception:
            return None

    def _drift_class(dist: float) -> str:
        if dist < 10:  return "drift-minor"
        if dist < 50:  return "drift-moderate"
        return "drift-major"

    def _gisid_status(row) -> str:
        g = str(row.get("gisid") or "").strip()
        if not g or g == "nan":
            return "no-gisid"
        if g.startswith("X_"):
            return "special-feature"
        js = row.get("join_status", "")
        if js == "BOTH":
            if g in _gis_loc_ids:
                return "matches"
            cur = _gis_par_to_loc.get(str(row.get("parcel_id") or ""), "")
            old_c, new_c = _parse_f(g), _parse_f(cur)
            if old_c and new_c:
                return _drift_class(math.sqrt((old_c[0]-new_c[0])**2 + (old_c[1]-new_c[1])**2))
            return "drift-major"
        return "missing"

    parcels["parcel_gisid_status"] = parcels.apply(_gisid_status, axis=1)
    # MASSGIS_ONLY rows have no assessor record, so parcel_class is NaN from the merge
    parcels["parcel_class"] = parcels["parcel_class"].fillna("no-gisid")

    def _massgis_status(row) -> str:
        pc = str(row.get("parcel_class") or "no-gisid")
        js = row.get("join_status", "")
        if pc == "special-feature":
            return "special-feature"
        if js == "MASSGIS_ONLY":
            return "unmatched-polygon"
        if js == "BOTH":
            return "ok"
        # ASSESSOR_ONLY
        if pc == "no-gisid":
            return "no-gisid"
        # standard ASSESSOR_ONLY: gisid in raw GIS means map_par_id is blank/wrong;
        # use raw massgis loc_ids because layer_massgis deduplication collapses
        # blank-map_par_id rows to a single entry, losing other loc_ids.
        g = str(row.get("gisid") or "").strip()
        if g and g != "nan" and g in _gis_loc_ids_raw:
            return "blank-map-par-id"
        return "absent"

    parcels["parcel_massgis_status"] = parcels.apply(_massgis_status, axis=1)

    def _use_code_norm(row) -> str:
        def _parse(v) -> str:
            s = str(v or "").strip()
            if not s or s == "nan":
                return ""
            try:
                return str(int(float(s))).zfill(4)
            except (ValueError, TypeError):
                return s.zfill(4) if len(s) <= 4 else s
        sc = _parse(row.get("stateclass") or row.get("state_class") or "")
        return sc if sc else _parse(row.get("use_code") or "")

    parcels["use_code_norm"] = parcels.apply(_use_code_norm, axis=1)

    use_desc_map   = {k: v[0] for k, v in USE_CODES.items()}
    prop_class_map = {k: v[1] for k, v in USE_CODES.items()}
    parcels["use_code_desc"]  = parcels["use_code_norm"].map(use_desc_map).fillna("")
    parcels["property_class"] = parcels["use_code_norm"].map(prop_class_map).fillna("Other")

    parcels["owner_name"]     = parcels.get("name1", pd.Series("", index=parcels.index)).fillna("").str.strip()
    parcels["owner_category"] = parcels["owner_name"].apply(owner_category)

    municipal_cats = {"Town of Dennis", "Commonwealth", "Conservation Commission",
                      "Housing Authority", "Water District", "Fire District"}
    parcels.loc[parcels["owner_category"].isin(municipal_cats), "property_class"] = "Municipal"

    parcels["is_public"] = (
        (parcels["property_class"] == "Municipal") &
        (~parcels["use_code_norm"].isin(EXEMPT_USE))
    ).astype(int)

    parcels["use_code"] = parcels["use_code_norm"]
    parcels = parcels.rename(columns={"gisid": "parcel_adb_gisid"})

    display_cols = [
        "parcel_id", "join_status", "backbone_source", "condo_units",
        "owner_name", "owner_category",
        "locno", "locst", "village",
        "site_addr",
        "booklast", "pagelast",
        "use_code", "use_code_norm", "use_code_desc", "property_class", "is_public",
        "billingacres", "totalapprvalue", "zonedesc",
        "centroid_lat", "centroid_lon",
        "parcel_adb_gisid", "parcel_class", "parcel_gisid_status", "parcel_massgis_status",
    ]
    keep = [c for c in display_cols if c in parcels.columns]
    parcels[keep].to_sql("parcels", engine, if_exists="replace", index=False)

    return len(parcels)
