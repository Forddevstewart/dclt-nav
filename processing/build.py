"""
Processing — Build CivicTwin/db/dennis.db from CivicTwin source data.

All columns from every source are loaded as-is; no cherry-picking.
Schema is inferred by pandas from the data at load time.

Sources (resolved from discovery/sources.yaml):
  assessor/          Annual ADB Excel extract (BT_Extract sheet)
  gis/               MassGIS GeoJSON parcel polygons
  ma-dennis.town_meeting_all_years.csv   Town meeting warrant articles (if present)
  gis/dennis_*.csv   MassGIS GIS layer join CSVs — merged into parcels_gis (if present)
  registry/index/    Per-parcel deed index JSON files (if present)

Raw tables (one-to-one with source files):
  assessor            All columns from Excel
  massgis             All columns from GeoJSON feature properties
  layer_soils         Farmland classification flags per parcel (prime/statewide/unique/not_prime)
  warrants            All columns from articles.csv
  registry_documents  All fields from per-parcel documents.json

Normalized tables:
  parcels             Backbone — one row per land parcel, lean display fields only
  layer_assessor      All assessor rows; parcel_id (2-part) + unit_key (3-part) added
  layer_massgis       Deduplicated massgis — one row per parcel_id (largest polygon)
  parcels_gis         GIS overlay attributes — one row per parcel, all layer columns merged
  schema_columns      Data dictionary — table/column metadata from schema_columns.csv
  ref_use_codes       MA land use code reference (hardcoded)
  _pipeline_runs      Audit log

Usage:
    python3 -m processing.build
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from discovery.config import get_config

KW_KEYS: list[str] = [
    "article_97",
    "ccr",
    "chapter_61",
    "deed_restriction",
    "conservation_restriction",
    "agricultural_preservation_restriction",
    "perpetual_restriction",
]

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names, replace spaces and slashes with underscores."""
    df.columns = [
        re.sub(r"[\s/]+", "_", c.lower().strip()) for c in df.columns
    ]
    return df


def _owner_category(name: str) -> str:
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


def _stage(engine, name: str, source: Path | None, fn) -> int:
    print(f"\n[{name}]")
    n = fn(engine)
    with engine.begin() as con:
        con.execute(text(
            "INSERT INTO _pipeline_runs (stage, source_file, rows_loaded, run_at)"
            " VALUES (:s, :f, :n, :t)"
        ), {"s": name, "f": str(source) if source else "", "n": n, "t": now_utc()})
    print(f"  → {n} rows")
    return n


# ── Source loaders ────────────────────────────────────────────────────────────

def load_assessor(engine, path: Path) -> int:
    df = pd.read_excel(path, sheet_name="BT_Extract")
    df = _norm_cols(df)
    df["_source_file"] = str(path)
    df["_loaded_at"] = now_utc()
    # Normalize key fields that feed parcel_id construction
    for col in ("map", "block", "parcel", "extension",
                "book_last", "page_last", "book_prev", "page_prev",
                "state_class", "use_code", "gis_id"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", "")
    df.to_sql("assessor", engine, if_exists="replace", index=False)
    return len(df)


def load_massgis(engine, path: Path) -> int:
    data = json.loads(path.read_text())
    rows = [f["properties"] for f in data.get("features", [])]
    df = pd.DataFrame(rows)
    df = _norm_cols(df)
    df["_source_file"] = str(path)
    df["_loaded_at"] = now_utc()
    df.to_sql("massgis", engine, if_exists="replace", index=False)
    return len(df)


def load_warrants(engine, path: Path) -> int:
    if not path.exists():
        print("  SKIP — warrants articles.csv not found")
        return 0
    df = pd.read_csv(path, dtype=str).fillna("")
    df = _norm_cols(df)
    df["_source_file"] = str(path)
    df["_loaded_at"] = now_utc()
    df.to_sql("warrants", engine, if_exists="replace", index=False)
    return len(df)


def load_gis_top20(engine, path: Path) -> int:
    if not path.exists():
        print("  SKIP — dennis_soil.csv not found")
        return 0
    df = pd.read_csv(path, dtype=str).fillna("")
    df = _norm_cols(df)

    classes = {
        "prime":     "All areas are prime farmland",
        "statewide": "Farmland of statewide importance",
        "unique":    "Farmland of unique importance",
        "not_prime": "Not prime farmland",
    }
    layer = (
        df.groupby("map_par_id")["frmlndcls"]
        .apply(set)
        .reset_index()
        .rename(columns={"map_par_id": "parcel_id"})
    )
    for col, val in classes.items():
        layer[col] = layer["frmlndcls"].apply(lambda s: int(val in s))
    layer = layer.drop(columns=["frmlndcls"])
    layer["_loaded_at"] = now_utc()
    layer.to_sql("layer_soils", engine, if_exists="replace", index=False)
    return len(layer)


_LAYER_SPECS: list[dict] = [
    {
        "name": "zone1",
        "file": "dennis_zone1.csv",
        "keep": {
            "TYPE":      "zone1_type",
            "SITE_NAME": "zone1_site",
            "SUPPLIER":  "zone1_supplier",
            "ZONE1_FT":  "zone1_ft",
            "PWS_ID":    "zone1_pws_id",
        },
    },
    {
        "name": "zone2",
        "file": "dennis_zone2.csv",
        "keep": {
            "ZII_NUM":    "zone2_id",
            "PWS_ID":     "zone2_pws_id",
            "SUPPLIER":   "zone2_supplier",
            "AREA_ACRES": "zone2_acres",
        },
    },
    {
        "name": "prihab",
        "file": "dennis_prihab.csv",
        "keep": {
            "PRIHAB_ID": "prihab_id",
            "VERSION":   "prihab_version",
        },
    },
    {
        "name": "esthab",
        "file": "dennis_esthab.csv",
        "keep": {
            "ESTHAB_ID": "esthab_id",
            "VERSION":   "esthab_version",
        },
    },
    {
        "name": "natcomm",
        "file": "dennis_natcomm.csv",
        "keep": {
            "COMMUN_NAM": "natcomm_name",
            "UNIQUE_ID":  "natcomm_id",
            "COMMUN_RAN": "natcomm_rank",
            "SPECIFIC_D": "natcomm_description",
            "COMMUN_DES": "natcomm_community",
            "VERSION":    "natcomm_version",
        },
    },
    # cvp (dennis_cvp.csv) — not yet generated; add here when available
    {
        "name": "bm3_vern",
        "file": "dennis_bm3_wern.csv",  # typo in QGIS export
        "keep": {
            "LOC_VP_ID": "bm3_vp_id",
            "AC_LOCVP":  "bm3_vp_acres",
        },
    },
    {
        "name": "bm3_wetlands",
        "file": "dennis_bm3_wetlands.csv",
        "keep": {
            "LOC_WC_ID":  "bm3_wc_id",
            "AC_LOCWC":   "bm3_wc_acres",
            "INTEGRITY":  "bm3_wc_integrity",
            "RESILIENCE": "bm3_wc_resilience",
        },
    },
    {
        "name": "bm3_core",
        "file": "dennis_bm3_core.csv",
        "keep": {
            "CH_ID":      "bm3_ch_id",
            "ACRES_CH":   "bm3_ch_acres",
            "AC_TOWN_CH": "bm3_ch_town_acres",
        },
    },
    {
        "name": "bm3_cnl",
        "file": "dennis_bm3_crit.csv",  # abbreviated in QGIS export
        "keep": {
            "CNL_ID":     "bm3_cnl_id",
            "AC_CNL":     "bm3_cnl_acres",
            "AC_TOWN_CN": "bm3_cnl_town_acres",
        },
    },
    {
        "name": "openspace",
        "file": "dennis_openspace.csv",
        "keep": {
            "SITE_NAME": "os_site_name",
            "FEE_OWNER": "os_owner",
            "OWNER_TYPE": "os_owner_type",
            "MANAGER":   "os_manager",
            "PRIM_PURP": "os_purpose",
            "PUB_ACCESS": "os_public_access",
            "LEV_PROT":  "os_protection_level",
            "GIS_ACRES": "os_acres",
            "OS_TYPE":   "os_type",
            "FORMAL_SIT": "os_formal_site",
            "CAL_DATE_R": "os_date_recorded",
            "ASSESS_MAP": "os_assess_map",
            "ASSESS_LOT": "os_assess_lot",
            "ALT_SITE_N": "os_alt_name",
            "COMMENTS":  "os_comments",
        },
    },
    {
        "name": "wetlands",
        "file": "dennis_wetlands.csv",
        "keep": {
            "WETCODE":    "wetlands_code",
            "IT_VALC":    "wetlands_val_code",
            "IT_VALDESC": "wetlands_val_desc",
            "POLY_CODE":  "wetlands_poly_code",
            "AREAACRES":  "wetlands_acres",
        },
    },
]

_SOIL_KEEP = {
    "MUSYM":      "soil_map_unit",
    "MUKEY":      "soil_map_unit_key",
    "MUNAME":     "soil_name",
    "COMPNAME":   "soil_component",
    "MUKIND":     "soil_kind",
    "FRMLNDCLS":  "soil_farmland_class",
    "HYDRCRATNG": "soil_hydric_rating",
    "DRAINCLASS": "soil_drainage_class",
    "HYDROLGRP":  "soil_hydro_group",
    "SLOPE":      "soil_slope",
    "DEP2WATTBL": "soil_depth_to_water_table",
    "FLOODING":   "soil_flooding",
    "PONDING":    "soil_ponding",
    "TAXCLNAME":  "soil_tax_class",
    "AWS100":     "soil_aws100",
    "SEPTANKAF":  "soil_septic",
}


def _read_gis_layer(path: Path, extra_cols: set[str]) -> pd.DataFrame:
    """Read a GIS join CSV keeping MAP_PAR_ID + requested columns, dropping empty parcel IDs."""
    want = {"MAP_PAR_ID"} | extra_cols
    df = pd.read_csv(path, dtype=str, usecols=lambda c: c in want).fillna("")
    return df[df["MAP_PAR_ID"] != ""].rename(columns={"MAP_PAR_ID": "parcel_id"})


def load_gis_layers(engine, gis_dir: Path) -> int:
    """Merge all GIS layer CSVs into parcels_gis — one row per parcel."""
    anchor = gis_dir / "dennis_zone1.csv"
    if not anchor.exists():
        print("  SKIP — GIS layer CSVs not found")
        return 0

    result = (
        _read_gis_layer(anchor, set())
        [["parcel_id"]]
        .drop_duplicates(subset=["parcel_id"])
    )

    for spec in _LAYER_SPECS:
        path = gis_dir / spec["file"]
        if not path.exists():
            print(f"  SKIP layer {spec['name']} — {spec['file']} not found")
            continue
        df = _read_gis_layer(path, set(spec["keep"]))
        df = df.rename(columns=spec["keep"])
        df = df[["parcel_id"] + list(spec["keep"].values())].drop_duplicates(subset=["parcel_id"])
        result = result.merge(df, on="parcel_id", how="left")
        print(f"    {spec['name']}: loaded")

    # structures — one-to-many: aggregate count + footprint + archived flag
    struct_path = gis_dir / "dennis_structures.csv"
    if struct_path.exists():
        s = _read_gis_layer(struct_path, {"STRUCT_ID", "AREA_SQ_FT", "ARCHIVED"})
        s["AREA_SQ_FT"] = pd.to_numeric(s["AREA_SQ_FT"], errors="coerce").fillna(0)
        agg = s.groupby("parcel_id").agg(
            struct_count=("STRUCT_ID", "count"),
            struct_total_sqft=("AREA_SQ_FT", "sum"),
            struct_has_archived=("ARCHIVED", lambda x: int((x == "Y").any())),
        ).reset_index()
        result = result.merge(agg, on="parcel_id", how="left")
        print(f"    structures: aggregated")

    # soil — one-to-many: take dominant map unit (largest SS_AREA) per parcel
    soil_path = gis_dir / "dennis_soil.csv"
    if soil_path.exists():
        s = _read_gis_layer(soil_path, set(_SOIL_KEEP) | {"SS_AREA"})
        s = s.rename(columns=_SOIL_KEEP)
        s["_ss_area"] = pd.to_numeric(s["SS_AREA"], errors="coerce").fillna(0)
        soil_dom = (
            s.sort_values("_ss_area", ascending=False)
            .drop_duplicates(subset=["parcel_id"])
            .drop(columns=["_ss_area", "SS_AREA"], errors="ignore")
        )
        result = result.merge(soil_dom, on="parcel_id", how="left")
        print(f"    soil: dominant unit per parcel")

    result["_loaded_at"] = now_utc()
    result.to_sql("parcels_gis", engine, if_exists="replace", index=False)
    return len(result)


def load_ocr(engine, docs_dir: Path) -> int:
    if not docs_dir.exists():
        print("  SKIP — registry documents directory not found")
        return 0

    records = []
    for p in sorted(docs_dir.rglob("scan.json")):
        # Path is .../documents/{book}/{page}/scan.json
        try:
            page_str = p.parent.name
            book_str = p.parent.parent.name
        except Exception:
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARN {p}: {e}")
            continue
        if "error" in data:
            continue

        pages = data.get("pages", [])
        full_text = "\n\n".join(pg.get("text", "") for pg in pages).strip()

        # Max composite score per keyword across all pages
        kw: dict[str, float] = {}
        for pg in pages:
            for name, score in pg.get("keyword_scores", {}).items():
                kw[name] = max(kw.get(name, 0.0), score.get("composite", 0.0))

        records.append({
            "book":                                    book_str,
            "page":                                    page_str,
            "full_text":                               full_text,
            "page_count":                              data.get("page_count", len(pages)),
            "kw_article_97":                           kw.get("article_97"),
            "kw_ccr":                                  kw.get("ccr"),
            "kw_chapter_61":                           kw.get("chapter_61"),
            "kw_deed_restriction":                     kw.get("deed_restriction"),
            "kw_conservation_restriction":             kw.get("conservation_restriction"),
            "kw_agricultural_preservation_restriction": kw.get("agricultural_preservation_restriction"),
            "kw_perpetual_restriction":                kw.get("perpetual_restriction"),
            "pipeline_version":                        data.get("pipeline_version"),
            "processed_at":                            data.get("processed_at"),
            "source_hash":                             data.get("source_hash"),
        })

    if not records:
        return 0

    df = pd.DataFrame(records)
    df["_loaded_at"] = now_utc()
    df.to_sql("registry_ocr", engine, if_exists="replace", index=False)

    with engine.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS registry_ocr_fts"))
        con.execute(text("""
            CREATE VIRTUAL TABLE registry_ocr_fts
            USING fts5(book, page, full_text, content=registry_ocr, content_rowid=rowid)
        """))
        con.execute(text("INSERT INTO registry_ocr_fts(registry_ocr_fts) VALUES('rebuild')"))

    return len(records)


def load_registry(engine, index_dir: Path) -> int:
    if not index_dir.exists():
        print("  SKIP — registry index not found")
        return 0

    from discovery.registry.cache import scan_path

    records = []
    for p in sorted(index_dir.glob("*/documents.json")):
        parcel_id = p.parent.name.replace("_", "-")
        try:
            docs = json.loads(p.read_text())
        except Exception as e:
            print(f"  WARN {p.parent.name}: {e}")
            continue
        for rank, doc in enumerate(docs, start=1):
            rec = {"parcel_id": parcel_id, "doc_rank": rank, **doc}
            if isinstance(rec.get("cross_refs"), list):
                rec["cross_refs"] = json.dumps(rec["cross_refs"])
            rec["scan_cached"] = int(
                scan_path(rec.get("book", ""), rec.get("page", "")).exists()
            )
            records.append(rec)
    if not records:
        return 0
    df = pd.DataFrame(records)
    df["_loaded_at"] = now_utc()
    df.to_sql("registry_documents", engine, if_exists="replace", index=False)
    return len(df)


# ── Reference tables ──────────────────────────────────────────────────────────

def load_schema_columns(engine) -> int:
    path = Path(__file__).parent / "schema_columns.csv"
    df = pd.read_csv(path, dtype=str).fillna("")
    df.to_sql("schema_columns", engine, if_exists="replace", index=False)
    return len(df)


def load_gis_sources(engine) -> int:
    src_dir = Path(__file__).parent.parent / "data" / "gis_sources"
    records = []
    for p in sorted(src_dir.glob("*.json")):
        rec = json.loads(p.read_text())
        records.append(rec)
    if not records:
        print("  WARN — no JSON files found in data/gis_sources/")
        return 0
    df = pd.DataFrame(records)
    df.to_sql("gis_sources", engine, if_exists="replace", index=False)
    return len(df)


def load_ref_use_codes(engine) -> int:
    df = pd.DataFrame(
        [(code, desc, cls) for code, (desc, cls) in USE_CODES.items()],
        columns=["code", "description", "property_class"],
    )
    df.to_sql("ref_use_codes", engine, if_exists="replace", index=False)
    return len(df)


# ── Build parcels ─────────────────────────────────────────────────────────────

def build_parcels(engine) -> int:
    assessor = pd.read_sql("SELECT * FROM assessor", engine)
    massgis  = pd.read_sql("SELECT * FROM massgis",  engine)

    # ── Prepare assessor ──────────────────────────────────────────────────────
    assessor["parcel_id"] = (
        assessor["map"].str.strip() + "-" + assessor["parcel"].str.strip()
    )
    ext = assessor["extension"].astype(str).str.strip()
    assessor["unit_key"] = assessor["parcel_id"].where(
        ext.isin(["", "0"]),
        assessor["parcel_id"] + "-" + ext,
    )
    assessor.to_sql("layer_assessor", engine, if_exists="replace", index=False)

    # ── Count condo units per parcel ──────────────────────────────────────────
    unit_counts = (
        assessor.loc[~ext.isin(["", "0"]), ["parcel_id"]]
        .groupby("parcel_id").size()
        .rename("condo_units").reset_index()
    )

    # ── Backbone: prefer extension=0 parent; synthesize from lowest extension ─
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

    # ── Deduplicate MassGIS: one row per parcel_id, take largest polygon ──────
    massgis = massgis.rename(columns={"map_par_id": "parcel_id"})
    massgis["_lot_num"] = pd.to_numeric(massgis["lot_size"], errors="coerce").fillna(0)
    layer_massgis = (
        massgis.sort_values("_lot_num", ascending=False)
        .drop_duplicates(subset=["parcel_id"], keep="first")
        .drop(columns=["_lot_num"])
    )
    layer_massgis.to_sql("layer_massgis", engine, if_exists="replace", index=False)

    # ── Outer join ────────────────────────────────────────────────────────────
    parcels = backbone.merge(
        layer_massgis, on="parcel_id", how="outer", suffixes=("", "_gis"), indicator=True,
    )
    parcels["join_status"] = parcels["_merge"].map({
        "both":      "BOTH",
        "left_only": "ASSESSOR_ONLY",
        "right_only": "MASSGIS_ONLY",
    })
    parcels = parcels.drop(columns=["_merge"])

    # ── Derived columns ───────────────────────────────────────────────────────
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
    parcels["owner_category"] = parcels["owner_name"].apply(_owner_category)

    municipal_cats = {"Town of Dennis", "Commonwealth", "Conservation Commission",
                      "Housing Authority", "Water District", "Fire District"}
    parcels.loc[parcels["owner_category"].isin(municipal_cats), "property_class"] = "Municipal"

    parcels["is_public"] = (
        (parcels["property_class"] == "Municipal") &
        (~parcels["use_code_norm"].isin(EXEMPT_USE))
    ).astype(int)

    # ── Lean backbone parcels (display fields only — no joins needed for list) ─
    display_cols = [
        "parcel_id", "join_status", "backbone_source", "condo_units",
        "owner_name", "owner_category",
        "locno", "locst", "village",
        "site_addr",
        "use_code_norm", "use_code_desc", "property_class", "is_public",
        "billingacres", "totalapprvalue", "zonedesc",
    ]
    keep = [c for c in display_cols if c in parcels.columns]
    parcels[keep].to_sql("parcels", engine, if_exists="replace", index=False)

    return len(parcels)


# ── Utility ───────────────────────────────────────────────────────────────────

def _table_exists(engine, name: str) -> bool:
    with engine.connect() as con:
        result = con.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        )
        return result.fetchone() is not None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = get_config()
    root = cfg.root

    assessor_files = cfg.collection_files("assessor")
    if not assessor_files:
        print("No assessor files in sources.yaml — cannot build.")
        sys.exit(1)
    assessor_path = assessor_files[0]["abs_path"]

    gis_files = cfg.collection_files("gis")
    massgis_path = gis_files[0]["abs_path"] if gis_files else root / "gis" / "dennis_parcels.geojson"

    warrants_path       = root / "ma-dennis" / "town_meeting_all_years.csv"
    soil_path           = root / "gis" / "dennis_soil.csv"
    gis_dir             = root / "gis"
    registry_index      = cfg.output_dir("registry") / "index"
    registry_docs   = cfg.output_dir("registry") / "documents"

    db_path = cfg.db_path("raw")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    for label, path in [("Assessor", assessor_path), ("MassGIS", massgis_path)]:
        if not path.exists():
            print(f"MISSING required source: {label} — {path}")
            sys.exit(1)

    for label, path in [
        ("Warrants",       warrants_path),
        ("Soil CSV",       soil_path),
        ("GIS layers dir", gis_dir),
        ("Registry index", registry_index),
        ("Registry OCR",   registry_docs),
    ]:
        print(f"  {label}: {'OK' if path.exists() else 'not found — stage will be skipped'}")

    if db_path.exists():
        db_path.unlink()
    print(f"\nBuilding {db_path}")

    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as con:
        con.execute(text("PRAGMA journal_mode=WAL"))
        con.execute(text("PRAGMA foreign_keys=ON"))
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS _pipeline_runs (
                run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                stage       TEXT,
                source_file TEXT,
                rows_loaded INTEGER,
                run_at      TEXT
            )
        """))

    stages = [
        ("load_assessor",  assessor_path,  lambda e: load_assessor(e, assessor_path)),
        ("load_massgis",   massgis_path,   lambda e: load_massgis(e, massgis_path)),
        ("load_warrants",  warrants_path,  lambda e: load_warrants(e, warrants_path)),
        ("layer_soils",    soil_path,      lambda e: load_gis_top20(e, soil_path)),
        ("parcels_gis",    gis_dir,        lambda e: load_gis_layers(e, gis_dir)),
        ("load_registry",  registry_index, lambda e: load_registry(e, registry_index)),
        ("load_ocr",       registry_docs,  lambda e: load_ocr(e, registry_docs)),
        ("schema_columns",  None,           lambda e: load_schema_columns(e)),
        ("gis_sources",     None,           lambda e: load_gis_sources(e)),
        ("ref_use_codes",   None,           lambda e: load_ref_use_codes(e)),
        ("build_parcels",  None,           lambda e: build_parcels(e)),
    ]

    for name, source, fn in stages:
        try:
            _stage(engine, name, source, fn)
        except Exception as exc:
            print(f"\nERROR in {name}: {exc}")
            raise

    size_mb = db_path.stat().st_size / 1_000_000
    print(f"\nDone. {db_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
