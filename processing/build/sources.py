"""Source loaders — one function per raw input, writes to reference.db."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from discovery.keywords import KW_KEYS
from .normalize import norm_cols

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

_ADDR_RE = re.compile(r"^\d[\d\-A-Za-z]*\s+.+,\s+.+,\s+MA\s+\d{5}$")
_PRICE_RE = re.compile(r"^\$[\d,]+$|^\$--$")
_DETAIL_RE = re.compile(
    r"(for sale|new construction|foreclosure|auction|for sale by owner)", re.IGNORECASE
)
_BEDS_RE   = re.compile(r"(\d+)\s*bds?")
_BATHS_RE  = re.compile(r"(\d+)\s*ba")
_SQFT_RE   = re.compile(r"([\d,]+)\s*sqft")
_ACRES_RE  = re.compile(r"([\d.]+)\s*acres?\s+lot")
_TYPE_RE   = re.compile(
    r"(House|Condo|Townhouse|Home|New construction|Lot / Land|Foreclosure|Auction|For sale by owner)",
    re.IGNORECASE,
)
_ADDR_NORM = {
    r"\bRd\b":  "Road",   r"\bSt\b":  "Street",
    r"\bAve\b": "Avenue", r"\bDr\b":  "Drive",
    r"\bLn\b":  "Lane",   r"\bCir\b": "Circle",
    r"\bCt\b":  "Court",  r"\bPl\b":  "Place",
    r"\bHwy\b": "Highway",r"\bRte\b": "Route",
    r"\bExt\b": "Extension",
}

_TD_DATE_RE = re.compile(r"_(\d{2})(\d{2})(\d{4})-")
_TD_SOURCES = ("agendacenter", "documentcenter")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _polygon_centroid(geometry: dict) -> tuple[float, float] | tuple[None, None]:
    try:
        gtype = geometry.get("type")
        if gtype == "Polygon":
            ring = geometry["coordinates"][0]
        elif gtype == "MultiPolygon":
            ring = max((poly[0] for poly in geometry["coordinates"]), key=len)
        else:
            return None, None
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        return sum(lats) / len(lats), sum(lons) / len(lons)
    except Exception:
        return None, None


def _read_gis_layer(path: Path, extra_cols: set[str]) -> pd.DataFrame:
    want = {"MAP_PAR_ID"} | extra_cols
    df = pd.read_csv(path, dtype=str, usecols=lambda c: c in want).fillna("")
    return df[df["MAP_PAR_ID"] != ""].rename(columns={"MAP_PAR_ID": "parcel_id"})


def _norm_addr(addr: str) -> str:
    a = addr.upper().strip()
    for pat, repl in _ADDR_NORM.items():
        a = re.sub(pat, repl.upper(), a, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", a)


def _parse_td_date(stem: str) -> str | None:
    m = _TD_DATE_RE.search(stem)
    if not m:
        return None
    month, day, year = m.group(1), m.group(2), m.group(3)
    try:
        from datetime import date
        return date(int(year), int(month), int(day)).isoformat()
    except ValueError:
        return None


def _build_ac_url_index(ma_dennis_dir: Path) -> dict[str, str]:
    from discovery.config import get_config
    civic_db = get_config().db_path("dennis_civic")
    if not civic_db.exists():
        return {}
    try:
        import sqlite3 as _sq
        conn = _sq.connect(str(civic_db))
        rows = conn.execute(
            "SELECT local_path, url FROM documents WHERE local_path IS NOT NULL AND url IS NOT NULL"
        ).fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows if r[0] and r[1]}
    except Exception:
        return {}


def _dc_source_url(stem: str, base_url: str) -> str:
    parts = stem.split("_", 1)
    if len(parts) == 2 and parts[0].isdigit():
        return f"{base_url}/DocumentCenter/View/{parts[0]}"
    return ""


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_assessor(engine, path: Path) -> int:
    df = pd.read_excel(path, sheet_name="BT_Extract")
    df = norm_cols(df)
    df["_source_file"] = str(path)
    df["_loaded_at"] = _now_utc()
    for col in ("map", "block", "parcel", "extension",
                "booklast", "bookprev", "stateclass", "use", "gisid"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", "")
    for col in ("pagelast", "pageprev"):
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .apply(lambda v: str(int(v)) if pd.notna(v) else "")
            )
    df.to_sql("assessor", engine, if_exists="replace", index=False)
    return len(df)


def load_massgis(engine, path: Path) -> int:
    data = json.loads(path.read_text())
    rows = []
    for f in data.get("features", []):
        props = dict(f["properties"])
        lat, lon = _polygon_centroid(f.get("geometry") or {})
        props["centroid_lat"] = lat
        props["centroid_lon"] = lon
        rows.append(props)
    df = pd.DataFrame(rows)
    df = norm_cols(df)
    df["_source_file"] = str(path)
    df["_loaded_at"] = _now_utc()
    df.to_sql("massgis", engine, if_exists="replace", index=False)
    return len(df)


def load_warrants(engine, path: Path) -> int:
    if not path.exists():
        print("  SKIP — warrants articles.csv not found")
        return 0
    df = pd.read_csv(path, dtype=str).fillna("")
    df = norm_cols(df)
    df["_source_file"] = str(path)
    df["_loaded_at"] = _now_utc()
    df.to_sql("warrants", engine, if_exists="replace", index=False)
    return len(df)


def load_gis_top20(engine, path: Path) -> int:
    """Load soil farmland classification flags into layer_soils."""
    if not path.exists():
        print("  SKIP — dennis_soil.csv not found")
        return 0
    df = pd.read_csv(path, dtype=str).fillna("")
    df = norm_cols(df)

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
    layer["_loaded_at"] = _now_utc()
    layer.to_sql("layer_soils", engine, if_exists="replace", index=False)
    return len(layer)


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

    result["_loaded_at"] = _now_utc()
    result.to_sql("parcels_gis", engine, if_exists="replace", index=False)
    return len(result)


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
    df["_loaded_at"] = _now_utc()
    df.to_sql("registry_documents", engine, if_exists="replace", index=False)
    return len(df)


def load_ocr(engine, docs_dir: Path) -> int:
    if not docs_dir.exists():
        print("  SKIP — registry documents directory not found")
        return 0

    records = []
    for p in sorted(docs_dir.rglob("scan.json")):
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

        kw: dict[str, float] = {}
        for pg in pages:
            for name, score in pg.get("keyword_scores", {}).items():
                kw[name] = max(kw.get(name, 0.0), score.get("composite", 0.0))

        records.append({
            "book":       book_str,
            "page":       page_str,
            "full_text":  full_text,
            "page_count": data.get("page_count", len(pages)),
            **{f"kw_{k}": kw.get(k) for k in KW_KEYS},
            "pipeline_version": data.get("pipeline_version"),
            "processed_at":     data.get("processed_at"),
            "source_hash":      data.get("source_hash"),
        })

    if not records:
        return 0

    df = pd.DataFrame(records)
    df["_loaded_at"] = _now_utc()
    df.to_sql("registry_ocr", engine, if_exists="replace", index=False)

    with engine.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS registry_ocr_fts"))
        con.execute(text("""
            CREATE VIRTUAL TABLE registry_ocr_fts
            USING fts5(book, page, full_text, content=registry_ocr, content_rowid=rowid)
        """))
        con.execute(text("INSERT INTO registry_ocr_fts(registry_ocr_fts) VALUES('rebuild')"))

    return len(records)


def load_for_sale(engine, path: Path) -> int:
    if not path.exists():
        print("  SKIP — HomeForSale.txt not found")
        return 0

    lines = [l.rstrip() for l in path.read_text(encoding="utf-8").splitlines()]
    listings: dict[str, dict] = {}

    i = 0
    while i < len(lines):
        line = lines[i]
        if _PRICE_RE.match(line.strip()):
            price_str = line.strip().lstrip("$").replace(",", "")
            price = None if price_str == "--" else (int(price_str) if price_str.isdigit() else None)

            detail_line = ""
            addr_line = ""
            for j in range(i + 1, min(i + 4, len(lines))):
                l = lines[j].strip()
                if not detail_line and _DETAIL_RE.search(l):
                    detail_line = l
                elif not addr_line and _ADDR_RE.match(l):
                    addr_line = l
                if detail_line and addr_line:
                    break

            if addr_line:
                beds  = int(m.group(1)) if (m := _BEDS_RE.search(detail_line))  else None
                baths = int(m.group(1)) if (m := _BATHS_RE.search(detail_line)) else None
                sqft_m  = _SQFT_RE.search(detail_line)
                sqft    = int(sqft_m.group(1).replace(",", "")) if sqft_m else None
                acres_m = _ACRES_RE.search(detail_line)
                acres   = float(acres_m.group(1)) if acres_m else None
                ptype   = m.group(1).title() if (m := _TYPE_RE.search(detail_line)) else ""

                norm = _norm_addr(addr_line)
                if norm not in listings:
                    listings[norm] = {
                        "raw_address":   addr_line,
                        "norm_address":  norm,
                        "price":         price,
                        "property_type": ptype,
                        "beds":          beds,
                        "baths":         baths,
                        "sqft":          sqft,
                        "acres":         acres,
                    }
        i += 1

    if not listings:
        return 0

    df = pd.DataFrame(listings.values())
    df["_loaded_at"] = _now_utc()
    df.to_sql("layer_for_sale", engine, if_exists="replace", index=False)
    return len(df)


def load_town_docs(engine, ma_dennis_dir: Path) -> int:
    if not ma_dennis_dir.exists():
        print("  SKIP — ma-dennis directory not found")
        return 0

    from discovery.config import get_config
    cfg = get_config()
    ac_url_index = _build_ac_url_index(ma_dennis_dir)
    dc_base = cfg.source("document_center").get("base_url", "") if cfg.enabled("document_center") else ""

    records = []
    for source_type in _TD_SOURCES:
        subdir = ma_dennis_dir / source_type
        if not subdir.exists():
            continue
        for json_path in sorted(subdir.rglob("*.json")):
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"  WARN {json_path}: {e}")
                continue
            if "error" in data:
                continue

            pages     = data.get("pages", [])
            full_text = "\n\n".join(pg.get("text", "") for pg in pages).strip()
            stem      = json_path.stem
            committee = (
                json_path.parent.name
                if source_type == "agendacenter" and json_path.parent != subdir
                else source_type
            )
            doc_type_raw = stem.split("_")[0] if "_" in stem else "unknown"

            pdf_path = str(json_path.with_suffix(".pdf"))
            if source_type == "agendacenter":
                source_url = ac_url_index.get(pdf_path, "")
            else:
                source_url = _dc_source_url(stem, dc_base)

            records.append({
                "doc_id":       f"{source_type}/{json_path.parent.name}/{stem}",
                "source_type":  source_type,
                "committee":    committee,
                "doc_type":     doc_type_raw,
                "meeting_date": _parse_td_date(stem),
                "source_path":  data.get("source_path", ""),
                "source_url":   source_url,
                "page_count":   data.get("page_count", len(pages)),
                "full_text":    full_text,
                "processed_at": data.get("processed_at"),
                "source_hash":  data.get("source_hash"),
            })

    if not records:
        return 0

    df = pd.DataFrame(records)
    df["_loaded_at"] = _now_utc()
    df.to_sql("town_docs", engine, if_exists="replace", index=False)

    with engine.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS town_docs_fts"))
        con.execute(text("""
            CREATE VIRTUAL TABLE town_docs_fts
            USING fts5(doc_id, full_text, content=town_docs, content_rowid=rowid)
        """))
        con.execute(text("INSERT INTO town_docs_fts(town_docs_fts) VALUES('rebuild')"))

    return len(records)
