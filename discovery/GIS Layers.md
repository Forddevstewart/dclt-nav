# GIS Field Mapping — DCLT Navigator
## Purpose
This document defines which columns to extract from each MassGIS layer during the QGIS → CSV → SQLite pipeline. Each layer is joined independently to `dennis_parcels_fixed` using **Join Attributes by Location (intersects, one-to-one)** and exported as a separate CSV with `parcel_id` as the join key.

## Pipeline Rules
- All layers clipped to `dennis_parcels_26986.geojson` before joining
- All joins use `dennis_parcels_fixed` as the base layer
- Each layer produces one independent CSV
- Python pipeline merges all CSVs on `parcel_id` using left joins
- Structures uses one-to-many join — aggregate to count + total area before export
- IWPA dropped — no records in Dennis

---
## parcels_base
**Source:** `dennis_parcels_26986.geojson`
**Export:** `parcels_base.csv` — full parcel attributes, one row per parcel
**Join key:** `LOC_ID` → renamed to `parcel_id`
**Spatial join base:** `dennis_parcels_join_base.shp` — LOC_ID + geometry only, used as input for all 13 GIS layer joins

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| LOC_ID | Yes | parcel_id | primary join key across all tables |
| MAP_PAR_ID | Yes | map_par_id | MassGIS parcel ID |
| PROP_ID | Yes | prop_id | assessor property ID |
| POLY_TYPE | Yes | poly_type | parcel polygon type |
| USE_CODE | Yes | use_code | land use code |
| SITE_ADDR | Yes | address | site address |
| ADDR_NUM | Yes | addr_num | street number |
| FULL_STR | Yes | street | full street name |
| LOCATION | Yes | location | location description |
| CITY | Yes | city | |
| ZIP | Yes | zip | |
| OWNER1 | Yes | owner | owner name |
| OWN_ADDR | Yes | owner_address | owner mailing address |
| OWN_CITY | Yes | owner_city | |
| OWN_STATE | Yes | owner_state | |
| OWN_ZIP | Yes | owner_zip | |
| ZONING | Yes | zoning | zoning district |
| YEAR_BUILT | Yes | year_built | |
| BLD_AREA | Yes | bld_area | building area sq ft |
| UNITS | Yes | units | number of units |
| RES_AREA | Yes | res_area | residential area sq ft |
| STYLE | Yes | style | building style |
| NUM_ROOMS | Yes | num_rooms | |
| STORIES | Yes | stories | |
| LOT_UNITS | Yes | lot_units | lot size units |
| LOT_SIZE | Yes | lot_size | lot size |
| BLDG_VAL | Yes | bldg_val | assessed building value |
| LAND_VAL | Yes | land_val | assessed land value |
| OTHER_VAL | No | | other assessed value — rarely populated |
| TOTAL_VAL | Yes | total_val | total assessed value |
| FY | Yes | fiscal_year | assessment fiscal year |
| REG_ID | Yes | reg_id | registry of deeds ID |
| OBJECTID | No | | |
| MAP_NO | No | | |
| SOURCE | No | | |
| PLAN_ID | No | | |
| LAST_EDIT | No | | |
| BND_CHK | No | | |
| NO_MATCH | No | | |
| TOWN_ID | No | | redundant |
| LS_DATE | No | | last sale date — consider adding later |
| LS_PRICE | No | | last sale price — consider adding later |
| OWN_CO | No | | rarely populated |
| LS_BOOK | No | | deed book |
| LS_PAGE | No | | deed page |
| GlobalID | No | | |
| Shape__Area | No | | |
| Shape__Length | No | | |

-------------
## zone1
**Source:** `zone2_zone1_iwpa/ZONE1_POLY.shp`
**Dennis clip:** `dennis_zone1.shp`
**Export CSV:** `dennis_zone1.csv` — canonical; `dennis.zone1.csv` (dot-separated) is a stale duplicate, ignore
**Records:** 22

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| TYPE | Yes | zone1_type | |
| SITE_NAME | Yes | zone1_site | |
| SUPPLIER | Yes | zone1_supplier | water supplier name |
| ZONE1_FT | Yes | zone1_ft | protection radius in feet |
| PWS_ID | Yes | zone1_pws_id | public water supply ID |
| SOURCE_ID | No | | |
| TOWN | No | | redundant |
| SHAPE_AREA | No | | |
| SHAPE_LEN | No | | |

---

## zone2
**Source:** `zone2_zone1_iwpa/ZONE2_POLY.shp`
**Dennis clip:** `dennis_zone2.shp`
**Records:** 13

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| ZII_NUM | Yes | zone2_id | |
| PWS_ID | Yes | zone2_pws_id | public water supply ID |
| SUPPLIER | Yes | zone2_supplier | water supplier name |
| AREA_ACRES | Yes | zone2_acres | |
| TOWN | No | | redundant |
| SHAPE_AREA | No | | |
| SHAPE_LEN | No | | |

---

## prihab
**Source:** `PRIHAB_POLY.shp`
**Dennis clip:** `dennis_prihab.shp`

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| PRIHAB_ID | Yes | prihab_id | |
| VERSION | Yes | prihab_version | NHESP version that designated it |
| SHAPE_AREA | No | | |
| SHAPE_LEN | No | | |

---

## esthab
**Source:** `ESTHAB_POLY.shp`
**Dennis clip:** `dennis_esthab.shp`

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| ESTHAB_ID | Yes | esthab_id | |
| VERSION | Yes | esthab_version | NHESP version |
| SHAPE_AREA | No | | |
| SHAPE_LEN | No | | |

---

## natcomm
**Source:** `NATCOMM_POLY.shp`
**Dennis clip:** `dennis_natcomm.shp`

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| COMMUN_NAM | Yes | natcomm_name | community type name |
| UNIQUE_ID | Yes | natcomm_id | |
| COMMUN_RAN | Yes | natcomm_rank | NatureServe rank S1/S2/S3 — rarity indicator |
| SPECIFIC_D | Yes | natcomm_description | |
| COMMUN_DES | Yes | natcomm_community | |
| VERSION | Yes | natcomm_version | |
| SHAPE_AREA | No | | |
| SHAPE_LEN | No | | |

---

## cvp (Certified Vernal Pools)
**Source:** `GISDATA_CVP_PTPoint.shp`
**Dennis clip:** `dennis_cvp.shp`
**Export CSV:** `dennis_cvp.csv` — **not yet generated**; run QGIS join before pipeline
**Geometry:** Point — join uses Within predicate

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| cvp_num | Yes | cvp_id | certified vernal pool number |
| criteria | Yes | cvp_criteria | qualification criteria |
| certified | Yes | cvp_certified_date | certification date |

---

## bm3_vern (BioMap3 Local Vernal Pools)
**Source:** `BioMap — BM3_LOCAL_VERNAL_POOLS`
**Dennis clip:** `dennis_bm3_vern.shp`
**Export CSV:** `dennis_bm3_wern.csv` — filename has typo (`wern` instead of `vern`); use as-is
**Geometry:** Point — join uses Within predicate

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| LOC_VP_ID | Yes | bm3_vp_id | BioMap3 vernal pool ID |
| AC_LOCVP | Yes | bm3_vp_acres | acreage |
| OBJECTID | No | | |
| Shape_Leng | No | | |
| Shape_Area | No | | |

---

## bm3_wetlands (BioMap3 Local Wetlands)
**Source:** `BioMap — BM3_LOCAL_WETLANDS`
**Dennis clip:** `dennis_bm3_wetlands.shp`

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| LOC_WC_ID | Yes | bm3_wc_id | wetland core ID |
| AC_LOCWC | Yes | bm3_wc_acres | acreage |
| INTEGRITY | Yes | bm3_wc_integrity | ecological integrity rating |
| RESILIENCE | Yes | bm3_wc_resilience | climate resilience rating |
| OBJECTID | No | | |
| Shape_Leng | No | | |
| Shape_Area | No | | |

---

## bm3_core (BioMap3 Core Habitat)
**Source:** `BioMap — BM3_CORE_HABITAT`
**Dennis clip:** `dennis_bm3_core.shp`

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| CH_ID | Yes | bm3_ch_id | core habitat ID |
| ACRES_CH | Yes | bm3_ch_acres | total core habitat acreage |
| AC_TOWN_CH | Yes | bm3_ch_town_acres | acreage within Dennis |
| TOWN | No | | redundant |
| OBJECTID | No | | |
| Shape_Leng | No | | |
| Shape_Area | No | | |

---

## bm3_cnl (BioMap3 Critical Natural Landscape)
**Source:** `BioMap — BM3_CRITICAL_NATURAL_LANDSCAPE`
**Dennis clip:** `dennis_bm3_cnl.shp`
**Export CSV:** `dennis_bm3_crit.csv` — QGIS exported with abbreviated name; use as-is

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| CNL_ID | Yes | bm3_cnl_id | critical natural landscape ID |
| AC_CNL | Yes | bm3_cnl_acres | total CNL acreage |
| AC_TOWN_CN | Yes | bm3_cnl_town_acres | acreage within Dennis |
| TOWN | No | | redundant |
| OBJECTID | No | | |
| Shape_Leng | No | | |
| Shape_Area | No | | |

---

## openspace
**Source:** `OpenSpace.gdb / OPENSPACE_POLY`
**Dennis clip:** `dennis_openspace.shp`

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| SITE_NAME | Yes | os_site_name | |
| FEE_OWNER | Yes | os_owner | fee owner name |
| OWNER_TYPE | Yes | os_owner_type | |
| MANAGER | Yes | os_manager | |
| PRIM_PURP | Yes | os_purpose | primary purpose code |
| PUB_ACCESS | Yes | os_public_access | public access level |
| LEV_PROT | Yes | os_protection_level | level of protection |
| GIS_ACRES | Yes | os_acres | |
| OS_TYPE | Yes | os_type | |
| FORMAL_SIT | Yes | os_formal_site | |
| CAL_DATE_R | Yes | os_date_recorded | |
| ASSESS_MAP | Yes | os_assess_map | useful for parcel matching |
| ASSESS_LOT | Yes | os_assess_lot | useful for parcel matching |
| ALT_SITE_N | Yes | os_alt_name | |
| COMMENTS | Yes | os_comments | |
| OBJECTID | No | | |
| TOWN_ID | No | | redundant |
| POLY_ID | No | | |
| OWNER_ABRV | No | | |
| MANAGR_ABR | No | | |
| MANAGR_TYP | No | | |
| OLI_1_* | No | | interest holder details — skip |
| OLI_2_* | No | | |
| OLI_3_* | No | | |
| GRANTPROG* | No | | |
| GRANTTYPE* | No | | |
| PROJ_ID* | No | | |
| EOEAINVOLV | No | | |
| ARTICLE97 | No | | |
| FY_FUNDING | No | | |
| DEED_ACRES | No | | |
| OS_DEED_* | No | | |
| ASSESS_ACR | No | | |
| ASSESS_BLK | No | | |
| ASSESS_SUB | No | | |
| BASE_MAP | No | | |
| SOURCE_MAP | No | | |
| SOURCE_TYP | No | | |
| LOC_ID | No | | |
| DCAM_ID | No | | |
| FEESYM | No | | |
| INTSYM | No | | |
| OS_ID | No | | |
| CR_REF | No | | |
| EEA_CR_ID | No | | |
| SHAPE_* | No | | |

---

## structures
**Source:** `structures_poly.shp`
**Dennis clip:** `dennis_structures.shp`
**Join type:** ONE-TO-MANY — aggregate before joining to parcels

**Aggregation before join:**
```python
# Group by parcel, compute count and total footprint
structures_agg = structures_df.groupby('parcel_id').agg(
    struct_count=('STRUCT_ID', 'count'),
    struct_total_sqft=('AREA_SQ_FT', 'sum'),
    struct_has_archived=('ARCHIVED', lambda x: (x == 'Y').any())
).reset_index()
```

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| STRUCT_ID | Yes | struct_id | use for groupby key |
| AREA_SQ_FT | Yes | struct_area_sqft | aggregate: sum per parcel |
| LOCAL_ID | Yes | struct_local_id | assessor ID |
| ARCHIVED | Yes | struct_archived | flag demolished buildings |
| COMMENTS | Yes | struct_comments | |
| SOURCE | No | | |
| SOURCETYPE | No | | |
| SOURCEDATE | No | | |
| SOURCEDATA | No | | |
| MOVED | No | | |
| TOWN_ID* | No | | redundant |
| ARCHIVEDAT | No | | |
| EDIT_DATE | No | | |
| EDIT_BY | No | | |
| SHAPE_* | No | | |

---

## wetlands
**Source:** `WETLANDSDEP_POLY.shp`
**Dennis clip:** `dennis_wetlands.shp`
**Export CSV:** `dennis_wetlands.csv` — 20,300 records; geometry fix resolved prior 0-record issue

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| WETCODE | Yes | wetlands_code | MassDEP wetland classification code |
| IT_VALC | Yes | wetlands_val_code | impact tolerance value code |
| IT_VALDESC | Yes | wetlands_val_desc | value description |
| POLY_CODE | Yes | wetlands_poly_code | polygon type code |
| AREAACRES | Yes | wetlands_acres | area in acres |
| SOURCE_2 | No | | renamed from SOURCE during QGIS join |
| SOURCE_SCA | No | | source scale |
| AREASQMI | No | | redundant with AREAACRES |
| SHAPE_AREA | No | | |
| SHAPE_LEN | No | | |

---

## soil (MassGIS SSURGO Soils)
**Source:** MassGIS SSURGO soils — GISTop20 dominant component join
**Dennis clip:** joined to `dennis_parcels_fixed`
**Export CSV:** `dennis_soil.csv` — 31,510 records (originally `GISTop20.csv`)
**Join type:** ONE-TO-MANY — multiple soil map units can intersect a parcel; aggregate to dominant (largest area) before joining to parcels

**Aggregation before join:**
```python
# Take dominant soil map unit per parcel (largest area intersection)
soil_dom = soil_df.sort_values('SS_AREA', ascending=False).groupby('parcel_id').first().reset_index()
```

| Source Column | Keep | Target Column | Notes |
|--------------|------|---------------|-------|
| MUSYM | Yes | soil_map_unit | soil map unit symbol |
| MUKEY | Yes | soil_map_unit_key | unique SSURGO map unit key |
| MUNAME | Yes | soil_name | map unit name |
| COMPNAME | Yes | soil_component | dominant component name |
| MUKIND | Yes | soil_kind | map unit kind (consociation, complex, etc.) |
| FRMLNDCLS | Yes | soil_farmland_class | farmland classification |
| HYDRCRATNG | Yes | soil_hydric_rating | hydric soil rating — wetland indicator |
| DRAINCLASS | Yes | soil_drainage_class | drainage class |
| HYDROLGRP | Yes | soil_hydro_group | hydrologic group A/B/C/D — runoff potential |
| SLOPE | Yes | soil_slope | slope % |
| DEP2WATTBL | Yes | soil_depth_to_water_table | depth to seasonal high water table (cm) |
| FLOODING | Yes | soil_flooding | flooding frequency |
| PONDING | Yes | soil_ponding | ponding frequency |
| TAXCLNAME | Yes | soil_tax_class | taxonomic class name |
| AWS100 | Yes | soil_aws100 | available water storage 0–100cm |
| SEPTANKAF | Yes | soil_septic | septic tank absorption field suitability |
| AREASYMBOL | No | | soil survey area admin code |
| SPATIALVER | No | | spatial version |
| SS_AREA | No | | use for dominant-soil sort only; drop after aggregation |
| MUSYM_AREA | No | | |
| AREANAME | No | | soil survey area name — redundant |
| MINSURFTEX | No | | |
| TFACTOR | No | | |
| AWS25 | No | | redundant with AWS100 |
| DWELLWB | No | | engineering use |
| NIRRLCC | No | | |
| ROADS | No | | engineering use |
| CORCONCRET | No | | engineering use |
| CM2RESLYR | No | | |
| RESKIND | No | | |
| PARMATNM | No | | |
| UNIFSOILCL | No | | |
| AASHTO | No | | |
| KFACTRF | No | | |
| KFACTWS | No | | |
| PHWATER | No | | |
| CLAY | No | | |
| SAND | No | | |
| OM | No | | |
| KSAT | No | | |
| NLEACHING | No | | |
| SLOPE_1 | No | | duplicate slope field |
| SHAPE_Leng | No | | |
| SHAPE_Area | No | | |

---

## Pipeline Instructions for Claude Code

1. Load each `_dennis` CSV independently
2. Each CSV contains `LOC_ID` as the join key — rename to `parcel_id` on load
3. Merge all CSVs to base parcels using left joins on `parcel_id`
4. Structures and soil require aggregation before merge
5. Point layers (cvp, bm3_vern) use Within spatial predicate in QGIS join
6. Rename all columns per Target Column names above
7. Null values are expected — not every parcel intersects every layer
8. Final output is `parcels_gis.csv` → ingested as `parcels_gis` table in `raw.db`
9. Provenance: all columns prefixed by layer name (os_, bm3_, zone1_, etc.)

## Actual CSV Filenames
These differ from the layer names in a few cases:

| Layer | Expected filename | Actual filename | Note |
|-------|------------------|-----------------|------|
| bm3_vern | dennis_bm3_vern.csv | `dennis_bm3_wern.csv` | typo in QGIS export |
| bm3_cnl | dennis_bm3_cnl.csv | `dennis_bm3_crit.csv` | abbreviated in QGIS export |
| soil | dennis_soil.csv | `dennis_soil.csv` | renamed from `GISTop20.csv` |
| cvp | dennis_cvp.csv | *(not yet generated)* | run QGIS join before pipeline |