# DCLT Portal — Technical Architecture

*Upstream authority: DCLT Portal — Conceptual Architecture (CA). This document implements what the CA defines and must not contradict it. When implementation surfaces a gap or contradiction, the resolution path is to update the CA first, then update this document. Last updated: April 30, 2026.*

---

## Purpose

This is the implementation companion to the Conceptual Architecture. Where the CA defines the model — Tag/Layer dichotomy, composition rules, dimension structure, filtering semantics — the Technical Architecture specifies how that model is built: schemas, module boundaries, pipeline stages, write paths, query patterns, and deployment mechanics.

**This document covers:**
- System topology and the two-database split
- File and module organization
- Database schemas: DDL, indexes, WORM triggers
- Layer catalog: all current External, Derived, and Dynamic Layers with their storage
- Tag dimension catalog: all current dimensions, state spaces, applicability
- Ingest pipeline: stage order, normalization, joining against the parcel spine
- Tag write path: validation, append-only enforcement
- Query patterns: fold query, AND filter logic, confidence threshold, applicability projection
- UI surface and two-axis filter pattern
- API blueprint inventory
- Deployment: VPS, GitHub Actions, rsync
- Migration state: current pre-refactor implementation vs. CA target

**This document does not cover:** the model itself, composition rules, naming canon, or anything else defined in the CA. The CA is the upstream authority on those.

---

## System topology

Three stages. Discovery runs locally only and never deploys. Processing runs locally and produces a deployable artifact. Presentation runs on the VPS.

```
[Local machine — CIVICTWIN_ROOT = /Volumes/DigitalTwin/CivicTwin/]
  Discovery   scrape / collect → source files on volume
  Processing  build pipeline   → raw.db → publish step → reference.db

[VPS — ionos-vps, /var/www/dclt-nav/]
  Presentation  gunicorn + Flask
    civictwin/db/reference.db     (rsynced from local; read-only from app)
    data/transactions.db                  (born on server; read-write; never overwritten by deploy)
    civictwin/registry/documents/ (rsynced PDF store)
```

**`CIVICTWIN_ROOT`** resolves all data paths. Set to `/Volumes/DigitalTwin/CivicTwin/` locally; `/var/www/dclt-nav/civictwin/` on the VPS via the systemd service environment. All resolution goes through `discovery/config.py::get_config()`.

**Two databases at runtime:**

| Store | CA name | App config key | Access function | Authorship |
|---|---|---|---|---|
| `reference.db` | `reference.db` | `REFERENCE_DATABASE` | `get_reference_db()` | Pipeline-written; read-only from app |
| `data/transactions.db` | `transactions.db` | `DATABASE` | `get_db()` | App-written; append-only taggings; never overwritten by deploy |

---

## Module organization

```
dclt-nav/
  discovery/
    config.py                  get_config() → SourceConfig; resolves all paths from CIVICTWIN_ROOT
    document_center.py         CivicPlus DocumentCenter scraper (8 categories)
    agenda_center/
      scrape.py / pull.py / ingest.py / download.py / db.py / models.py
                               civic-scraper AgendaCenter module
    registry/
      pipeline.py              Orchestrator: enumerate → build → download (--override-robots required)
      enumerate.py             Tier 1 book/page lookups + Tier 2 name searches
      sweep.py                 Cross-reference expansion + Town of Dennis date-windowed sweep
      download.py              PDF download for approved instrument types
      queue.py                 Target queue CSV generated from raw.db priority scores
      cache.py                 Per-parcel JSON index cache; staleness windows; spread_expiry()
      ratelimit.py             2s minimum delay; 15s/100-request pause; 429/503 → RegistryThrottleError

  processing/
    build.py                   Full 15-stage pipeline → raw.db
    publish.py                 raw.db → reference.db; optionally applies parcel_corrections
    ocr/                       OCR processing for registry and town doc PDFs
    score.py                   OCR keyword scoring (composite score per keyword per document)
    migrate_keywords_to_tags.py  One-off: OCR composite scores → taggings rows (safe to re-run)
    migrate_gis_tags.py          One-off: parcels_gis presence → taggings rows (safe to re-run)
    migrate_for_sale_tags.py     One-off: layer_for_sale presence → taggings rows
    town_doc_candidates.py     Town doc → parcel link candidate generation
    schema_columns.csv         Data dictionary loaded into reference.db at build time

  app/
    __init__.py                create_app(); blueprint registration; session middleware; usage logging
    models.py                  get_db(), get_reference_db(), run_migrations()
    migrations.py              Forward-only numbered migrations for transactions.db (current: version 12)
    auth.py                    Flask-Login; login/logout; ensure_ford() bootstrap
    routes.py                  HTML page routes (three-pane parcel view, document view, etc.)
    api.py                     Parcel and document query API
    tags.py                    Tag and tagging read/write API (/api/tags, /api/tagging, /api/tagged)
    adjudications.py           Legacy adjudication API (pre-Tags system; being superseded)
    admin.py                   Admin routes (usage log, user management)
    exports.py                 Data export routes
    usage.py                   Usage event logging to usage_log table

  data/
    seed.sql                   Schema bootstrap for transactions.db (schema_version, users tables)
    transactions.db                    Local dev copy of the read-write store
    gis_sources/               JSON provenance metadata for each GIS layer

  wsgi.py                      WSGI entry point (gunicorn)
  RUNBOOK.md                   Operational runbook: triggers, commands, data locations
  COMMANDS.md                  Quick command reference
```

---

## reference.db schema

Built by `processing/build.py`. **Every table is dropped and replaced on each full build** — reference.db is never partially updated. The `_pipeline_runs` audit table records each stage execution.

### External Layers

These map directly to upstream source files. All columns are loaded as-is; no cherry-picking at load time.

| Table | Source | Notes |
|---|---|---|
| `assessor` | ADB Excel, `BT_Extract` sheet | Key fields (map, block, parcel, extension, book/page) normalized to strings; page columns converted from float |
| `massgis` | MassGIS GeoJSON | Feature properties; `centroid_lat`/`centroid_lon` computed from polygon ring |
| `layer_soils` | `gis/dennis_soil.csv` (QGIS export) | Farmland class flags per parcel: `prime`, `statewide`, `unique`, `not_prime` (boolean) |
| `warrants` | `ma-dennis/town_meeting_all_years.csv` | Town meeting warrant articles; optional — stage skips if absent |
| `registry_documents` | Per-parcel `registry/index/*/documents.json` | Deed index with book/page, instrument type, cross-refs; `scan_cached` flag |
| `registry_ocr` | Per-book `registry/documents/*/*/scan.json` | Full text; max composite OCR keyword scores per document |
| `town_docs` | `ma-dennis/agendacenter/**/*.json` and `ma-dennis/documentcenter/**/*.json` | Full text; committee; meeting date parsed from filename |
| `layer_for_sale` | `HomeForSale.txt` (manually pasted Zillow export) | Normalized address; price; beds/baths/sqft/acres |

**FTS virtual tables** (rebuilt on each load using `fts5`):
- `registry_ocr_fts` — full-text over `registry_ocr.full_text`, content-linked
- `town_docs_fts` — full-text over `town_docs.full_text`, content-linked

### Derived Layers

Computed from External Layers and materialized in reference.db at ingest time.

| Table | Primary inputs | Description |
|---|---|---|
| `layer_assessor` | `assessor` | All assessor rows; adds `parcel_id` (map-parcel, 2-part) and `unit_key` (map-parcel-extension, 3-part for condo units) |
| `layer_massgis` | `massgis` | Deduplicated to one row per `parcel_id`; keeps largest polygon by `lot_size` |
| `parcels_gis` | `dennis_*.csv` GIS layer exports | One row per `parcel_id`; all layer columns merged via left join on `MAP_PAR_ID`; structures aggregated (count, total sqft, archived flag); soil dominant map unit per parcel |
| `parcels` | `layer_assessor` + `layer_massgis` | Backbone: one row per land parcel; outer join with indicator; derived columns added (see below) |

**Parcel backbone key derived columns:**

| Column | Derivation |
|---|---|
| `parcel_id` | `map` + `-` + `parcel` |
| `unit_key` | `parcel_id` + `-` + `extension` when extension is not 0 or blank |
| `backbone_source` | `'parent'` when an extension=0 row exists; `'synthesized'` when built from the lowest-numbered extension |
| `condo_units` | Count of non-zero extension rows for this parcel_id |
| `join_status` | `BOTH` / `ASSESSOR_ONLY` / `MASSGIS_ONLY` from outer join indicator |
| `use_code_norm` | `stateclass` or `use`, zero-padded to 4 chars |
| `use_code_desc` | Mapped from `ref_use_codes` |
| `property_class` | Mapped from `ref_use_codes`; overridden to `Municipal` when `owner_category` is a municipal category |
| `owner_category` | Regex-matched from `owner_name`: Town of Dennis, Commonwealth, Conservation Commission, Housing Authority, Water District, Fire District, Conservation Land Trust, Conservation Trust |
| `is_public` | 1 when `property_class = Municipal` and `use_code_norm` not in exempt set {9460, 9820} |
| `coverage_ratio` | `struct_total_sqft` ÷ (`billingacres` × 43,560); capped at 1.0 |
| `coverage_status` | `ok` / `data_issue` (ratio > 1.0) / `no_structure` / `no_acreage` |

### Reference tables

| Table | Source |
|---|---|
| `ref_use_codes` | Hardcoded in `build.py` |
| `schema_columns` | `processing/schema_columns.csv` |
| `gis_sources` | `data/gis_sources/*.json` |
| `_pipeline_runs` | Audit log; one row per stage per run (stage, source_file, rows_loaded, run_at) |

---

## transactions.db schema

Initialized by `data/seed.sql` (creates `schema_version` and `users` tables). Advances through forward-only numbered migrations in `app/migrations.py`. Current migration version: **12**.

### Tags (migration 4, extended in 5 and 6)

```sql
tags (
    tag_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE,
    states_csv    TEXT    NOT NULL,      -- 'system' sentinel for system tags; comma-separated states for user tags
    display_order INTEGER NOT NULL DEFAULT 0,
    deprecated_at TEXT,
    tag_type      TEXT    NOT NULL DEFAULT 'user',   -- 'system' | 'user'
    target_entity TEXT    NOT NULL DEFAULT 'any'     -- 'parcel' | 'document' | 'any'
)
```

```sql
taggings (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id      INTEGER NOT NULL,
    state       TEXT,                 -- NULL = untag event; non-null = the asserted state
    target_type TEXT    NOT NULL,     -- 'parcel' | 'document'
    target_id   TEXT    NOT NULL,
    user_id     INTEGER NOT NULL,
    timestamp   TEXT    NOT NULL DEFAULT (datetime('now')),
    system      INTEGER NOT NULL DEFAULT 0,  -- 1 for system-written rows (pre-refactor)
    confidence  REAL,                        -- set for system tags; null for user tags
    source      TEXT                         -- 'ocr_keyword' | 'gis_layer' | 'doc_sync'
)
```

**WORM enforcement:** Database-level triggers (`no_upd_taggings`, `no_del_taggings`) reject any UPDATE or DELETE on `taggings`. Application code must never construct such statements. Corrections append a new event; latest row wins.

**Indexes:**
- `idx_taggings_target` on `(target_type, target_id, tag_id, event_id DESC)` — supports the fold query
- `idx_taggings_tag` on `(tag_id)` — supports tag-level queries

**Fold query pattern** — latest-wins for a `(target_type, target_id, tag_id)` triple, as used in `app/tags.py`:

```sql
SELECT t1.state, t1.confidence
FROM taggings t1
WHERE t1.target_type = :type
  AND t1.target_id   = :id
  AND t1.tag_id      = :tid
  AND t1.event_id = (
      SELECT MAX(t2.event_id) FROM taggings t2
      WHERE t2.tag_id      = t1.tag_id
        AND t2.target_type = t1.target_type
        AND t2.target_id   = t1.target_id
  )
```

### Legacy tables (pre-Tags system)

These predate the unified Tag model and are being superseded for new work.

| Table | Purpose | Status |
|---|---|---|
| `adjudications` | Keyword verdict log (yes/no/unclear per keyword per target) | Append-only; superseded by `taggings` |
| `user_tags` | Free-form presence/absence tag log | Append-only; superseded by `taggings` |
| `notes` | Per-target note log | Active |
| `parcel_link_adjudications` | Town doc → parcel link outcomes (confirmed/rejected/user_manual) | Active; migrated from `parcel_links` in migration 11 |

### Users, sessions, audit

```sql
users           (id, username, password_hash, created_at, last_login, role, full_name)
schema_version  (version INTEGER PRIMARY KEY)
_env_sentinel   (seq, env, detail, set_at)
usage_log       (seq, ts, user_id, username, session_id, event_type, api_call, details, ip, user_agent)
```

**Environment guard:** `_env_sentinel` carries a `'dev'` row when first used in a non-production environment. On startup, `run_migrations()` rejects a production start if a dev sentinel is present, preventing a dev database from being served in production.

---

## Current Layer and Tag dimension catalog

### Pre-refactor system — document-scoped OCR keyword tags

Seeded by `processing/migrate_keywords_to_tags.py` (reads `registry_ocr` from reference.db; safe to re-run). Stored as `tag_type='system'`, `target_entity='document'`, `states_csv='system'`. Confidence = raw OCR composite score; source = `'ocr_keyword'`.

In CA terms these should be an External Layer (`DocumentKeywordScores`) with values already present in `registry_ocr`. The taggings rows are a pre-refactor materialization.

| Tag name | display_order |
|---|---|
| Conservation Restriction | 100 |
| Article 97 | 101 |
| Deed Restriction | 102 |
| Chapter 61 | 103 |
| Ag. Preservation Restriction | 104 |
| Perpetual Restriction | 105 |
| CC&R | 106 |

### Pre-refactor system — parcel-scoped GIS layer presence tags

Seeded by `processing/migrate_gis_tags.py` (reads `parcels_gis`; safe to re-run). Stored as `tag_type='system'`, `target_entity='parcel'`, `states_csv='system'`. Confidence = 1.0; source = `'gis_layer'`.

In CA terms these should be a Derived Layer (`ParcelGISPresence` or per-layer named attributes) materialized in reference.db. The taggings rows are a pre-refactor materialization.

| Tag name | display_order |
|---|---|
| Zone 1 WHP | 200 |
| Zone 2 WHP | 201 |
| Priority Habitat | 202 |
| Est. Habitat | 203 |
| Nat. Community | 204 |
| BioMap3 VP | 205 |
| BioMap3 Wetland | 206 |
| BioMap3 Core | 207 |
| BioMap3 CNL | 208 |
| Open Space | 209 |
| Wetlands | 210 |
| Structures | 211 |
| Soil | 212 |
| For Sale | 213 |

### User Tag dimensions (CA-model Tags)

| Name | States | Node type | display_order | Notes |
|---|---|---|---|---|
| Development Status | `undeveloped`, `underdeveloped` | parcel | 300 | Partial state space; will expand to `{Unconfirmed, Undeveloped, Underdeveloped, Developed}` when `CoverageDetermination` is introduced |

---

## Ingest pipeline

`python3 -m processing.build` executes 15 stages in order. Each stage is wrapped by `_stage()` which logs to `_pipeline_runs`. The entire database is rebuilt from scratch on each run (no incremental update). On error, the pipeline raises immediately.

| # | Stage name | Output | Optional |
|---|---|---|---|
| 1 | `load_assessor` | `assessor` | No |
| 2 | `load_massgis` | `massgis` | No |
| 3 | `load_warrants` | `warrants` | Yes — skips if file absent |
| 4 | `layer_soils` | `layer_soils` | Yes |
| 5 | `parcels_gis` | `parcels_gis` | Yes — skips if anchor CSV absent |
| 6 | `load_registry` | `registry_documents` | Yes |
| 7 | `load_ocr` | `registry_ocr`, `registry_ocr_fts` | Yes |
| 8 | `load_for_sale` | `layer_for_sale` | Yes |
| 9 | `load_town_docs` | `town_docs`, `town_docs_fts` | Yes |
| 10 | `build_parcels` | `layer_assessor`, `layer_massgis`, `parcels` | No |
| 11 | `coverage` | adds `coverage_ratio`, `coverage_status` to `parcels` | Yes — skips if parcels_gis absent |
| 12 | `link_candidates` | `town_doc_link_candidates` | — |
| 13 | `schema_columns` | `schema_columns` | No |
| 14 | `gis_sources` | `gis_sources` | No |
| 15 | `ref_use_codes` | `ref_use_codes` | No |

**Publish step** (`python3 -m processing.publish`): copies `raw.db` → `reference.db`. If a locally-synced `transactions.db` (i.e., a copy of the server's `transactions.db`) is present, applies `parcel_corrections` rows before the copy completes.

---

## Tag write path

**Read endpoints** (no auth required):
- `GET /api/tags` — all non-deprecated tags, optionally filtered by `?entity=parcel|document`
- `GET /api/tagging/<target_type>/<target_id>` — all tags plus current fold state for one node
- `GET /api/tagged/<entity_type>?tag_ids=1,2&threshold=0.4` — AND intersection of tag membership sets

**Write endpoint** (requires `@login_required`):
- `POST /api/tagging` — body: `{tag_id, state, target_type, target_id}`

Validation sequence in `app/tags.py::apply_tag()`:
1. `tag_id`, `target_type`, and `target_id` must be present.
2. Tag must exist and `deprecated_at` must be null.
3. `tag_type` must not be `'system'` — system tags cannot be manually applied.
4. If `state` is not None, it must appear in the tag's `states_csv` list.

On success: inserts into `taggings` with `user_id=current_user.id`, `system=0`. The WORM trigger at the database level makes UPDATE and DELETE impossible regardless of application code.

**AND filter implementation:** For each requested `tag_id`, the fold query (latest `event_id` wins per triple) produces a set of `target_id` values. For `tag_type='system'` tags, a `confidence >= threshold` clause is applied (default threshold: 0.4). The sets are intersected; all tags must be present on the entity.

---

## API blueprint inventory

| Blueprint file | URL prefix | Key responsibilities |
|---|---|---|
| `app/auth.py` | `/` | Login, logout, Flask-Login integration |
| `app/routes.py` | `/` | HTML page routes: parcel list, parcel detail, document view |
| `app/api.py` | `/api` | Parcel and document search and query |
| `app/tags.py` | `/api` | Tag catalog, per-node tag state, tag write |
| `app/adjudications.py` | `/api` | Legacy adjudication CRUD |
| `app/admin.py` | `/api/admin` | Usage log, user management |
| `app/exports.py` | `/export` | Data exports |

Usage logging: `app/__init__.py` hooks `after_request` to log all `/api/` calls (except a skip list) to `usage_log` via `app/usage.py`.

---

## Deployment

### Code

```bash
git push origin main
```

GitHub Actions SSHs to ionos-vps, runs `git reset --hard origin/main`, `pip install -r requirements.txt`, and `systemctl restart dclt-nav`. No data files are touched.

### Data (separate from code deploy)

```bash
# Sync database (run after every processing.publish)
rsync -avz /Volumes/DigitalTwin/CivicTwin/db/reference.db \
  ionos-vps:/var/www/dclt-nav/civictwin/db/reference.db

# Sync registry PDFs (incremental)
rsync -avz /Volumes/DigitalTwin/CivicTwin/registry/documents/ \
  ionos-vps:/var/www/dclt-nav/civictwin/registry/documents/
```

### Process and logs

gunicorn managed by systemd service `dclt-nav`. To edit the service file, SSH as root (the `deployer` user can only restart, not edit). Logs:

```bash
tail -30 /var/log/dclt-nav-error.log
tail -10 /var/log/dclt-nav-access.log
systemctl status dclt-nav
```

---

## Migration state: current implementation → CA target

The CA's "Migration notes" section defines six steps to move from the current mixed-authorship Tag table to a clean Layer/Tag split. Status as of April 30, 2026:

| Step | Status | What's needed |
|---|---|---|
| 1. Inventory existing tag dimensions; classify each as Tag or Attribute | Partial | `tag_type` split exists; CA naming convention not yet applied; `adjudications` and `user_tags` tables not yet inventoried |
| 2. Move attribute dimensions into Layers (External / Derived / Dynamic) | Not started | System tags still stored in `taggings`; OCR scores exist in `registry_ocr` but are duplicated into `taggings`; GIS presence is duplicated similarly |
| 3. Restrict `taggings` to portal-authored events only; add WORM trigger if absent | Partial | WORM trigger exists; `system=1` rows and `tag_type='system'` tags still present |
| 4. Rebuild system seeds as default rules on Tag dimensions | Not started | Seeds are written at ingest time into `taggings`; should become default-rule reads at fold time |
| 5. Audit all read paths | Not started | `tags.py` treats system tags as confidence-filtered tagging rows rather than Layer reads |
| 6. Audit filtering UIs; replace applicability-as-state with two-axis pattern | Not started | Confidence slider exists; `Not Applicable` affordance not yet implemented as a separate axis |

**Concrete changes the refactor requires:**

- `DocumentKeywordScores` → External Layer (data already in `registry_ocr.kw_*` columns); remove OCR keyword rows from `taggings`
- GIS layer presence → Derived Layer per attribute or as flags in `parcels_gis` (data already present); remove GIS system tag rows from `taggings`
- ~~Rename `data/transactions.db`~~ Done.
- Introduce `CoverageDetermination` Tag dimension: states `{Unconfirmed, Undeveloped, Underdeveloped, Developed}`; node type parcel; applicability all parcels; replaces current `Development Status` tag
- Introduce `IdentityState` Derived Layer (already computable from `join_status` in `parcels`) and `IdentityResolution` Tag dimension: states `{Unconfirmed, ADB Add, ADB Remove, GIS Add, GIS Remove}`; applicability when `IdentityState ≠ OK`; constrained transitions per CA example
- Introduce `Article97Determination` Tag dimension: states `{Unconfirmed, Confirmed, Denied}`; node type document; applicability when OCR keyword score above threshold; replaces current binary system tag presence
- `ParcelCoverageRollup` — Dynamic Layer: reads `CoverageDetermination` folds; produces inventory-level percentages by state; drives the cover-page hygiene view
- `ParcelArticle97Rollup` — Dynamic Layer: reads `Article97Determination` folds; aggregates to parcel level; denominator is applicable documents, not all documents

The `reference.db` / `transactions.db` storage split survives the refactor unchanged. What changes is what rows live on each side of the line, and what the application code reads to get current state.

