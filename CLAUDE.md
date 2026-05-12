# DCLT Navigator — Claude instructions

## Architecture

Three-stage design: **Discovery** (local scraping) → **Processing** (pipeline builds `reference.db`) → **Presentation** (Flask app on VPS).

**Two databases — never confuse them:**
- `reference.db` — only flows **up** (local pipeline builds it → `sync.sh` deploys it to VPS). Read-only from the app. Contains parcels, GIS layers, registry documents, OCR scores.
- `transactions.db` — only flows **down** (lives on VPS → `sync.sh` pulls it to local backup). Written by the app, never overwritten by deploy. WORM triggers enforce append-only on taggings and notes.

**Tags vs Layers:**
- Tag = user-authored decision, written to `transactions.db`. Defined in `app/dimensions.py`.
- Layer = external/derived/dynamic attribute, lives in `reference.db`. Never written by the app.

**Write path for tags:** `app/tags.py::apply_tag()` → applicability + transition checks via `dimensions.py` → INSERT into `transactions.db`. Reference DB is read-only throughout.

## Testing

Include tests with every code change. New API endpoints go in `app/test_routes.py`, new UI flows in `app/test_ui.py`, DB/schema logic in `app/test_adjudications.py`. Run the relevant test file locally before committing to confirm it passes.

`processing/test_build.py` requires the full data pipeline output and is intentionally excluded from CI.

## Commits

Follow the existing commit style: lowercase summary line, no period, body explains why not what. Co-author line required.
