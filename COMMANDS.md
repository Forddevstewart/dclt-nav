# CivicTwin Common Commands

Working directory for all commands: `/Users/fordstewart/Projects/dclt-nav`

---

## Standard runs (Quick Reference)

### Daily incremental

```bash
# 1. Pull new agendas/minutes + PDFs
python3 -m discovery.agenda_center.pull

# 2. OCR new PDFs — Tesseract pass
python3 -m processing.ocr.ocr_pipeline \
  --input-root /Volumes/DigitalTwin/CivicTwin/ma-dennis/agendacenter

# 3. VLM enrichment pass (requires Ollama running with qwen2.5vl:7b)
python3 -m processing.ocr.vlm_repass \
  --input-roots /Volumes/DigitalTwin/CivicTwin/ma-dennis/agendacenter

# 4. Rebuild raw.db + candidate parcel links
python3 -m processing.build

# 5. Publish to reference.db
python3 -m processing.publish

# 6. Deploy
./deploy_reference.sh
```

### Monthly registry refresh

```bash
python3 -m discovery.registry.pipeline --override-robots
python3 -m discovery.registry.pipeline --override-robots --confirm

# 2. OCR new registry PDFs — Tesseract pass
python3 -m processing.ocr.ocr_pipeline \
  --input-root /Volumes/DigitalTwin/CivicTwin/registry/documents

# 3. VLM enrichment pass on registry + agendacenter
python3 -m processing.ocr.vlm_repass \
  --input-roots \
    /Volumes/DigitalTwin/CivicTwin/registry/documents \
    /Volumes/DigitalTwin/CivicTwin/ma-dennis/agendacenter \
    /Volumes/DigitalTwin/CivicTwin/ma-dennis/warrants/pdfs

# 4–6. Build, publish, deploy
python3 -m processing.build
python3 -m processing.publish
./deploy_reference.sh
```

Note: `processing.build` is also called internally by `discovery.registry.pipeline`, but re-running it after OCR ensures the latest JSON scores are included.

### Annual assessor update

```bash
# 1. Place new .xlsx in CivicTwin/assessor/
# 2. Update discovery/sources.yaml (comment out old entry, add new)
python3 -m processing.build
python3 -m discovery.registry.queue
python3 -m discovery.registry.pipeline --override-robots --confirm
python3 -m processing.publish
./deploy_reference.sh
```

### GIS layer update

```bash
# 1. Export updated CSVs from QGIS → CivicTwin/gis/
python3 -m processing.build
python3 -m processing.migrate_gis_tags
python3 -m processing.publish
./deploy_reference.sh
```

---

## Discovery — command reference

### AgendaCenter pull

> **Pipeline step** — daily incremental step 1. Orchestrates internally: `scrape` → `ingest` → `download`.

```bash
# Daily (default) — scrapes last N days, downloads new PDFs
python3 -m discovery.agenda_center.pull

# Full history scrape (year-by-year from start to today)
python3 -m discovery.agenda_center.pull --full
```

Optional flags: `--limit N` (cap downloads), `--delay SEC` (between PDFs, default 1.0), `--start-date YYYY-MM-DD`, `--end-date YYYY-MM-DD`.

---

### Registry pipeline

> **Pipeline step** — monthly registry refresh. Orchestrates internally: `enumerate` (Tier 1 → Tier 2 → xrefs → town sweep) → `processing.build` → download manifest → [PDF download].

Run `queue` first if assessor data changed (see [Registry queue](#registry-queue) below).

```bash
# Step 1: enumerate + rebuild raw.db + print download manifest (no PDFs yet)
python3 -m discovery.registry.pipeline --override-robots

# Step 2: review manifest, then download PDFs
python3 -m discovery.registry.pipeline --override-robots --confirm
```

`--override-robots` is required every session. Never commit `override_robots: true` in `sources.yaml`.

#### Child: Enumerate (also standalone)

> Called automatically by the pipeline. Run standalone to enumerate without triggering a full pipeline build.

```bash
python3 -m discovery.registry.enumerate --override-robots
```

Optional flags: `--tier2` (name search only), `--limit N`, `--start-after PARCEL_ID`.

#### Child: Download (also standalone)

> Called automatically by the pipeline with `--confirm`. Run standalone to selectively download PDFs after a separate enumerate pass.

```bash
# Step 1: review what will be downloaded
python3 -m discovery.registry.download --override-robots

# Step 2: confirm and download
python3 -m discovery.registry.download --override-robots --confirm

# Cap the run
python3 -m discovery.registry.download --override-robots --confirm --limit 200
```

---

### Registry queue

> **Utility — prerequisite.** Regenerates `target_queue.csv` from `raw.db`. Run before the pipeline after new assessor data. Not called by the pipeline — must be run manually when the parcel set changes.

```bash
# Priority parcels only
python3 -m discovery.registry.queue

# Every parcel (not just priority ones)
python3 -m discovery.registry.queue --full
```

---

### Spread cache expiry

> **Utility — one-time** after initial load.

```bash
python3 -c "
from discovery.registry.cache import spread_expiry
n = spread_expiry()
print(f'{n} entries spread')
"
```

---

## Processing — command reference

### OCR pipeline (sequential pair)

> **Pipeline steps** — daily incremental steps 2–3, monthly registry refresh steps 2–3. Always run Tesseract first, then VLM re-pass on the same input roots.

#### Step 1: Tesseract pass (PDF → keyword-scored JSON)

Runs Tesseract (+ PaddleOCR if available) on every PDF under an input root. Output JSON is written alongside each PDF.

```bash
# Registry documents
python3 -m processing.ocr.ocr_pipeline \
  --input-root /Volumes/DigitalTwin/CivicTwin/registry/documents

# Town agendas / minutes (AgendaCenter)
python3 -m processing.ocr.ocr_pipeline \
  --input-root /Volumes/DigitalTwin/CivicTwin/ma-dennis/agendacenter

# Warrants
python3 -m processing.ocr.ocr_pipeline \
  --input-root /Volumes/DigitalTwin/CivicTwin/ma-dennis/warrants/pdfs
```

Optional flags: `--workers N` (parallel pages, default 4), `--force` (reprocess already-done PDFs), `--dry-run` (list without processing).

#### Step 2: VLM re-pass (targeted enrichment)

Runs a vision-language model (via Ollama) on candidate pages — those with a composite score above the threshold but no confirmed exact match. Requires Ollama running locally with `qwen2.5vl:7b` pulled.

```bash
python3 -m processing.ocr.vlm_repass \
  --input-roots \
    /Volumes/DigitalTwin/CivicTwin/registry/documents \
    /Volumes/DigitalTwin/CivicTwin/ma-dennis/agendacenter \
    /Volumes/DigitalTwin/CivicTwin/ma-dennis/warrants/pdfs
```

Optional flags: `--dry-run` (count candidates without running VLM), `--force` (re-score already-scored pages), `--min-composite FLOAT` (default 0.15), `--vlm-model MODEL`, `--vlm-url URL`.

---

### Build raw.db

> **Pipeline step** — daily incremental step 4, monthly registry refresh step 4. Orchestrates internally: loads all sources → `town_doc_candidates` (parcel link extraction). Also called automatically by `discovery.registry.pipeline`, but re-run after OCR to pick up new scores.

```bash
python3 -m processing.build
```

#### Child: Town doc candidates (also standalone)

> Called automatically at the end of `processing.build`. Run standalone to re-extract parcel references without rebuilding all of `raw.db`.

```bash
python3 -m processing.town_doc_candidates
```

---

### Publish (raw.db → reference.db)

> **Pipeline step** — daily incremental step 5, monthly registry refresh step 5. Copies `raw.db` → `reference.db`, applies any `parcel_corrections` from `transactional.db`.

```bash
python3 -m processing.publish
```

Sync `transactional.db` down first to pick up any portal corrections before publishing:

```bash
rsync ionos-vps:/var/www/dclt-nav/civictwin/db/transactional.db \
  /Volumes/DigitalTwin/CivicTwin/db/
python3 -m processing.publish
```

---

### Tag migrations

> **Utility — one-off, safe to re-run.** Only needed after specific database migrations.

```bash
# OCR keyword scores → transactions.db taggings (run after migration 5)
python3 -m processing.migrate_keywords_to_tags

# GIS layer presence → transactions.db taggings (run after migration 7)
python3 -m processing.migrate_gis_tags

# For Sale layer presence → transactions.db taggings (run after migration 8)
python3 -m processing.migrate_for_sale_tags
```

---

## Deploy

### Code

```bash
git push origin main
```

GitHub Actions SSHs into the VPS, resets, installs requirements, restarts the service.
Monitor at github.com/Forddevstewart/dclt-nav/actions.

### Data (reference.db + PDFs) — preferred: deploy script

> **Pipeline step** — daily incremental step 6. Orchestrates internally: snapshot `transactions.db` → stop service → rsync `reference.db` → rsync PDFs → restart service.

```bash
./deploy_reference.sh
```

### Data — manual rsync

```bash
# Database
rsync -avz --progress \
  /Volumes/DigitalTwin/CivicTwin/db/reference.db \
  ionos-vps:/var/www/dclt-nav/civictwin/db/reference.db

# Registry PDFs + JSON (incremental)
rsync -avz --progress \
  /Volumes/DigitalTwin/CivicTwin/registry/documents/ \
  ionos-vps:/var/www/dclt-nav/civictwin/registry/documents/

# ma-dennis PDFs + JSON (incremental)
rsync -avz --progress \
  /Volumes/DigitalTwin/CivicTwin/ma-dennis/ \
  ionos-vps:/var/www/dclt-nav/civictwin/ma-dennis/
```

---

## Server

### Start local dev server

```bash
flask run -p 5001
```

Or, if you need to test WSGI behaviour directly:

```bash
python3 wsgi.py
```

Runs on http://127.0.0.1:5001. Port 5000 is taken by macOS AirPlay Receiver.

### Logs and status

```bash
ssh ionos-vps
tail -30 /var/log/dclt-nav-error.log
tail -10 /var/log/dclt-nav-access.log
systemctl status dclt-nav
```

### Edit systemd service (requires root)

```bash
ssh root@198.71.50.88
nano /etc/systemd/system/dclt-nav.service
systemctl daemon-reload
systemctl restart dclt-nav
```

---

## Volume backup

> **Utility.** Copy CivicTwin data from DigitalTwin to AbeFroman (incremental, preserving permissions):

```bash
rsync -aH --progress \
  /Volumes/DigitalTwin/CivicTwin/ \
  /Volumes/AbeFroman/CivicTwin/
```

`-a` — archive (recursive, symlinks, permissions, timestamps, owner, group)  
`-H` — preserve hard links  
`-X` — preserve extended attributes (macOS resource forks, Finder metadata)  
`--progress` — per-file transfer progress

Add `--delete` if you want AbeFroman to mirror DigitalTwin exactly (removes files deleted from source):

```bash
rsync -aHX --delete --progress \
  /Volumes/DigitalTwin/CivicTwin/ \
  /Volumes/AbeFroman/CivicTwin/
```
