# CivicTwin Common Commands

Working directory for all commands: `/Users/fordstewart/Projects/dclt-nav`

---

## Discovery

### Registry (monthly)

```bash
# Step 1: enumerate + rebuild raw.db + print download manifest (no PDFs yet)
python3 -m discovery.registry.pipeline --override-robots

# Step 2: review manifest, then download PDFs
python3 -m discovery.registry.pipeline --override-robots --confirm
```

`--override-robots` is required every session. Never commit `override_robots: true` in `sources.yaml`.

### Town agendas & minutes (AgendaCenter)

```bash
# Daily (default) — scrapes last N days, downloads new PDFs
python3 -m discovery.agenda_center.pull

# Full history scrape (year-by-year from start to today)
python3 -m discovery.agenda_center.pull --full
```

Optional flags: `--limit N` (cap downloads), `--delay SEC` (between PDFs, default 1.0), `--start-date YYYY-MM-DD`, `--end-date YYYY-MM-DD`.

### Registry queue (after new assessor data)

```bash
# Regenerate target_queue.csv from raw.db
python3 -m discovery.registry.queue

# Queue every parcel (not just priority ones)
python3 -m discovery.registry.queue --full
```

### Spread cache expiry (one-time after initial load)

```bash
python3 -c "
from discovery.registry.cache import spread_expiry
n = spread_expiry()
print(f'{n} entries spread')
"
```

---

## Processing

### Build raw.db

```bash
python3 -m processing.build
```

Run after: new assessor data, GIS layer update, or registry pipeline completion.

### Publish (raw.db → reference.db)

```bash
python3 -m processing.publish
```

Copies `raw.db` → `reference.db`, applies any `parcel_corrections` from `transactional.db`.

### Sync transactional.db down before publishing

```bash
rsync ionos-vps:/var/www/dclt-nav/civictwin/db/transactional.db \
  /Volumes/DigitalTwin/CivicTwin/db/
python3 -m processing.publish
```

### Tag migrations (one-off, safe to re-run)

```bash
# OCR keyword scores → dclt.db taggings (run after migration 5)
python3 -m processing.migrate_keywords_to_tags

# GIS layer presence → dclt.db taggings (run after migration 7)
python3 -m processing.migrate_gis_tags

# For Sale layer presence → dclt.db taggings (run after migration 8)
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

```bash
./deploy_reference.sh
```

Snapshots `dclt.db` on the server, stops the service, rsyncs `reference.db` and PDFs, restarts.

### Data — manual rsync

```bash
# Database
rsync -avz --progress \
  /Volumes/DigitalTwin/CivicTwin/db/reference.db \
  ionos-vps:/var/www/dclt-nav/civictwin/db/reference.db

# Registry PDFs (incremental)
rsync -avz --progress \
  /Volumes/DigitalTwin/CivicTwin/registry/documents/ \
  ionos-vps:/var/www/dclt-nav/civictwin/registry/documents/
```

---

## Server

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

Copy CivicTwin data from DigitalTwin to AbeFroman (incremental, preserving permissions):

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

---

## Standard pipeline sequences

### Monthly registry refresh

```bash
python3 -m discovery.registry.pipeline --override-robots
python3 -m discovery.registry.pipeline --override-robots --confirm
python3 -m processing.build
python3 -m processing.publish
./deploy_reference.sh
```

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
