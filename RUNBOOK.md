# CivicTwin Pipeline Runbook

## Overview

The pipeline ingests several source types into a local SQLite database (`raw.db`) and a
read-only server copy (`reference.db`). The Barnstable Registry of Deeds is the only source
that requires active network scraping; everything else is either a manually-placed file or a
one-time download.

**Data root:** `/Volumes/DigitalTwin/CivicTwin/` (external volume, set via `CIVICTWIN_ROOT`)

---

## Triggers

| Trigger | Frequency | What changes |
|---------|-----------|--------------|
| New assessor data (ADS) | Annually (~January) | Parcel ownership, deed references, assessed values |
| Registry refresh | Monthly | Newly recorded deeds, conservation restrictions, takings |
| GIS layer update | As needed (MassGIS releases) | Habitat, wetlands, soils, open space overlays |
| Deploy to server | After any rebuild | Publish `reference.db` |

---

## One-time: Spread cache expiry after initial load

After the initial enumeration and download completes, all cache entries have
timestamps from the same week and will expire simultaneously — causing the
first weekly run after 30 days to re-fetch everything at once.

Run this once to spread expiry timestamps deterministically across each entry's
staleness window:

```bash
python3 -c "
from discovery.registry.cache import spread_expiry
n = spread_expiry()
print(f'{n} entries spread')
"
```

This is safe to re-run. The same parcel always gets the same jitter offset, so
re-running is idempotent. After this, each weekly pipeline run touches roughly
1/39th of parcel entries and 1/4 of sweep entries.

Staleness windows by entry type:

| Entry type | Staleness | Rationale |
|------------|-----------|-----------|
| Parcel (Tier 1/2) | 365 days | Deed records don't change; new recordings caught by sweep |
| Sweep (`sweep-denn-*`) | 30 days | Town records new instruments continuously |
| Xref (`xref-*`) | 90 days | Cross-references stable but worth refreshing quarterly |

---

## Monthly: Registry refresh

Run this when a month or more has passed since the last enumeration. The 30-day cache
staleness window means anything older than that is automatically re-fetched.

```bash
cd /Users/fordstewart/Projects/dclt-nav

# Step 1: enumerate + build + print download manifest (no PDFs yet)
python3 -m discovery.registry.pipeline --override-robots

# Step 2: review the manifest, then download PDFs
python3 -m discovery.registry.pipeline --override-robots --confirm
```

`--override-robots` is required every session. It is a deliberate acknowledgement of the
robots.txt disallow on `search.barnstabledeeds.org`. Do not set `override_robots: true` in
`sources.yaml` and commit it.

The pipeline runs four enumeration steps in sequence (Tier 1 book/page lookups, Tier 2 name
searches, cross-reference expansion, Town of Dennis sweep), then rebuilds `raw.db`, then
downloads any newly found PDFs. If interrupted, re-run from the top — cache hits skip
completed work.

After the pipeline completes, publish to server if needed:

```bash
python3 -m processing.publish
```

---

## Annually: New assessor data (ADS)

When a new assessor extract arrives (typically a `.xlsx` with a `BT_Extract` sheet):

**Step 1 — Place the file on the volume**

```
CivicTwin/assessor/2027 Assessor Database - 2027.01.XX.xlsx
```

**Step 2 — Update `sources.yaml`**

In `discovery/sources.yaml`, under `assessor.files`, comment out the old entry and add the
new one. Keep old entries for reference.

```yaml
assessor:
  files:
    # - id: annual_extract_2026
    #   path: assessor/2026 Assessor Database - 2026.01.13.x.xlsx
    #   ...
    - id: annual_extract_2027
      path: assessor/2027 Assessor Database - 2027.01.XX.xlsx
      description: 2027 annual assessor extract, received 2027-01-XX
      format: xlsx
      sheet: BT_Extract
```

**Step 3 — Rebuild the database**

```bash
python3 -m processing.build
```

This rebuilds `raw.db` from all sources: the new assessor extract, existing GIS layers,
warrants, and the current registry index cache.

**Step 4 — Regenerate the registry target queue**

The queue CSV controls which parcels enumerate searches. It is derived from priority scores
in `raw.db`, so it must be regenerated after a build whenever parcel ownership or scores
may have changed.

```bash
python3 -m discovery.registry.queue
```

This writes `CivicTwin/registry/queue/target_queue.csv`. The `--full` flag queues every
parcel (not just priority ones) if you need comprehensive coverage.

**Step 5 — Run the full registry pipeline**

Because parcels may have changed owners or deed references, expire the existing index
cache before re-enumerating. Delete stale entries if you want a clean re-fetch, otherwise
the 30-day staleness window handles it automatically.

```bash
python3 -m discovery.registry.pipeline --override-robots --confirm
```

**Step 6 — Publish**

```bash
python3 -m processing.publish
```

---

## GIS layer update

GIS layers are manually exported from QGIS and placed in `CivicTwin/gis/`. The field mapping
and join procedure for each layer is documented in `discovery/GIS Layers.md`.

After placing updated CSVs:

```bash
python3 -m processing.build
python3 -m processing.publish
```

No registry work needed unless the parcel geometry itself changed (new parcels created by
subdivision). If it did, re-run the queue step and a fresh enumeration pass.

---

## Publish to server

```bash
python3 -m processing.publish
```

Copies `raw.db` to `reference.db`, then applies any `parcel_corrections` rows from
`transactional.db` (server-side user edits synced down). If `transactional.db` is absent
the copy still completes. `reference.db` is the file deployed to the server.

Sync `transactional.db` down before publishing if you want corrections included:

```bash
rsync ionos-vps:/var/www/dclt-nav/civictwin/db/transactional.db \
  /Volumes/DigitalTwin/CivicTwin/db/
python3 -m processing.publish
```

---

## Deploy to VPS

### Code — push to trigger GitHub Actions

```bash
git push origin main
```

The Actions workflow SSHs into the VPS, runs `git reset --hard origin/main`,
`pip install -r requirements.txt`, and `systemctl restart dclt-nav`. Monitor
the result at github.com/Forddevstewart/dclt-nav/actions.

### Data — rsync reference.db and PDFs

Run `processing.publish` first to build a fresh `reference.db`, then sync:

```bash
# Database (run after every processing.publish)
rsync -avz --progress \
  /Volumes/DigitalTwin/CivicTwin/db/reference.db \
  ionos-vps:/var/www/dclt-nav/civictwin/db/reference.db

# Registry PDFs (incremental — only new files transfer)
rsync -avz --progress \
  /Volumes/DigitalTwin/CivicTwin/registry/documents/ \
  ionos-vps:/var/www/dclt-nav/civictwin/registry/documents/
```

Uses the `ionos-vps` alias from `~/.ssh/config`. No restart needed after data sync.

### Server layout

```
/var/www/dclt-nav/
  civictwin/
    db/
      reference.db       ← rsynced from local volume
      transactional.db   ← born on server, never overwritten
    registry/
      documents/         ← rsynced from local volume
```

`CIVICTWIN_ROOT=/var/www/dclt-nav/civictwin` must be set in the systemd service.

### Editing the systemd service (requires root)

The `deployer` user can only `sudo systemctl restart dclt-nav`. To edit the
service file, SSH as root:

```bash
ssh root@198.71.50.88
nano /etc/systemd/system/dclt-nav.service
systemctl daemon-reload
systemctl restart dclt-nav
```

### Checking logs on the server

```bash
ssh ionos-vps
tail -30 /var/log/dclt-nav-error.log
tail -10 /var/log/dclt-nav-access.log
systemctl status dclt-nav
```

---

## Scripts reference

| Command | When to run | Description |
|---------|-------------|-------------|
| `processing.build` | After new ADS, GIS update, or registry pipeline | Rebuilds `raw.db` from all sources |
| `processing.publish` | After any build, before deploy | Copies `raw.db` → `reference.db`, applies corrections |
| `discovery.registry.queue` | After new ADS | Regenerates `target_queue.csv` from `raw.db` |
| `discovery.registry.pipeline` | Monthly | Full enumeration → build → download |
| `discovery.registry.enumerate` | (called by pipeline) | Tier 1 + Tier 2 parcel-level lookups |
| `discovery.registry.sweep` | (called by pipeline) | Cross-ref expansion + Town of Dennis sweep |
| `discovery.registry.download` | (called by pipeline) | Download PDFs for approved instruments |

### Key flags

```
pipeline --override-robots    Required for any registry network access
pipeline --confirm            Also download PDFs (default: manifest only)
pipeline --limit N            Cap network requests per step (useful for testing)
queue    --full               Queue every parcel, not just priority ones
```

---

## Data locations

```
CivicTwin/
  assessor/                  Annual assessor Excel extracts (manually placed)
  gis/                       MassGIS GeoJSON and CSV layers (manually placed)
  ma-dennis/                 DocumentCenter and AgendaCenter PDFs (scraped)
  registry/
    index/                   Per-parcel deed index JSON (enumerate cache)
    documents/               Downloaded deed scan PDFs
    queue/target_queue.csv   Current parcel target list
  db/
    raw.db                   Built locally; source of truth
    reference.db             Deployed to server (built by publish)
    transactional.db         Server-side user data; sync down before publish

discovery/output/            Logs, reports, access log (gitignored)
  registry_access.log        Every HTTP request to the registry
  registry_pipeline_report.txt
  registry_enumerate_report.txt
  registry_sweep_report.txt
  registry_download_report.txt
```

---

## Rate limiting and throttle behavior

The registry rate limiter enforces a minimum 2-second delay between requests and a 15-second
pause every 100 requests. If the server responds slowly it activates a 10-second adaptive
delay for the next 10 requests.

An HTTP 429 or 503 response raises `RegistryThrottleError` and stops the pipeline
immediately. If this happens, wait at least 30 minutes before retrying. Do not
increase batch sizes or reduce delays without evidence the server can handle it.

---

## Notes

- **robots.txt:** `search.barnstabledeeds.org` disallows all automated access. The
  override is justified as civic research on public records. Use `--override-robots`
  on the CLI; never commit `override_robots: true` in `sources.yaml` on main.

- **Page cap subdivision:** The town sweep date-windows results to 5 pages × 30 = 150
  raw results per window. Busy windows (typically Dennis as grantor/grantee post-1950)
  automatically subdivide into 1-year sub-windows. Sub-year results are cached with pids
  like `sweep-denn-G-1960-1960` and are reused on re-runs.

- **Cache staleness:** 30 days. After 30 days any cached index entry is considered stale
  and re-fetched. This means a monthly pipeline run will re-check everything over a month
  old — mostly cache hits, new documents picked up automatically.

- **Download filter:** The download step fetches only approved instrument types (deeds,
  conservation restrictions, takings, easements, certificates of vote, etc.). Mortgages,
  discharges, liens, and homestead declarations are excluded.
