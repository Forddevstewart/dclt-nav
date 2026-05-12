#!/usr/bin/env bash
# Full sync: AbeFroman backup → Ionos deploy.
#
# Usage: ./sync.sh
#
# Order of operations:
#   1. Backup DigitalTwin/CivicTwin → AbeFroman (local safety net first)
#      - All files rsynced incrementally with --delete
#      - Live .db files hot-copied via SQLite .backup (WAL-safe, no server stop)
#   2. Snapshot + pull transactions.db from Ionos
#   3. Pull current reference.db from Ionos as local backup
#   4. Stop server, push reference.db + all CivicTwin content, start server
#
# Requires SSH access as root@ionos-vps (systemctl runs without sudo).

set -euo pipefail

SRC="/Volumes/DigitalTwin/CivicTwin"
ABE="/Volumes/AbeFroman/CivicTwin"

VPS="root@ionos-vps"
APP="/var/www/dclt-nav"
CIVICTWIN="$APP/civictwin"

LOCAL_DB_BACKUPS="$SRC/db"
STAMP=$(date -u +"%Y%m%d_%H%M%S")
SOCKET="/tmp/dclt-deploy-ssh-$STAMP.sock"

DATABASES=(dennis.db dennis_civic.db raw.db reference.db transactions.db)

# ── Preflight ──────────────────────────────────────────────────────────────────

if [[ ! -d "$SRC" ]]; then echo "ERROR: DigitalTwin not mounted at $SRC" >&2; exit 1; fi
if [[ ! -d "$ABE" ]]; then echo "ERROR: AbeFroman not mounted at $ABE"  >&2; exit 1; fi

# ── 1. AbeFroman backup ────────────────────────────────────────────────────────

echo "==> [1/4] Syncing files to AbeFroman (excluding db/)"
rsync -avz --delete --progress \
  --exclude='db/' \
  "$SRC/" "$ABE/"

echo "==> [1/4] Hot-backing up databases to AbeFroman"
mkdir -p "$ABE/db"
for db in "${DATABASES[@]}"; do
  src_file="$SRC/db/$db"
  if [[ ! -f "$src_file" ]]; then echo "    Skipping $db (not found)"; continue; fi
  echo "    $db"
  sqlite3 "$src_file" ".backup $ABE/db/$db"
done

# ── SSH multiplex (one passphrase prompt for all Ionos steps) ─────────────────

echo "==> Opening SSH connection to $VPS"
ssh -M -S "$SOCKET" -o ControlPersist=60 -fN "$VPS"
SSH="ssh -S $SOCKET"
RSYNC_SSH="ssh -S $SOCKET"

cleanup() { ssh -S "$SOCKET" -O exit "$VPS" 2>/dev/null || true; }
trap cleanup EXIT

# ── 2. Pull transactions.db from Ionos ────────────────────────────────────────

echo "==> [2/4] Snapshot transactions.db on server"
$SSH "$VPS" "cp $APP/data/transactions.db $APP/data/transactions.db.bak.$STAMP"

echo "==> [2/4] Rotate server backups (keep 10)"
$SSH "$VPS" "ls -1t $APP/data/transactions.db.bak.* 2>/dev/null | tail -n +11 | xargs -r rm --"

echo "==> [2/4] Pull transactions.db to local backup"
rsync -az --progress -e "$RSYNC_SSH" \
  "$VPS:$APP/data/transactions.db.bak.$STAMP" \
  "$LOCAL_DB_BACKUPS/transactions.$STAMP"
echo "    Saved → $LOCAL_DB_BACKUPS/transactions.$STAMP"

# ── 3. Pull reference.db from Ionos as local backup ───────────────────────────

echo "==> [3/4] Pull reference.db from server to local backup"
rsync -az --progress -e "$RSYNC_SSH" \
  "$VPS:$CIVICTWIN/db/reference.db" \
  "$LOCAL_DB_BACKUPS/reference.$STAMP"
echo "    Saved → $LOCAL_DB_BACKUPS/reference.$STAMP"

# ── 4. Deploy to Ionos ────────────────────────────────────────────────────────

echo "==> [4/4] Stop server"
$SSH "$VPS" "systemctl stop dclt-nav"

echo "==> [4/4] Sync reference.db"
rsync -avz --progress -e "$RSYNC_SSH" \
  "$SRC/db/reference.db" "$VPS:$CIVICTWIN/db/reference.db"

echo "==> [4/4] Sync registry PDFs (incremental)"
rsync -avz --progress -e "$RSYNC_SSH" \
  "$SRC/registry/documents/" "$VPS:$CIVICTWIN/registry/documents/"

echo "==> [4/4] Sync ma-dennis PDFs + JSON (incremental)"
rsync -avz --progress -e "$RSYNC_SSH" \
  "$SRC/ma-dennis/" "$VPS:$CIVICTWIN/ma-dennis/"

echo "==> [4/4] Sync GIS files"
rsync -avz --progress -e "$RSYNC_SSH" \
  --include="*.geojson" --include="*.csv" --exclude="*" \
  "$SRC/gis/" "$VPS:$CIVICTWIN/gis/"

echo "==> [4/4] Start server"
$SSH "$VPS" "systemctl start dclt-nav"

echo ""
echo "==> Done."
echo "    Server backup : $APP/data/transactions.db.bak.$STAMP"
echo "    Local backups : $LOCAL_DB_BACKUPS/transactions.$STAMP"
echo "                    $LOCAL_DB_BACKUPS/reference.$STAMP"
