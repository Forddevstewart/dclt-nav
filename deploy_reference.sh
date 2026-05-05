#!/usr/bin/env bash
# Deploy a new reference.db and PDF folder to the VPS.
# Run from the local Mac after processing.publish has completed.
#
# Usage: ./deploy_reference.sh
#
# Safety:
#   - Snapshots transactions.db on the server before deploying.
#   - Stops the server before swapping reference.db and PDFs.
#   - Restarts the server after the swap.
#   - The server must have DCLT_ENV=production set (systemd service).
#
# Requires SSH access as root (systemctl runs without sudo).

set -euo pipefail

VPS="root@ionos-vps"
APP="/var/www/dclt-nav"
CIVICTWIN="$APP/civictwin"
LOCAL_DB="/Volumes/DigitalTwin/CivicTwin/db/reference.db"
LOCAL_PDFS="/Volumes/DigitalTwin/CivicTwin/registry/documents/"
LOCAL_MA_DENNIS="/Volumes/DigitalTwin/CivicTwin/ma-dennis/"
LOCAL_GIS="/Volumes/DigitalTwin/CivicTwin/gis/"

STAMP=$(date -u +"%Y%m%d_%H%M%S")
SOCKET="/tmp/dclt-deploy-ssh-$STAMP.sock"
LOCAL_DB_BACKUPS="/Volumes/DigitalTwin/CivicTwin/db"

# Open one multiplexed SSH connection; all subsequent ssh/rsync calls reuse it.
# Key passphrase is entered once here.
echo "==> Opening SSH connection to $VPS"
ssh -M -S "$SOCKET" -o ControlPersist=60 -fN "$VPS"
SSH="ssh -S $SOCKET"
RSYNC_SSH="ssh -S $SOCKET"

cleanup() {
  ssh -S "$SOCKET" -O exit "$VPS" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Snapshot transactions.db on server"
$SSH "$VPS" "cp $APP/data/transactions.db $APP/data/transactions.db.bak.$STAMP"

echo "==> Rotate server backups (keep 10)"
$SSH "$VPS" "ls -1t $APP/data/transactions.db.bak.* 2>/dev/null | tail -n +11 | xargs -r rm --"

echo "==> Pull transactions.db to local backup"
rsync -az --progress -e "$RSYNC_SSH" \
  "$VPS:$APP/data/transactions.db.bak.$STAMP" \
  "$LOCAL_DB_BACKUPS/transactions.$STAMP"
echo "    Saved to $LOCAL_DB_BACKUPS/transactions.$STAMP"

echo "==> Stop server"
$SSH "$VPS" "systemctl stop dclt-nav"

echo "==> Sync reference.db"
rsync -avz --progress -e "$RSYNC_SSH" "$LOCAL_DB" "$VPS:$CIVICTWIN/db/reference.db"

echo "==> Sync registry PDFs (incremental)"
rsync -avz --progress -e "$RSYNC_SSH" "$LOCAL_PDFS" "$VPS:$CIVICTWIN/registry/documents/"

echo "==> Sync ma-dennis PDFs + JSON (incremental)"
rsync -avz --progress -e "$RSYNC_SSH" "$LOCAL_MA_DENNIS" "$VPS:$CIVICTWIN/ma-dennis/"

echo "==> Sync GIS files (parcel geometry + overlays)"
rsync -avz --progress -e "$RSYNC_SSH" --include="*.geojson" --include="*.csv" --exclude="*" "$LOCAL_GIS" "$VPS:$CIVICTWIN/gis/"

echo "==> Start server"
$SSH "$VPS" "systemctl start dclt-nav"

echo "==> Done. Server backup: $APP/data/transactions.db.bak.$STAMP"
echo "    Local backup:  $LOCAL_DB_BACKUPS/transactions.$STAMP"
