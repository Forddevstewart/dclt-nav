#!/usr/bin/env bash
# Deploy a new reference.db and PDF folder to the VPS.
# Run from the local Mac after processing.publish has completed.
#
# Usage: ./deploy_reference.sh
#
# Safety:
#   - Snapshots dclt.db on the server before deploying.
#   - Stops the server before swapping reference.db and PDFs.
#   - Restarts the server after the swap.
#   - The server must have DCLT_ENV=production set (systemd service).

set -euo pipefail

VPS="ionos-vps"
APP="/var/www/dclt-nav"
CIVICTWIN="$APP/civictwin"
LOCAL_DB="/Volumes/DigitalTwin/CivicTwin/db/reference.db"
LOCAL_PDFS="/Volumes/DigitalTwin/CivicTwin/registry/documents/"

STAMP=$(date -u +"%Y%m%dT%H%M%SZ")

echo "==> Snapshot dclt.db on server"
ssh "$VPS" "cp $APP/data/dclt.db $APP/data/dclt.db.bak.$STAMP"

echo "==> Stop server"
ssh "$VPS" "sudo systemctl stop dclt-nav"

echo "==> Sync reference.db"
rsync -avz --progress "$LOCAL_DB" "$VPS:$CIVICTWIN/db/reference.db"

echo "==> Sync registry PDFs (incremental)"
rsync -avz --progress "$LOCAL_PDFS" "$VPS:$CIVICTWIN/registry/documents/"

echo "==> Start server"
ssh "$VPS" "sudo systemctl start dclt-nav"

echo "==> Done. Backup at $APP/data/dclt.db.bak.$STAMP"
