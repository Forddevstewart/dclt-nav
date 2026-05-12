#!/usr/bin/env bash
# Backup CivicTwin to AbeFroman.
#
# Usage: ./backup_civictwin.sh
#
# Safe to run while the server is up. All .db files are backed up via
# SQLite's .backup command (checkpoints WAL before writing) rather than
# rsync, so the destination is always consistent. Everything else is
# rsynced incrementally with --delete.

set -euo pipefail

SRC="/Volumes/DigitalTwin/CivicTwin"
DST="/Volumes/AbeFroman/CivicTwin"

DATABASES=(
  dennis.db
  dennis_civic.db
  raw.db
  reference.db
  transactions.db
)

# Check both drives are mounted
if [[ ! -d "$SRC" ]]; then
  echo "ERROR: Source not mounted at $SRC" >&2; exit 1
fi
if [[ ! -d "$DST" ]]; then
  echo "ERROR: Destination not mounted at $DST" >&2; exit 1
fi

echo "==> Syncing files (excluding db/)"
rsync -avz --delete --progress \
  --exclude='db/' \
  "$SRC/" "$DST/"

echo "==> Backing up databases"
mkdir -p "$DST/db"
for db in "${DATABASES[@]}"; do
  src_file="$SRC/db/$db"
  dst_file="$DST/db/$db"
  if [[ ! -f "$src_file" ]]; then
    echo "    Skipping $db (not found)"
    continue
  fi
  echo "    $db"
  sqlite3 "$src_file" ".backup $dst_file"
done

echo "==> Done"
