#!/usr/bin/env bash
set -euo pipefail

# Gebruik: ./split_duckdb.sh [BRONBESTAND] [DOELMAP] [CHUNK_GROOTTE]
# Voorbeeld: ./split_duckdb.sh duck.db chunks 10M   (macOS: liever 10m)

rm -r chunks/*

SRC_FILE="${1:-azc.db}"
OUT_DIR="${2:-chunks}"
CHUNK_SIZE="${3:-10M}"

if [[ ! -f "$SRC_FILE" ]]; then
  echo "Bestand niet gevonden: $SRC_FILE" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

base="$(basename -- "$SRC_FILE")"
prefix="$OUT_DIR/${base}.part_"

# Bepaal checksum-tool
if command -v sha256sum >/dev/null; then
  CHECKSUM=$(sha256sum "$SRC_FILE" | awk '{print $1}')
else
  CHECKSUM=$(shasum -a 256 "$SRC_FILE" | awk '{print $1}')
fi

# Grootte (portable)
FILESIZE=$(wc -c < "$SRC_FILE" | tr -d ' ')

# Splitsen (BSD split kent geen -d of --additional-suffix; dat is ok)
if split --version >/dev/null 2>&1; then
  # GNU split beschikbaar -> mag -d gebruiken voor numerieke suffix, maar hoeft niet
  split -b "$CHUNK_SIZE" -d -a 4 "$SRC_FILE" "$prefix"
else
  # BSD/macOS split
  split -b "$CHUNK_SIZE" -a 4 "$SRC_FILE" "$prefix"
fi

# Hernoem alleen de echte part-bestanden naar .chunk
shopt -s nullglob
for f in "$OUT_DIR"/*.part_*; do
  mv "$f" "$f.chunk"
done
shopt -u nullglob

# Manifest en metadata
(
  cd "$OUT_DIR"
  if command -v sha256sum >/dev/null; then
    sha256sum -- *.chunk > manifest.sha256
  else
    shasum -a 256 -- *.chunk > manifest.sha256
  fi
  {
    echo "filename=$base"
    echo "size_bytes=$FILESIZE"
    echo "chunk_size=$CHUNK_SIZE"
    echo "chunk_count=$(ls -1 *.chunk | wc -l | tr -d ' ')"
    echo "prefix=${base}.part_"
    echo "original_checksum=$CHECKSUM"
  } > meta.txt
)

echo "✅ Splitsen klaar."
echo "Bestand : $SRC_FILE"
echo "Grootte : $FILESIZE bytes"
echo "SHA256  : $CHECKSUM"
echo "Chunks  : $OUT_DIR"
