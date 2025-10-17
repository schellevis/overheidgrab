#!/usr/bin/env bash
set -euo pipefail

# Gebruik: ./join_duckdb.sh [BRONMAP] [UITBESTAND]
# Voorbeeld: ./join_duckdb.sh chunks duck.db

IN_DIR="${1:-chunks}"
OUT_FILE="${2:-azc.db}"

if [[ ! -d "$IN_DIR" ]]; then
  echo "Map niet gevonden: $IN_DIR" >&2
  exit 1
fi

shopt -s nullglob
chunks=( "$IN_DIR"/*.chunk )
shopt -u nullglob

if (( ${#chunks[@]} == 0 )); then
  echo "Geen .chunk-bestanden gevonden in $IN_DIR" >&2
  exit 1
fi

# Controleer chunk-checksums
if [[ -f "$IN_DIR/manifest.sha256" ]]; then
  echo "Controleer chunk-checksums..."
  if command -v sha256sum >/dev/null; then
    ( cd "$IN_DIR" && sha256sum -c manifest.sha256 )
  else
    ( cd "$IN_DIR" && shasum -a 256 -c manifest.sha256 )
  fi
fi

# Sorteer chunks (lexicografisch is goed voor aaaa, aaab, aaac...)
sorted_chunks=$(printf '%s\n' "${chunks[@]}" | sort)

tmp="${OUT_FILE}.tmp"
: > "$tmp"
# shellcheck disable=SC2086
cat $sorted_chunks >> "$tmp"

# Checksum (tool-afhankelijk) en grootte (portable)
if command -v sha256sum >/dev/null; then
  NEW_CHECKSUM=$(sha256sum "$tmp" | awk '{print $1}')
else
  NEW_CHECKSUM=$(shasum -a 256 "$tmp" | awk '{print $1}')
fi
ACTUAL_SIZE=$(wc -c < "$tmp" | tr -d ' ')

# Verwachte waarden (optioneel)
EXPECTED_SIZE=""
EXPECTED_CHECKSUM=""
if [[ -f "$IN_DIR/meta.txt" ]]; then
  EXPECTED_SIZE=$(grep '^size_bytes=' "$IN_DIR/meta.txt" | cut -d= -f2 || true)
  EXPECTED_CHECKSUM=$(grep '^original_checksum=' "$IN_DIR/meta.txt" | cut -d= -f2 || true)
fi

mv -f "$tmp" "$OUT_FILE"

echo "✅ Reconstructie klaar: $OUT_FILE"
echo "Grootte : $ACTUAL_SIZE bytes"
echo "SHA256  : $NEW_CHECKSUM"

if [[ -n "$EXPECTED_SIZE" ]]; then
  [[ "$EXPECTED_SIZE" = "$ACTUAL_SIZE" ]] && echo "Grootte ✔" || echo "⚠ Grootte mismatch!"
fi
if [[ -n "$EXPECTED_CHECKSUM" ]]; then
  [[ "$EXPECTED_CHECKSUM" = "$NEW_CHECKSUM" ]] && echo "Checksum ✔" || echo "⚠ Checksum mismatch!"
fi
