#!/bin/bash
####
# Split MySQL dump SQL file into one file per table
# macOS-compatible version using GNU coreutils (gcsplit)
# based on https://gist.github.com/jasny/1608062
####

if [ $# -lt 1 ] ; then
  echo "USAGE: $0 DUMP_FILE [OUTPUT_DIR]"
  exit 1
fi

DUMP_FILE="$1"
OUTPUT_DIR="${2:-.}"

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR" || exit 1

# Use GNU csplit on macOS
CSPLIT="csplit"
if command -v gcsplit &> /dev/null; then
  CSPLIT="gcsplit"
fi

# Split on "Table structure for table" markers
$CSPLIT -s -f table "$DUMP_FILE" "/-- Table structure for table/" "{*}" 2>/dev/null

if [ $? -ne 0 ]; then
  exit 1
fi

# Remove header file (table00)
rm -f table00

# Rename files based on table name
for FILE in table*; do
  if [ -f "$FILE" ]; then
    # Extract table name from backticks
    NAME=$(grep -m1 -oE '\`[^\`]+\`' "$FILE" | head -1 | tr -d '\`')
    if [ -n "$NAME" ]; then
      mv "$FILE" "${NAME}.sql" 2>/dev/null
    else
      rm -f "$FILE"
    fi
  fi
done
