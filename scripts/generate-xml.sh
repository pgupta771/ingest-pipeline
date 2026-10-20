#!/usr/bin/env bash
set -euo pipefail

# Generates a deterministic 155 MB+ ECB-shaped XML file for memory and throughput benchmarks.
OUTPUT_FILE="${1:-/tmp/ecb-synthetic-155mb.xml}"
TARGET_DAYS="${2:-320000}"

echo "[generate-xml] Generating ~155 MB deterministic ECB XML at $OUTPUT_FILE..."

awk -v days="$TARGET_DAYS" 'BEGIN {
  print "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
  print "<gesmes:Envelope xmlns:gesmes=\"http://www.gesmes.org/xml/2002-08-01\" xmlns=\"http://www.ecb.int/vocabulary/2002-08-01/eurofxref\">"
  print "  <Cube>"
  split("USD GBP JPY CHF CAD AUD SEK NOK DKK PLN", curr, " ")
  for (d = 1; d <= days; d++) {
    printf "    <Cube time=\"2026-%02d-%02d-%06d\">\n", (d % 12) + 1, (d % 28) + 1, d
    for (c = 1; c <= 10; c++) {
      printf "      <Cube currency=\"%s\" rate=\"1.%04d\"/>\n", curr[c], ((d + c) % 9000) + 1000
    }
    print "    </Cube>"
  }
  print "  </Cube>"
  print "</gesmes:Envelope>"
}' > "$OUTPUT_FILE"

SIZE_MB=$(du -m "$OUTPUT_FILE" | cut -f1)
echo "[generate-xml] Done: $OUTPUT_FILE (${SIZE_MB} MB)"
