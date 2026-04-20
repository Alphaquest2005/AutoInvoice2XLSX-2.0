#!/bin/bash
# Re-run pipeline on all folders that had unprocessed PDFs copied back.
# Run from repo root: bash scripts/rerun_unprocessed.sh

set -e
cd "$(dirname "$0")/.."

REPORT="workspace/rerun_unprocessed_report.json"
TIMEOUT=900

FOLDERS=(
  "03152025110538"
  "03152025135238"
  "03152025_FASHIOMiOVA"
  "03152025_FASHIOMWVA"
  "03152025_Free_returns_within_90_days"
  "03152025_Order_information"
  "03152025_Order_totai"
  "03152025_Payment_Grand_TotahXCD_102.48_Visa_ending_in_9228_July"
  "03152025_Payment_information"
  "03152025_Price_59.92"
  "03152025_RECEIPT"
  "03152025_USD_279.46_XCD_765.72_765.72"
)

echo "Processing ${#FOLDERS[@]} folders..."
echo ""

for i in "${!FOLDERS[@]}"; do
  folder="${FOLDERS[$i]}"
  n=$((i + 1))
  echo "=== [$n/${#FOLDERS[@]}] $folder ==="
  .venv/bin/python scripts/rerun_corpus.py \
    --folder "$folder" \
    --no-backup \
    --timeout "$TIMEOUT" \
    --report "$REPORT" 2>&1
  echo ""
done

echo "All done. Report: $REPORT"
