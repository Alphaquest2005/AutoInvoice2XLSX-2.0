#!/bin/bash
# One-off reruner for the 13 phase-2 zero/partial-invoice folders.
# Emits one JSON per folder under workspace/rerun_phase3_reports/.
set -u
cd "/mnt/c/Insight Software/AutoInvoice2XLSX.2.0"

folders=(
  "03152025110538"
  "03152025135830"
  "03152025_140.56_0.00_0.00_-_7.40_140.56_USD_you_saved_7.40"
  "03152025_8_18_24_9_05_PM_ai_on.com_"
  "03152025_ASHIONNOVA"
  "03152025_FASHIOMWVA"
  "03152025_FASHIOMiOVA"
  "03152025_Jul_16_2024_order_Order_2000120-11892676"
  "03152025_Order_totai"
  "03152025_Price_59.92"
  "03152025_RECEIPT"
  "03152025_Sales_Invoice"
  "03152025_USD_279.46_XCD_765.72_765.72"
)

reports_dir="workspace/rerun_phase3_reports"
mkdir -p "$reports_dir"

i=0
total=${#folders[@]}
for f in "${folders[@]}"; do
  i=$((i+1))
  # sanitize filename
  safe=$(printf '%s' "$f" | tr '/ ' '__')
  log="${reports_dir}/${safe}.log"
  rpt="${reports_dir}/${safe}.json"
  printf '=== [%d/%d] %s ===\n' "$i" "$total" "$f" | tee -a "${reports_dir}/_master.log"
  printf 'START: %s\n' "$(date -u +%FT%TZ)" >> "$log"
  .venv/bin/python scripts/rerun_corpus.py \
      --folder "$f" \
      --no-backup \
      --report "$rpt" \
      --timeout 900 \
    >> "$log" 2>&1
  rc=$?
  printf 'END: %s  rc=%s\n' "$(date -u +%FT%TZ)" "$rc" >> "$log"
  printf '    rc=%s (log: %s)\n' "$rc" "$log" | tee -a "${reports_dir}/_master.log"
done

printf 'ALL DONE: %s\n' "$(date -u +%FT%TZ)" | tee -a "${reports_dir}/_master.log"
