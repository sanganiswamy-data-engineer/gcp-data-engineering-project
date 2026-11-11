#!/usr/bin/env bash
set -euo pipefail
PROJECT="${PROJECT:-retail-omkar-cli-org-dev}"
REGION="${REGION:-us-central1}"
SOURCE_BUCKET="${SOURCE_BUCKET:-hackathon_hyderabad_swara}"
QUARANTINE_BUCKET="${QUARANTINE_BUCKET:-bankeva-quarantine-retail-omkar-cli-org-dev}"
BRONZE_DATASET="${BRONZE_DATASET:-hack_bronze}"
CONTROL_DATASET="${CONTROL_DATASET:-bankeva_control}"
TEMP_BUCKET="${TEMP_BUCKET:-$SOURCE_BUCKET}"
DAY="${1:-2}"
python ingestion_dataflow.py \
  --runner DataflowRunner \
  --day "$DAY" \
  --project "$PROJECT" \
  --region "$REGION" \
  --bucket "$SOURCE_BUCKET" \
  --quarantine_bucket "$QUARANTINE_BUCKET" \
  --bronze_dataset "$BRONZE_DATASET" \
  --control_dataset "$CONTROL_DATASET" \
  --temp_location "gs://$TEMP_BUCKET/dataflow/temp" \
  --staging_location "gs://$TEMP_BUCKET/dataflow/staging" \
  --job_name "bankeva-bronze-day${DAY}-$(date +%Y%m%d%H%M%S)" \
  --requirements_file requirements_dataflow.txt
