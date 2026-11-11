
#!/usr/bin/env bash
set -Eeuo pipefail

# ==========================================
# BankEva Dataflow Ingestion Launcher
# Usage: ./run_dataflow.sh 0|1|2
# ==========================================

PROJECT="${PROJECT:-retail-omkar-cli-org-dev}"
REGION="${REGION:-us-central1}"

SOURCE_BUCKET="${SOURCE_BUCKET:-hackathon_hyderabad_swara}"
QUARANTINE_BUCKET="${QUARANTINE_BUCKET:-bankeva-quarantine-retail-omkar-cli-org-dev}"

BRONZE_DATASET="${BRONZE_DATASET:-bankeva_bronze_test}"
CONTROL_DATASET="${CONTROL_DATASET:-bankeva_control_test}"

TEMP_BUCKET="${TEMP_BUCKET:-$SOURCE_BUCKET}"

DAY="${1:-}"

if [[ ! "$DAY" =~ ^[012]$ ]]; then
    echo "ERROR: Specify day 0, 1 or 2."
    echo "Usage: ./run_dataflow.sh 0"
    exit 1
fi

cd "$(dirname "$0")"

# Validate local files
for file in \
    ingestion_dataflow_parameterized.py \
    requirements_dataflow.txt
do
    if [[ ! -f "$file" ]]; then
        echo "ERROR: Missing file: $file"
        exit 1
    fi
done

# Configure project
gcloud config set project "$PROJECT"

# Source file names
FILES=(
  "day0/hack149_ws01_Source_data_EvalDB_Day0_Interleaved.txt"
  "day1/hack149_ws01_Source_data_EvalDB_Day1_Interleaved.txt"
  "day2/hack149_ws01_Source_data_EvalDB_Day2_Interleaved.txt"
)

# Check selected source files
for ((i=0; i<=DAY; i++)); do
    SOURCE="gs://$SOURCE_BUCKET/${FILES[$i]}"
    echo "Checking: $SOURCE"
    gcloud storage ls "$SOURCE" >/dev/null
done

# Build input list for Python
INPUT_FILES_JSON=$(python3 - <<'PY'
import json
print(json.dumps([
 "day0/hack149_ws01_Source_data_EvalDB_Day0_Interleaved.txt",
 "day1/hack149_ws01_Source_data_EvalDB_Day1_Interleaved.txt",
 "day2/hack149_ws01_Source_data_EvalDB_Day2_Interleaved.txt"
]))
PY
)

JOB_NAME="bankeva-bronze-day${DAY}-$(date -u +%Y%m%d%H%M%S)"

echo "=========================================="
echo "Starting BankEva Dataflow Pipeline"
echo "Project : $PROJECT"
echo "Region  : $REGION"
echo "Day     : $DAY"
echo "Job     : $JOB_NAME"
echo "=========================================="

python3 ingestion_dataflow_parameterized.py \
  --runner DataflowRunner \
  --day "$DAY" \
  --project "$PROJECT" \
  --region "$REGION" \
  --bucket "$SOURCE_BUCKET" \
  --quarantine_bucket "$QUARANTINE_BUCKET" \
  --bronze_dataset "$BRONZE_DATASET" \
  --control_dataset "$CONTROL_DATASET" \
  --input_files_json "$INPUT_FILES_JSON" \
  --temp_location "gs://$TEMP_BUCKET/dataflow/temp" \
  --staging_location "gs://$TEMP_BUCKET/dataflow/staging" \
  --job_name "$JOB_NAME" \
  --requirements_file requirements_dataflow.txt

echo "Python Dataflow command returned."

echo "=========================================="
echo "Checking Dataflow job status"
echo "=========================================="

gcloud dataflow jobs list \
  --project="$PROJECT" \
  --region="$REGION" \
  --filter="name=$JOB_NAME" \
  --format="table(id,name,currentState)"

echo "Verify JOB_STATE_DONE before proceeding."
