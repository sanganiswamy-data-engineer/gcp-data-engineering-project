pip3 install --user pyyaml google-cloud-storage google-cloud-bigquery

ls -l config.yaml ingestion.py

python3 ingestion.py 0 --config config.yaml


echo "$PROJECT_ID"
echo "$BRONZE"
echo "$SILVER"
echo "$GOLD"
echo "$CLUSTER"

gcloud dataproc jobs submit pyspark \
  "${SCRIPT_PREFIX}/${BRONZE_SCRIPT}" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --project "$PROJECT_ID" \
  --bronze_dataset "$BRONZE" \
  --silver_dataset "$SILVER" \
  --region "$REGION" \
  --temp_bucket "$TEMP_BUCKET"


  gcloud dataproc jobs submit pyspark \
  "${SCRIPT_PREFIX}/${GOLD_SCRIPT}" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --project "$PROJECT_ID" \
  --silver_dataset "$SILVER" \
  --gold_dataset "$GOLD" \
  --region "$REGION" \
  --temp_bucket "$TEMP_BUCKET"

