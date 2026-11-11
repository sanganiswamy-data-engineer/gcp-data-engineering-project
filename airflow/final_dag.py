
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from google.cloud import storage


# STEP 1: Run existing ingestion script from GCS
def run_ingest():
    client = storage.Client()
    bucket = client.bucket("hack_1")
    blob = bucket.blob("ingest.py")

    script = blob.download_as_text()

    exec(
        compile(script, "ingest.py", "exec"),
        {"__name__": "__main__", "__file__": "ingest.py"}
    )


# STEP 2: Combined Airflow DAG
with DAG(
    dag_id="use-case",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "dataproc", "bronze", "silver", "gold", "scd2"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    ingest = PythonOperator(
        task_id="run_ingest",
        python_callable=run_ingest
    )

    # STEP 3: Bronze to Silver PySpark
    submit_job = DataprocSubmitJobOperator(
        task_id="submit_bronze_to_silver",
        project_id="retail-omkar-cli-org-dev",
        region="us-central1",
        job={
            "placement": {
                "cluster_name": "cluster-silver-test"
            },
            "pyspark_job": {
                "main_python_file_uri":
                    "gs://hack_1/scripts/scripts_bronze_silver_updated_with_dataset.py"
            }
        }
    )

    # STEP 4: Silver to Gold SCD Type 2 PySpark
    silver_to_gold = DataprocSubmitJobOperator(
        task_id="submit_silver_to_gold_scd2",
        project_id="retail-omkar-cli-org-dev",
        region="us-central1",
        job={
            "placement": {
                "cluster_name": "cluster-silver-test"
            },
            "pyspark_job": {
                "main_python_file_uri":
                    "gs://hack_1/scripts/silver_to_gold_all_10_scd2_cleanup.py"
            }
        }
    )

    # STEP 5: End
    end = EmptyOperator(
        task_id="end"
    )

    # STEP 6: Task dependencies
    start >> ingest >> submit_job >> silver_to_gold >> end
