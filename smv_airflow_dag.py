"""BankEva: config-driven Python Bronze ingestion -> existing Dataproc scripts."""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from ingestion import run_ingestion, load_config

# Read configuration from the same DAG folder
CONFIG_URI = Path(__file__).resolve().parent / "config.yaml"

CONFIG = load_config(str(CONFIG_URI))


GCP = CONFIG["gcp"]
STORAGE = CONFIG["storage"]
BQ = CONFIG["bigquery"]
PIPELINE = CONFIG["pipeline"]
SCRIPTS = CONFIG["scripts"]
DAY = "{{ dag_run.conf.get('day', params.default_day) if dag_run and dag_run.conf else params.default_day }}"


def pyspark_job(script_name, args):
    return {
        "reference": {"project_id": GCP["project_id"]},
        "placement": {"cluster_name": CONFIG["dataproc"]["cluster_name"]},
        "pyspark_job": {
            "main_python_file_uri": STORAGE["dataproc_code_prefix"].rstrip("/") + "/" + script_name,
            "args": args,
        },
    }

common = ["--project", GCP["project_id"], "--region", GCP["region"],
          "--temp_bucket", STORAGE["dataproc_temp_bucket"]]

with DAG(
    dag_id="smv_dag_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": PIPELINE.get("retries", 1),
        "retry_delay": timedelta(minutes=PIPELINE.get("retry_delay_minutes", 5)),
    },
    params={"default_day": PIPELINE.get("default_day", 2)},
    tags=["bankeva", "python", "dataproc", "scd2"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")
    
    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_ingestion,
        op_kwargs={"day": DAY, "config_path": CONFIG_URI},
    )

    bronze_to_silver = DataprocSubmitJobOperator(
        task_id="bronze_to_silver",
        project_id=GCP["project_id"],
        region=GCP["region"],
        job=pyspark_job(SCRIPTS["bronze_to_silver"], common + [
            "--bronze_dataset", BQ["bronze_dataset"],
            "--silver_dataset", BQ["silver_dataset"],
        ]),
    )

    silver_to_gold = DataprocSubmitJobOperator(
        task_id="silver_to_gold_scd2",
        project_id=GCP["project_id"],
        region=GCP["region"],
        job=pyspark_job(SCRIPTS["silver_to_gold"], common + [
            "--silver_dataset", BQ["silver_dataset"],
            "--gold_dataset", BQ["gold_dataset"],
        ]),
    )

    start >> ingest_bronze >> bronze_to_silver >> silver_to_gold >> stop
