"""Parameterized BankEva Airflow DAG. Place config_bankeva.yaml beside this DAG."""
import json
from datetime import datetime, timedelta
from pathlib import Path
import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

CONFIG_PATH = Path(__file__).resolve().parent / "config_bankeva.yaml"
with CONFIG_PATH.open(encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

PROJECT = cfg["gcp"]["project_id"]
REGION = cfg["gcp"]["region"]
STORAGE = cfg["storage"]
BQ = cfg["bigquery"]
SCRIPTS = cfg["scripts"]
# Automatically process all configured days: 0, 1, 2...
DAY = str(len(cfg["pipeline"]["input_files"]) - 1)
#DEFAULT_DAY = cfg["pipeline"]["default_day"]
#DAY = '{{ dag_run.conf.get("day", params.default_day) if dag_run and dag_run.conf else params.default_day }}'

with DAG(
    dag_id="dataflow_dag1_param",
    start_date=datetime(2026, 10, 22),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "dataengineering",
        "retries": cfg["pipeline"]["retries"],
        "retry_delay": timedelta(minutes=cfg["pipeline"]["retry_delay_minutes"]),
    },
    tags=["bankeva", "beam", "dataflow", "dataproc", "scd2"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_bronze = BeamRunPythonPipelineOperator(
        task_id="run_bronze_dataflow",
        py_file=f'{STORAGE["code_prefix"]}/{SCRIPTS["beam_ingestion"]}',
        py_requirements=None,
        py_interpreter="python3",
        runner="DataflowRunner",
        pipeline_options={
            "project": PROJECT,
            "region": REGION,
            "day": DAY,
            "bucket": STORAGE["source_bucket"],
            "quarantine_bucket": STORAGE["quarantine_bucket"],
            "bronze_dataset": BQ["bronze_dataset"],
            "control_dataset": BQ["control_dataset"],
            "input_files_json": json.dumps(cfg["pipeline"]["input_files"]),
            "temp_location": STORAGE["dataflow_temp"],
            "staging_location": STORAGE["dataflow_staging"],
            "job_name": "bankeva-bronze-{{ ts_nodash | lower }}-{{ ti.try_number }}",
            "requirements_file": f'{STORAGE["code_prefix"]}/{SCRIPTS["beam_requirements"]}',
        },
    )

    check_audit = BigQueryCheckOperator(
        task_id="check_file_audit",
        sql=(f"SELECT COUNT(*) = SAFE_CAST('{DAY}' AS INT64) + 1 "
             f"AND COUNTIF(status != 'SUCCESS') = 0 "
             f"FROM `{PROJECT}.{BQ['control_dataset']}.file_audit`"),
        use_legacy_sql=False,
        location=REGION,
    )

    bronze_to_silver = DataprocSubmitJobOperator(
        task_id="submit_bronze_to_silver",
        project_id=PROJECT,
        region=REGION,
        job={
            "placement": {"cluster_name": cfg["dataproc"]["cluster_name"]},
            "pyspark_job": {
                "main_python_file_uri": f'{STORAGE["dataproc_code_prefix"]}/{SCRIPTS["bronze_to_silver"]}',
                "args": ["--project", PROJECT,
                         "--bronze_dataset", BQ["bronze_dataset"],
                         "--silver_dataset", BQ["silver_dataset"],
                         "--region", REGION,
                         "--temp_bucket", STORAGE["dataproc_temp_bucket"]],
            },
        },
    )

    silver_to_gold = DataprocSubmitJobOperator(
        task_id="submit_silver_to_gold_scd2",
        project_id=PROJECT,
        region=REGION,
        job={
            "placement": {"cluster_name": cfg["dataproc"]["cluster_name"]},
            "pyspark_job": {
                "main_python_file_uri": f'{STORAGE["dataproc_code_prefix"]}/{SCRIPTS["silver_to_gold"]}',
                "args": ["--project", PROJECT,
                         "--silver_dataset", BQ["silver_dataset"],
                         "--gold_dataset", BQ["gold_dataset"],
                         "--region", REGION,
                         "--temp_bucket", STORAGE["dataproc_temp_bucket"]],
            },
        },
    )
    end = EmptyOperator(task_id="end")
    start >> run_bronze >> check_audit >> bronze_to_silver >> silver_to_gold >> end
