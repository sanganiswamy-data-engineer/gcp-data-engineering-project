from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from google.cloud import storage

PROJECT = 'retail-omkar-cli-org-dev'
REGION = 'us-central1'
SOURCE_BUCKET = 'hackathon_hyderabad_swara'
QUARANTINE_BUCKET = 'bankeva-quarantine-retail-omkar-cli-org-dev'
BRONZE = 'hack_bronze'
CONTROL = 'bankeva_control'
CODE_PREFIX = f'gs://{SOURCE_BUCKET}/airflow/code'

with DAG(
    dag_id='hackathon_pipeline',
    description='Run cumulative BankEva Bronze Beam ingestion in Dataflow',
    start_date=datetime(2026, 10, 22),
    schedule=None,  
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'dataengineering', 'retries': 1,
                  'retry_delay': timedelta(minutes=5)},
    tags=['bankeva', 'beam', 'dataflow', 'bronze'],
) as dag:
    start = EmptyOperator(task_id='start')

    # Select 0, 1, or 2 when manually triggering the DAG.
    run_bronze = BeamRunPythonPipelineOperator(
        task_id='run_bronze_dataflow',
        py_file=f'{CODE_PREFIX}/ingestion_dataflow.py',
        py_requirements=None,
        #py_requirements=["apache-beam[gcp]==2.77.0", "google-cloud-bigquery", "google-cloud-storage"],
        py_interpreter='python3',
        runner='DataflowRunner',
        pipeline_options={
            'project': PROJECT,
            'region': REGION,
            'runner': 'DataflowRunner',
            #'day': '2',
            #'day': '{{ dag_run.conf.get("day", 2) if dag_run and dag_run.conf else 2 }}',
            "day": '{{ dag_run.conf.get("day", params.default_day) if dag_run and dag_run.conf else params.default_day }}',
            'bucket': SOURCE_BUCKET,
            'quarantine_bucket': QUARANTINE_BUCKET,
            'bronze_dataset': BRONZE,
            'control_dataset': CONTROL,
            'temp_location': f'gs://{SOURCE_BUCKET}/dataflow/temp',
            'staging_location': f'gs://{SOURCE_BUCKET}/dataflow/staging',
            'job_name': 'bankeva-bronze-{{ ts_nodash | lower }}-{{ ti.try_number }}',
            'requirements_file': f'{CODE_PREFIX}/requirements_dataflow.txt',
        },
        dataflow_config=DataflowConfiguration(
            location=REGION,
            project_id=PROJECT,
            wait_until_finished=True,
        ),
    )

    check_audit = BigQueryCheckOperator(
        task_id='check_file_audit',
        sql=f'''SELECT COUNT(*) = (CAST('{{{{ dag_run.conf.get("day", 2) if dag_run and dag_run.conf else 2 }}}}' AS INT64) + 1)
                 AND COUNTIF(status != 'SUCCESS') = 0
                 FROM `{PROJECT}.{CONTROL}.file_audit`''',
        use_legacy_sql=False,
        location=REGION,
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
    end = EmptyOperator(task_id='end')
    start >> run_bronze >> check_audit >> submit_job >> silver_to_gold >> end
