import os
import requests
import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# extract data from this url
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

# your env variable
BUCKET = os.environ.get("GCP_GCS_BUCKET", "your-bucketname")
DATASET_NAME = "dbt_dataset"
DBT_DIR = "/home/airflow/dbt"
RUN_WITHOT_TEST = 'is_test_run: false'

# upload file to GCS
def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# extract data & load into data lake
def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # upload to GCS and remove local file 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")
        os.remove(file_name)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id = "gcs_bq_dbt_dag",
    schedule_interval = None,
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ['dbt-bq'],
) as dag:

    # extract data & load into data lake
    years = ['2019', '2020']
    services = ['green', 'yellow']
    for year in years:
        for service in services:
            extract_and_load = PythonOperator(
                task_id = f"load_table_{year}_{service}",
                python_callable = web_to_gcs,
                op_kwargs = {
                    "year": year,
                    "service": service
                }
            )

    # create external table
    create_external_table_query = """
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME}`
    OPTIONS (
    format = 'CSV',
    uris = ['DATA_SOURCE']
    );
    """

    create_external_table = BigQueryInsertJobOperator(
        task_id='create_external_table',
        configuration={
            "query": {
                "query": create_external_table_query,
                "useLegacySql": False
            }
        },
        gcp_conn_id='airflow-conn-id'
    )

    # transform data
    run_dbt_task = BashOperator(
        task_id='run_dbt',
        bash_command=f"cd {DBT_DIR} && dbt build --vars {RUN_WITHOT_TEST}",
        trigger_rule="all_success"
    )

    extract_and_load >> create_external_table >> run_dbt_task
