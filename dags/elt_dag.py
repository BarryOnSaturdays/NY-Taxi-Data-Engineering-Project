import io
import os
import requests
import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# extract data from this url
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# GCS bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "your-bucketname")

# upload file to GCS
def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# extract data & load into data lake
def web_to_gcs(year, service):
    for i in range(1):
        
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

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip')
        # file_name = file_name.replace('.csv.gz', '.parquet')
        # df.to_parquet(file_name, engine='pyarrow')
        # print(f"Parquet: {file_name}")

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
    dag_id = "steam_store_ingestion_dag",
    schedule_interval = "@weekly",
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ['steam-de'],
) as dag:

    # extract data & load into data lake
    extract_and_load = PythonOperator(
        task_id = "extract_and_load",
        python_callable = web_to_gcs,
        op_kwargs = {
            "year": '2019',
            "service": 'green' 
        }
    )

# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')

    # staging
    hook = GCSHook()
    bq_parallel_tasks = list()
    gcs_objs_list = hook.list(bucket_name = BUCKET_STORE, prefix = "proc")
    for obj in gcs_objs_list: 
        TABLE_NAME = obj.split("/")[-2].replace('.parquet', '')
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_external_table_{TABLE_NAME}_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": TABLE_NAME,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET_STORE}/{obj}"]
                },
            }
        )
        bq_parallel_tasks.append(bigquery_external_table_task)

    # transform data
    run_dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command=f'cd {DBT_DIR} && dbt run --profile airflow',
    trigger_rule="all_success"
    )

    extract_and_load >> run_dbt_task
