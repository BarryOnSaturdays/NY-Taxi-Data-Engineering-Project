# NY Taxi Data Engineering Project
## Project Overview
This is an end-to-end data engineering project. 
This project builds an **ELT** data pipeline with modern data stack, including **DBT**, **Airflow**, **Docker**, **Google Cloud Storage**, **BigQuery**, and **Looker Studio**.
## Data Architecture
![alt text](architecture.png)
### Data Source
Download from [NYC TLC Data](https://github.com/DataTalksClub/nyc-tlc-data/?tab=readme-ov-file), which was copied from the [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
### Docker
- Follow airflow [official guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) to run airflow in docker

- Mount dbt and gcp credential files into docker
    - .dbt/profile.yml (fill in your dataset, credential, project name)
    - dbt (model, macros, seeds etc.)
    - gcp credential

- Docker file will install packages in requirements.txt
```
pandas
pyarrow
google-cloud-storage
dbt-bigquery
apache-airflow
apache-airflow-providers-google
```
### Airflow
Manage 3 tasks:
- `extract_and_load`
    - Using: PythonOperator
    - Doing: download csv files and upload to GCS (data lake)
- `create_external_table`
    - Using: BigQueryInsertJobOperator
    - Doing: ingest raw data into BigQuery by creating external table (data warehouse)
- `run_dbt_task`
    - Using: BashOperator
    - Doing: build DBT model
### Transform (DBT & BigQuery)
![alt text](dbt.png)
- models: staging, dimension table, fact table, datamart
- seeds: csv files that have infrequent updates (run `dbt seed` to create table)
- macros: like python function
- package: imported using packages.yml (run `dbt deps` to download packages)
- tests: run `dbt test --select <model_name>` to test model
    - severity: `warn` or `error`
    - test: `unique`, `not null`, `accepted values`, `foreign key`
### Visualizaton (Looker Studio)
![alt text](dashboard.png)
Quick finding:
- 90% of NY taxi trips in 2019 ~ 2020 were run by yellow taxi
- upper east side south/north and midtown center are top pickup zones
- The covid 19 pandemic has huge impact on NY taxi trips
## Improvement
- try data ingestion tool like airbyte or fivetran to replace python in EL part
- divide airflow tasks into more steps to make it easier for debugging
- use production profile to deploy dbt model
- add dbt documentation
## Credit
- This project is inspired by [data engineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
