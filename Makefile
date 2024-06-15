up: .env
	docker compose up -d

.env:
	cp .env.example .env
	sed -i "s/AIRFLOW_UID=1000/AIRFLOW_UID=$(shell id -u)/" .env
	sed -i "s/GCP_GCS_BUCKET=.*/GCP_GCS_BUCKET=$(GCP_GCS_BUCKET)/" .env

bash: .env
	docker compose run --rm airflow-cli bash

down:
	docker compose down

.PHONY: up bash down