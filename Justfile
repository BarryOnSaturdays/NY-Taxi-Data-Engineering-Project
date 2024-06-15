up:
  docker compose up -d

init:
  cp .env.example .env
  sed -i "s/AIRFLOW_UID=1000/AIRFLOW_UID=$(id -u)/" .env  

bash:
  docker compose run --rm airflow-cli bash

down:
  docker compose down

restart: down up
  @echo 'done'