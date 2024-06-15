up:
  docker compose up -d

init:
  mkdir -p ./dags ./logs ./plugins ./config
  echo -e "AIRFLOW_UID=$(id -u)" > .env  

bash:
  docker compose run --rm airflow-cli bash

down:
  docker compose down
