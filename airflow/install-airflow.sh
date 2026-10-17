#!/bin/bash
set -e

# Versões fixas (nada de "latest")
AIRFLOW_VERSION=3.0.2
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Usu  rio dedicado pro Airflow
id -u airflow &>/dev/null || useradd -m -s /bin/bash airflow

# Ambiente virtual em /home/airflow
python3 -m venv /home/airflow/venv
/home/airflow/venv/bin/pip install --upgrade pip

# Instala Airflow + driver do Postgres, respeitando o constraints
/home/airflow/venv/bin/pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
/home/airflow/venv/bin/pip install "pandas"

# AIRFLOW_HOME e conex  o com o Postgres
export AIRFLOW_HOME=/home/airflow/airflow
mkdir -p "$AIRFLOW_HOME"
chown -R airflow:airflow /home/airflow

echo "Instalação concluída."
