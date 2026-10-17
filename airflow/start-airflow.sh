#!/bin/bash
set -e

# Roda tudo como usu  rio 'airflow', com o venv ativado no PATH.
# O '-i' + '-H' garante ambiente de login limpo e HOME correto do airflow.
# Ativar o venv coloca /home/airflow/venv/bin no PATH, para que os
# subprocessos do 'standalone' encontrem o comando 'airflow'.
sudo -u airflow -H bash -lc '
  export AIRFLOW_HOME=/home/airflow/airflow
  source /home/airflow/venv/bin/activate
  airflow standalone
'

