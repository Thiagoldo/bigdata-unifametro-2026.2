FROM python:3.12-slim

COPY install-airflow.sh /home/airflow/
COPY dag_convenios.py /home/airflow/airflow/dags/
COPY dag_contratos.py /home/airflow/airflow/dags/

WORKDIR /home/airflow

RUN chmod +x install-airflow.sh && \
      ./install-airflow.sh

