# Image Airflow avec dépendances DAG / scraper (évite _PIP_ADDITIONAL_REQUIREMENTS au démarrage = UI plus fiable)
FROM apache/airflow:2.8.0

USER airflow
WORKDIR /opt/airflow

COPY requirements-docker.txt /opt/airflow/requirements-docker.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements-docker.txt
