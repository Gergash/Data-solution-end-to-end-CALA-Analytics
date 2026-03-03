# Imagen Airflow con soporte Google Cloud (BigQuery, GCS, etc.)
# Compatible con Cloud Composer para migración futura
FROM apache/airflow:2.10.4

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN chown airflow: /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER root

RUN rm /tmp/requirements.txt

USER airflow