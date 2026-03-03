"""
DAG 3: Actualización RAG (Knowledge Base + embeddings).
Chunking de /kb (archivo + fragmento) y refresh de vectores FAISS para POST /ask.
ExternalTaskSensor espera a que DAG 1 termine (mismo execution_date) para actuar
como sistema único. Para solo actualizar RAG (nuevos .md en /kb): dispara este DAG
y, tras timeout del sensor, re-ejecuta solo chunking_kb y refresh_embeddings.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from pipeline_common import chunking_kb, refresh_embeddings

SENSOR_TIMEOUT = 600

with DAG(
    dag_id="dag_3_actualizacion_rag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["rag", "kb", "faiss", "embeddings"],
) as dag:
    esperar_ingestion = ExternalTaskSensor(
        task_id="esperar_ingestion_transaccional",
        external_dag_id="dag_1_ingestion_transaccional",
        external_task_id="ejecutar_sql_kpi_recurrencia",
        timeout=SENSOR_TIMEOUT,
        poke_interval=30,
        mode="poke",
    )
    chunking = PythonOperator(
        task_id="chunking_kb",
        python_callable=chunking_kb,
    )
    embeddings = PythonOperator(
        task_id="refresh_embeddings",
        python_callable=refresh_embeddings,
    )
    esperar_ingestion >> chunking >> embeddings
