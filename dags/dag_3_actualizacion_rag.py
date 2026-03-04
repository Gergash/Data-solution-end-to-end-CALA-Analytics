"""
DAG 3: Actualización RAG (Knowledge Base + embeddings).
Chunking de /kb (archivo + fragmento) y refresh de vectores FAISS para POST /ask.
ExternalTaskSensor espera a que el DAG 1 haya terminado (execution_date_fn = Opción 1).
mode='reschedule', timeout=3600 (1 h). Tras timeout, re-ejecuta solo chunking_kb y refresh_embeddings.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.rag import chunking_kb, refresh_embeddings
from utils.github_commit import commit_razonamiento_json


def elegir_ejecucion_reciente(logical_date, **kwargs):
    """Opción 1: devuelve un único datetime para que el sensor busque esa ejecución del DAG 1 (la misma logical_date)."""
    return logical_date


with DAG(
    dag_id="dag_3_actualizacion_rag",
    schedule_interval=None,
    start_date=datetime(2026, 1, 2),
    catchup=False,
    tags=["rag", "kb", "faiss", "embeddings"],
) as dag:
    esperar_ingestion = ExternalTaskSensor(
        task_id="esperar_ingestion_transaccional",
        external_dag_id="dag_1_ingestion_transaccional",
        execution_date_fn=elegir_ejecucion_reciente,
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )
    chunking = PythonOperator(
        task_id="chunking_kb",
        python_callable=chunking_kb,
    )
    embeddings = PythonOperator(
        task_id="refresh_embeddings",
        python_callable=refresh_embeddings,
    )
    commit_razonamiento = PythonOperator(
        task_id="commit_razonamiento_json",
        python_callable=commit_razonamiento_json,
    )
    esperar_ingestion >> chunking >> embeddings >> commit_razonamiento
