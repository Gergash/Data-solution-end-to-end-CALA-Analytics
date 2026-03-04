"""
DAG 3: Actualización RAG (Knowledge Base + embeddings).
Chunking de /kb (archivo + fragmento) y refresh de vectores FAISS para POST /ask.
ExternalTaskSensor espera a que todo el DAG 1 termine (external_task_id=None)
con mode='reschedule' para no bloquear CPU en Docker; timeout=3600 (1 h).
Para solo actualizar RAG (nuevos .md en /kb): tras timeout, re-ejecuta chunking_kb y refresh_embeddings.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.rag import chunking_kb, refresh_embeddings

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
        external_task_id=None,
        check_existence=True,
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
    esperar_ingestion >> chunking >> embeddings
