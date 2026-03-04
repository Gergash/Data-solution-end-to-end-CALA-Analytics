"""
DAG 2: Preprocesamiento IA (PyTorch).
Lógica pesada: 95%/5% split, tokenización con <unk>, generación de tensores y vocabulario .pt.
Separado para monitorear uso de RAM de torch sin afectar ingestión ni RAG.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.ia import procesar_datos_ia

with DAG(
    dag_id="dag_2_preprocesamiento_ia",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ia", "pytorch", "preprocesamiento", "nlp"],
) as dag:
    procesar = PythonOperator(
        task_id="procesar_datos_ia",
        python_callable=procesar_datos_ia,
    )
