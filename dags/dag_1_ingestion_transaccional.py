"""
DAG 1: Ingestión transaccional (BigQuery).
Flujo exclusivo de ingeniería de datos: limpieza → carga particionada → KPI recurrencia 30d.
Si falla la conexión a la nube, el procesamiento local de IA (DAG 2) y RAG (DAG 3) no se ven afectados.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline_common import (
    cargar_bigquery_atenciones_clientes,
    ejecutar_sql_kpi_recurrencia,
    limpiar_datos_transaccionales,
)

with DAG(
    dag_id="dag_1_ingestion_transaccional",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bigquery", "ingestion", "kpis"],
) as dag:
    limpiar = PythonOperator(
        task_id="limpiar_y_estandarizar",
        python_callable=limpiar_datos_transaccionales,
    )
    cargar_bq = PythonOperator(
        task_id="cargar_bigquery_atenciones_clientes",
        python_callable=cargar_bigquery_atenciones_clientes,
    )
    sql_kpi = PythonOperator(
        task_id="ejecutar_sql_kpi_recurrencia",
        python_callable=ejecutar_sql_kpi_recurrencia,
    )
    limpiar >> cargar_bq >> sql_kpi
