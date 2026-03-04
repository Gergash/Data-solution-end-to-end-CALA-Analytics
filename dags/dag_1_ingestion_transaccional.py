"""
DAG 1: Ingestión transaccional (BigQuery).
Flujo: limpieza → DDL (tablas desde scripts_sql) → carga particionada → KPI recurrencia 30d.
Los DDL se ejecutan con BigQueryInsertJobOperator usando {% include %} sobre scripts_sql.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from utils.ingestion import (
    cargar_bigquery_atenciones_clientes,
    ejecutar_sql_kpi_recurrencia,
    limpiar_datos_transaccionales,
)

# Ruta donde Airflow busca plantillas SQL (mismo nivel que dags/ en el repo)
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
TEMPLATE_SEARCHPATH = os.path.join(AIRFLOW_HOME, "scripts_sql")

# Parámetros para los SQL (project/dataset); los DDL y queries usan {{ params.bq_project }} y {{ params.bq_dataset }}
DEFAULT_PARAMS = {
    "bq_project": os.environ.get("BQ_PROJECT_ID") or os.environ.get("GCP_PROJECT_ID") or "",
    "bq_dataset": os.environ.get("BQ_DATASET_ID", "cala_analytics"),
}

with DAG(
    dag_id="dag_1_ingestion_transaccional",
    schedule_interval=None,
    start_date=datetime(2026, 1, 2),
    catchup=False,
    tags=["bigquery", "ingestion", "kpis"],
    template_searchpath=[TEMPLATE_SEARCHPATH],
    default_params=DEFAULT_PARAMS,
) as dag:
    limpiar = PythonOperator(
        task_id="limpiar_y_estandarizar",
        python_callable=limpiar_datos_transaccionales,
    )
    # DDL desde archivos .sql (sin SQL embebido en Python)
    crear_tabla_atenciones = BigQueryInsertJobOperator(
        task_id="crear_tabla_atenciones",
        project_id="{{ params.bq_project }}",
        configuration={
            "query": {
                "query": "{% include 'ddl/create_table_atenciones.sql' %}",
                "useLegacySql": False,
            }
        },
    )
    crear_tabla_clientes = BigQueryInsertJobOperator(
        task_id="crear_tabla_clientes",
        project_id="{{ params.bq_project }}",
        configuration={
            "query": {
                "query": "{% include 'ddl/create_table_clientes.sql' %}",
                "useLegacySql": False,
            }
        },
    )
    cargar_bq = PythonOperator(
        task_id="cargar_bigquery_atenciones_clientes",
        python_callable=cargar_bigquery_atenciones_clientes,
    )
    sql_kpi = PythonOperator(
        task_id="ejecutar_sql_kpi_recurrencia",
        python_callable=ejecutar_sql_kpi_recurrencia,
    )
    limpiar >> [crear_tabla_atenciones, crear_tabla_clientes] >> cargar_bq >> sql_kpi
