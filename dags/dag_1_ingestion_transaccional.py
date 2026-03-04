"""
DAG 1: Ingestión transaccional (BigQuery).
Flujo: limpieza → DDL (tablas desde scripts_sql) → carga → KPIs desde SQL externos.
Todo el SQL está en /scripts_sql/ como plantillas Jinja; sin SQL embebido en Python.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from utils.ingestion import (
    cargar_bigquery_atenciones_clientes,
    cargar_bigquery_eventos_app,
    limpiar_datos_transaccionales,
)

# Búsqueda de plantillas: Airflow resuelve {% include '...' %} desde esta ruta (Docker: /opt/airflow/scripts_sql)
TEMPLATE_SEARCHPATH = ["/opt/airflow/scripts_sql"]

# Parámetros inyectados en todos los .sql (bq_project, bq_dataset)
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
    template_searchpath=TEMPLATE_SEARCHPATH,
    default_params=DEFAULT_PARAMS,
) as dag:
    limpiar = PythonOperator(
        task_id="limpiar_y_estandarizar",
        python_callable=limpiar_datos_transaccionales,
    )
    # Creación de tablas (DDL externos)
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
    crear_tabla_eventos_app = BigQueryInsertJobOperator(
        task_id="crear_tabla_eventos_app",
        project_id="{{ params.bq_project }}",
        configuration={
            "query": {
                "query": "{% include 'ddl/create_table_eventos_app.sql' %}",
                "useLegacySql": False,
            }
        },
    )
    cargar_bq = PythonOperator(
        task_id="cargar_bigquery_atenciones_clientes",
        python_callable=cargar_bigquery_atenciones_clientes,
    )
    cargar_eventos = PythonOperator(
        task_id="cargar_bigquery_eventos_app",
        python_callable=cargar_bigquery_eventos_app,
    )
    # KPIs: vistas desde archivos SQL externos
    ejecutar_kpi_valor_segmento = BigQueryInsertJobOperator(
        task_id="ejecutar_kpi_valor_por_segmento",
        project_id="{{ params.bq_project }}",
        configuration={
            "query": {
                "query": "{% include 'queries/kpi_valor_por_segmento.sql' %}",
                "useLegacySql": False,
            }
        },
    )
    ejecutar_kpi_recurrencia = BigQueryInsertJobOperator(
        task_id="ejecutar_kpi_recurrencia_30d",
        project_id="{{ params.bq_project }}",
        configuration={
            "query": {
                "query": "{% include 'queries/kpi_recurrencia_30d.sql' %}",
                "useLegacySql": False,
            }
        },
    )
    ejecutar_kpi_rendimiento_plataforma = BigQueryInsertJobOperator(
        task_id="ejecutar_kpi_rendimiento_plataforma",
        project_id="{{ params.bq_project }}",
        configuration={
            "query": {
                "query": "{% include 'queries/kpi_rendimiento_plataforma.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # Orden lógico: Crear tablas → Cargar datos (atenciones+clientes + eventos_app) → Ejecutar consultas KPI
    limpiar >> [crear_tabla_atenciones, crear_tabla_clientes, crear_tabla_eventos_app] >> [cargar_bq, cargar_eventos] >> [
        ejecutar_kpi_valor_segmento,
        ejecutar_kpi_recurrencia,
        ejecutar_kpi_rendimiento_plataforma,
    ]
