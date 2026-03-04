"""Funciones para DAG 1: ingestión transaccional y BigQuery."""
from __future__ import annotations

import os

import pandas as pd

from utils import CLEANED_DIR, PATH_ATENCIONES, PATH_CLIENTES, limpiar_y_estandarizar


def limpiar_datos_transaccionales() -> None:
    """Carga atenciones y clientes, aplica limpieza y guarda CSVs en logs/cleaned/ para BigQuery."""
    atenciones = pd.read_csv(PATH_ATENCIONES)
    clientes = pd.read_csv(PATH_CLIENTES)
    atenciones = limpiar_y_estandarizar(atenciones)
    clientes = limpiar_y_estandarizar(clientes)
    os.makedirs(CLEANED_DIR, exist_ok=True)
    if "fecha_proceso" in atenciones.columns:
        atenciones["fecha_proceso"] = pd.to_datetime(atenciones["fecha_proceso"]).dt.date
    atenciones.to_csv(os.path.join(CLEANED_DIR, "atenciones.csv"), index=False)
    clientes.to_csv(os.path.join(CLEANED_DIR, "clientes.csv"), index=False)
    print(f"[OK] Datos transaccionales limpios guardados en {CLEANED_DIR}")


def cargar_bigquery_atenciones_clientes() -> None:
    """Carga atenciones (partición fecha_proceso) y clientes a BigQuery."""
    try:
        from google.cloud import bigquery
    except ImportError:
        print("[SKIP] google-cloud-bigquery no disponible.")
        return
    project_id = os.environ.get("BQ_PROJECT_ID") or os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "cala_analytics")
    if not project_id:
        print("[SKIP] BQ_PROJECT_ID o GCP_PROJECT_ID no definido.")
        return
    client = bigquery.Client(project=project_id)
    table_atenciones = f"{project_id}.{dataset_id}.atenciones"
    table_clientes = f"{project_id}.{dataset_id}.clientes"
    path_at = os.path.join(CLEANED_DIR, "atenciones.csv")
    path_cl = os.path.join(CLEANED_DIR, "clientes.csv")
    if not os.path.isfile(path_at) or not os.path.isfile(path_cl):
        print("[SKIP] No se encontraron CSVs en logs/cleaned/.")
        return
    df_at = pd.read_csv(path_at)
    df_cl = pd.read_csv(path_cl)
    if "fecha_proceso" in df_at.columns:
        df_at["fecha_proceso"] = pd.to_datetime(df_at["fecha_proceso"])
    job_config_at = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha_proceso",
        ),
    )
    client.load_table_from_dataframe(df_at, table_atenciones, job_config=job_config_at).result()
    client.load_table_from_dataframe(
        df_cl, table_clientes, job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    ).result()
    print(f"[OK] BigQuery: {table_atenciones} (partición fecha_proceso), {table_clientes}")


def ejecutar_sql_kpi_recurrencia() -> None:
    """Crea vista v_kpi_recurrencia_30d para GET /kpis."""
    try:
        from google.cloud import bigquery
    except ImportError:
        print("[SKIP] google-cloud-bigquery no disponible.")
        return
    project_id = os.environ.get("BQ_PROJECT_ID") or os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "cala_analytics")
    if not project_id:
        print("[SKIP] BQ_PROJECT_ID no definido.")
        return
    client = bigquery.Client(project=project_id)
    view_id = f"{project_id}.{dataset_id}.v_kpi_recurrencia_30d"
    sql = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH ventana AS (
      SELECT id_cliente, fecha_proceso,
        COUNT(*) OVER (
          PARTITION BY id_cliente
          ORDER BY DATE(fecha_proceso)
          RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
        ) AS atenciones_30d
      FROM `{project_id}.{dataset_id}.atenciones`
      WHERE fecha_proceso IS NOT NULL
    )
    SELECT fecha_proceso, COUNT(DISTINCT id_cliente) AS clientes_recurrentes_30d
    FROM ventana WHERE atenciones_30d >= 2
    GROUP BY fecha_proceso ORDER BY fecha_proceso
    """
    client.query(sql).result()
    print(f"[OK] BigQuery: vista {view_id} creada.")
