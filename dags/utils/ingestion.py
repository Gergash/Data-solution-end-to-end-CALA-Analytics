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
    """Carga atenciones y clientes desde logs/cleaned/ a BigQuery. Tipos alineados con DDL (FLOAT64, INT64, JSON)."""
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

    # Tipos alineados con DDL: valor_facturado FLOAT64, fechas TIMESTAMP
    if "fecha_atencion" in df_at.columns:
        df_at["fecha_atencion"] = pd.to_datetime(df_at["fecha_atencion"])
    if "fecha_proceso" in df_at.columns:
        df_at["fecha_proceso"] = pd.to_datetime(df_at["fecha_proceso"])
    if "valor_facturado" in df_at.columns:
        df_at["valor_facturado"] = pd.to_numeric(df_at["valor_facturado"], errors="coerce").astype("float64")
    # json_detalle: BigQuery espera tipo JSON; el CSV viene como string (válido JSON)
    if "json_detalle" in df_at.columns:
        df_at["json_detalle"] = df_at["json_detalle"].astype(str).replace("nan", None)

    # Clientes: score_crediticio INT64
    if "score_crediticio" in df_cl.columns:
        df_cl["score_crediticio"] = pd.to_numeric(df_cl["score_crediticio"], errors="coerce").astype("Int64")
    if "fecha_registro" in df_cl.columns:
        df_cl["fecha_registro"] = pd.to_datetime(df_cl["fecha_registro"])

    # Schema atenciones: obligatorio para columna JSON (evita "Unable to determine type for field")
    schema_atenciones = [
        bigquery.SchemaField("id_atencion", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_cliente", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("documento_cliente", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fecha_atencion", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("fecha_proceso", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("valor_facturado", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("estado", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("codigo_cups", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("canal_ingreso", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("json_detalle", "JSON", mode="NULLABLE"),
    ]
    cols_at = [f.name for f in schema_atenciones]
    df_at = df_at.reindex(columns=[c for c in cols_at if c in df_at.columns])
    job_config_at = bigquery.LoadJobConfig(
        schema=schema_atenciones,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha_atencion",
        ),
    )
    client.load_table_from_dataframe(df_at, table_atenciones, job_config=job_config_at).result()
    client.load_table_from_dataframe(
        df_cl, table_clientes, job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    ).result()
    print(f"[OK] BigQuery: {table_atenciones} (partición fecha_atencion), {table_clientes}")


