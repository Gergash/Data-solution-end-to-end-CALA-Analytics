"""
Utilidades compartidas por los DAGs.
Rutas y limpieza común; el resto en ingestion, ia y rag.
"""
from __future__ import annotations

import os

import pandas as pd

# ---------------------------------------------------------------------------
# Rutas (contenedor Docker). Fallback para ejecución local.
# ---------------------------------------------------------------------------
BASE_DATOS = os.environ.get("PIPELINE_DATOS_DIR", os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
PATH_ATENCIONES = os.path.join(BASE_DATOS, "atenciones.csv")
PATH_CLIENTES = os.path.join(BASE_DATOS, "clientes.csv")
# Metadata de la app: Dataset_Prueba_Tecnica_GCP_RAG/eventos_app.json
PATH_EVENTOS_APP = os.path.join(BASE_DATOS, "eventos_app.json")
PATH_KB = os.path.join(BASE_DATOS, "kb")
# En Docker: túnel directo a la base de conocimientos en /opt/airflow/kb
if os.path.isdir("/opt/airflow/kb"):
    PATH_KB = "/opt/airflow/kb"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
CLEANED_DIR = os.path.join(AIRFLOW_HOME, "logs", "cleaned")
ARTEFACTOS_DIR = os.path.join(AIRFLOW_HOME, "logs", "pipeline_ia_artefactos")
KB_CHUNKS_PATH = os.path.join(AIRFLOW_HOME, "logs", "kb_chunks.json")
EMBEDDINGS_DIR = os.path.join(AIRFLOW_HOME, "logs", "embeddings")
RAZONAMIENTO_JSON_PATH = os.path.join(AIRFLOW_HOME, "logs", "razonamiento.json")
METRICS_JSON_PATH = os.path.join(AIRFLOW_HOME, "logs", "metrics.json")

# Inicialización con fallback
if not os.path.isfile(PATH_ATENCIONES):
    _fallback = os.path.join(os.path.dirname(__file__), "..", "..", "Dataset_Prueba_Tecnica_GCP_RAG")
    if os.path.isfile(os.path.join(_fallback, "atenciones.csv")):
        BASE_DATOS = _fallback
        PATH_ATENCIONES = os.path.join(BASE_DATOS, "atenciones.csv")
        PATH_CLIENTES = os.path.join(BASE_DATOS, "clientes.csv")
        PATH_EVENTOS_APP = os.path.join(BASE_DATOS, "eventos_app.json")
        PATH_KB = os.path.join(BASE_DATOS, "kb")


def limpiar_y_estandarizar(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina nulos y estandariza columnas de texto a MAYÚSCULAS."""
    df = df.dropna()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.upper()
    return df
