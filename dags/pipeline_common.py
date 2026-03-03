"""
Código compartido entre los DAGs de ingestión, preprocesamiento IA y RAG.
Rutas, limpieza y funciones reutilizables. Los artefactos se escriben en
/opt/airflow/logs/ (shared volumes en Docker) para que la API FastAPI los use.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Rutas (contenedor Docker). Fallback para ejecución local.
# ---------------------------------------------------------------------------
BASE_DATOS = os.environ.get("PIPELINE_DATOS_DIR", os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
PATH_ATENCIONES = os.path.join(BASE_DATOS, "atenciones.csv")
PATH_CLIENTES = os.path.join(BASE_DATOS, "clientes.csv")
PATH_KB = os.path.join(BASE_DATOS, "kb")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
CLEANED_DIR = os.path.join(AIRFLOW_HOME, "logs", "cleaned")
ARTEFACTOS_DIR = os.path.join(AIRFLOW_HOME, "logs", "pipeline_ia_artefactos")
KB_CHUNKS_PATH = os.path.join(AIRFLOW_HOME, "logs", "kb_chunks.json")
EMBEDDINGS_DIR = os.path.join(AIRFLOW_HOME, "logs", "embeddings")

# Inicialización con fallback
if not os.path.isfile(PATH_ATENCIONES):
    _fallback = os.path.join(os.path.dirname(__file__), "..", "Dataset_Prueba_Tecnica_GCP_RAG")
    if os.path.isfile(os.path.join(_fallback, "atenciones.csv")):
        BASE_DATOS = _fallback
        PATH_ATENCIONES = os.path.join(BASE_DATOS, "atenciones.csv")
        PATH_CLIENTES = os.path.join(BASE_DATOS, "clientes.csv")
        PATH_KB = os.path.join(BASE_DATOS, "kb")


def limpiar_y_estandarizar(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina nulos y estandariza columnas de texto a MAYÚSCULAS."""
    df = df.dropna()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.upper()
    return df


# ---------- Ingestión transaccional (DAG 1): solo limpieza y guardado para BQ ----------
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
    client.load_table_from_dataframe(df_cl, table_clientes, job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)).result()
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


# ---------- Preprocesamiento IA (DAG 2): tensores y vocabulario .pt ----------
def _yield_tokens(textos, tokenizer):
    for texto in textos:
        if pd.isna(texto) or not str(texto).strip():
            continue
        yield tokenizer(str(texto))


def procesar_datos_ia() -> None:
    """Lógica pesada PyTorch: 95%/5% split, tokenización, vocabulario <unk>, guarda .pt en logs."""
    import torch
    from torchtext.data.utils import get_tokenizer
    from torchtext.vocab import build_vocab_from_iterator

    atenciones = pd.read_csv(PATH_ATENCIONES)
    clientes = pd.read_csv(PATH_CLIENTES)
    atenciones = limpiar_y_estandarizar(atenciones)
    clientes = limpiar_y_estandarizar(clientes)
    atenciones_merged = atenciones.merge(
        clientes, left_on="id_cliente", right_on="id_cliente", how="left", suffixes=("", "_cliente")
    )
    df = atenciones_merged.sample(frac=1, random_state=42).reset_index(drop=True)
    n = len(df)
    n_train = int(0.95 * n)
    train_df = df.iloc[:n_train]
    val_df = df.iloc[n_train:]
    col_texto = "json_detalle"
    if col_texto not in train_df.columns:
        raise ValueError(f"Columna '{col_texto}' no encontrada.")
    train_textos = train_df[col_texto].astype(str).tolist()
    val_textos = val_df[col_texto].astype(str).tolist()
    tokenizer = get_tokenizer("basic_english")
    vocab = build_vocab_from_iterator(
        _yield_tokens(train_textos, tokenizer),
        specials=["<unk>"],
        special_first=True,
        min_freq=1,
    )
    vocab.set_default_index(vocab["<unk>"])
    def texto_a_indices(textos):
        return [torch.tensor([vocab[t] for t in tokenizer(str(txt))], dtype=torch.long) for txt in textos]
    train_indices = texto_a_indices(train_textos)
    val_indices = texto_a_indices(val_textos)
    os.makedirs(ARTEFACTOS_DIR, exist_ok=True)
    torch.save(vocab, os.path.join(ARTEFACTOS_DIR, "vocab.pt"))
    torch.save({"train_indices": train_indices, "val_indices": val_indices}, os.path.join(ARTEFACTOS_DIR, "indices.pt"))
    print(f"[OK] IA: train={len(train_indices)}, val={len(val_indices)}, vocab_size={len(vocab)}")


# ---------- RAG (DAG 3): chunking y embeddings ----------
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50


def chunking_kb() -> None:
    """Lee /kb *.md, fragmentos de tamaño fijo, metadatos { archivo, fragmento, texto }."""
    if not os.path.isdir(PATH_KB):
        print(f"[SKIP] KB no encontrada: {PATH_KB}")
        return
    chunks = []
    for path in sorted(Path(PATH_KB).rglob("*.md")):
        archivo = path.name
        try:
            texto = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"[WARN] {path}: {e}")
            continue
        start = 0
        fragmento = 1
        while start < len(texto):
            end = start + CHUNK_SIZE
            trozo = texto[start:end].strip()
            if trozo:
                chunks.append({"archivo": archivo, "fragmento": fragmento, "texto": trozo})
                fragmento += 1
            start = end - CHUNK_OVERLAP if CHUNK_OVERLAP < CHUNK_SIZE else end
    os.makedirs(os.path.dirname(KB_CHUNKS_PATH), exist_ok=True)
    with open(KB_CHUNKS_PATH, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)
    print(f"[OK] KB: {len(chunks)} fragmentos -> {KB_CHUNKS_PATH}")


def refresh_embeddings() -> None:
    """Genera vectores (sentence-transformers) y guarda FAISS + metadata para POST /ask."""
    if not os.path.isfile(KB_CHUNKS_PATH):
        print(f"[SKIP] No existe {KB_CHUNKS_PATH}; ejecutar chunking_kb antes.")
        return
    try:
        from sentence_transformers import SentenceTransformer
        import faiss
    except ImportError as e:
        print(f"[SKIP] sentence_transformers o faiss: {e}")
        return
    with open(KB_CHUNKS_PATH, "r", encoding="utf-8") as f:
        chunks = json.load(f)
    if not chunks:
        print("[SKIP] Chunks vacíos.")
        return
    textos = [c["texto"] for c in chunks]
    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = model.encode(textos, convert_to_numpy=True)
    os.makedirs(EMBEDDINGS_DIR, exist_ok=True)
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings.astype("float32"))
    faiss.write_index(index, os.path.join(EMBEDDINGS_DIR, "faiss.index"))
    metadata_path = os.path.join(EMBEDDINGS_DIR, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump([{"archivo": c["archivo"], "fragmento": c["fragmento"], "texto": c["texto"]} for c in chunks], f, ensure_ascii=False, indent=2)
    print(f"[OK] Embeddings: {len(chunks)} vectores -> FAISS + {metadata_path}")
