"""
API FastAPI para CALA Analytics.
- POST /ask: motor RAG (FAISS + citas archivo/fragmento).
- GET /kpis: analítica transaccional (vista BigQuery recurrencia 30d).
- Documentación automática en http://localhost:8000/docs
- Middleware de latencia para mantener el sistema optimizado.
"""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Rutas: mismo layout que en Docker (raíz del proyecto = logs/ al lado)
PROJECT_ROOT = Path(__file__).resolve().parent
LOGS_DIR = PROJECT_ROOT / "logs"
EMBEDDINGS_DIR = LOGS_DIR / "embeddings"
FAISS_INDEX_PATH = EMBEDDINGS_DIR / "faiss.index"
METADATA_PATH = EMBEDDINGS_DIR / "metadata.json"

# Artefactos cargados al inicio (DAG 2 y DAG 3)
faiss_index = None
metadata_chunks: list[dict] = []
embedding_model = None

# Número de fragmentos a devolver por pregunta RAG
TOP_K_RAG = 5


def _load_artefacts() -> None:
    """Carga índice FAISS, metadata y modelo de embeddings (startup)."""
    global faiss_index, metadata_chunks, embedding_model
    if not FAISS_INDEX_PATH.exists() or not METADATA_PATH.exists():
        raise FileNotFoundError(
            f"Artefactos no encontrados. Ejecuta DAG 3 (chunking_kb + refresh_embeddings). "
            f"Esperados: {FAISS_INDEX_PATH}, {METADATA_PATH}"
        )
    import faiss
    import json
    from sentence_transformers import SentenceTransformer

    faiss_index = faiss.read_index(str(FAISS_INDEX_PATH))
    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        metadata_chunks = json.load(f)
    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Carga de artefactos al iniciar y mensaje al cerrar."""
    try:
        _load_artefacts()
        print("[OK] FAISS, metadata y modelo de embeddings cargados.")
    except FileNotFoundError as e:
        print(f"[WARN] {e}. POST /ask fallará hasta que ejecutes el DAG 3.")
    except Exception as e:
        print(f"[WARN] Error cargando artefactos: {e}")
    yield
    print("Shutdown: liberando recursos.")


app = FastAPI(
    title="CALA Analytics API",
    description="RAG (POST /ask) y KPIs transaccionales (GET /kpis). Documentación en /docs.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----- Middleware de latencia -----
@app.middleware("http")
async def measure_latency(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"
    return response


# ----- Pydantic: validación de datos -----
class AskRequest(BaseModel):
    """Cuerpo del POST /ask. Validado con Pydantic."""

    pregunta: str = Field(..., min_length=1, max_length=2000, description="Pregunta en texto plano para el RAG")


class Cita(BaseModel):
    archivo: str
    fragmento: int


class AskResponse(BaseModel):
    respuesta: str = Field(..., description="Texto de los fragmentos más relevantes")
    citas: list[Cita] = Field(..., description="Archivo Markdown y número de fragmento para auditoría")


# ----- POST /ask (Motor RAG) -----
@app.post("/ask", response_model=AskResponse)
async def ask(request: AskRequest):
    """
    Convierte la pregunta a vector, busca los fragmentos más similares en FAISS
    y devuelve la respuesta con citas (archivo + fragmento) para auditoría.
    """
    if faiss_index is None or not metadata_chunks or embedding_model is None:
        raise HTTPException(
            status_code=503,
            detail="Artefactos RAG no cargados. Ejecuta el DAG 3 (chunking_kb + refresh_embeddings) y reinicia la API.",
        )

    query_embedding = embedding_model.encode([request.pregunta], convert_to_numpy=True).astype("float32")
    distances, indices = faiss_index.search(query_embedding, min(TOP_K_RAG, len(metadata_chunks)))
    # indices[0] puede contener -1 si hay menos de TOP_K vectores
    selected = []
    for idx in indices[0]:
        if 0 <= idx < len(metadata_chunks):
            selected.append(metadata_chunks[idx])
    if not selected:
        return AskResponse(respuesta="No se encontraron fragmentos relevantes.", citas=[])
    respuesta_texto = "\n\n".join(c["texto"] for c in selected)
    citas = [Cita(archivo=c["archivo"], fragmento=c["fragmento"]) for c in selected]
    return AskResponse(respuesta=respuesta_texto, citas=citas)


# ----- GET /kpis (Analítica Transaccional) -----
@app.get("/kpis")
async def kpis():
    """
    Consulta la vista v_kpi_recurrencia_30d en BigQuery (generada por DAG 1)
    y devuelve los datos de recurrencia en formato JSON para dashboards.
    """
    project_id = os.environ.get("BQ_PROJECT_ID") or os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "cala_analytics")
    if not project_id:
        raise HTTPException(
            status_code=503,
            detail="BQ_PROJECT_ID o GCP_PROJECT_ID no definido. Configura las variables de entorno.",
        )
    try:
        from google.cloud import bigquery
    except ImportError:
        raise HTTPException(status_code=503, detail="google-cloud-bigquery no instalado.")

    client = bigquery.Client(project=project_id)
    view_id = f"{project_id}.{dataset_id}.v_kpi_recurrencia_30d"
    query = f"SELECT fecha_proceso, clientes_recurrentes_30d FROM `{view_id}` ORDER BY fecha_proceso"
    try:
        rows = list(client.query(query).result())
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error al consultar BigQuery (¿ejecutaste el DAG 1?): {e!s}",
        )
    data = [
        {"fecha_proceso": str(row.fecha_proceso), "clientes_recurrentes_30d": row.clientes_recurrentes_30d}
        for row in rows
    ]
    return {"kpis": data, "vista": view_id}


@app.get("/health")
async def health():
    """Comprueba que la API y los artefactos estan disponibles."""
    rag_ok = faiss_index is not None and metadata_chunks and embedding_model is not None
    return {"status": "ok", "rag_cargado": rag_ok}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
