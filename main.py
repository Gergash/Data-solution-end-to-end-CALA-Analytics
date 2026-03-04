"""
API FastAPI para CALA Analytics.
- POST /ask: motor RAG (FAISS + citas archivo/fragmento).
- GET /kpis: analítica transaccional (vista BigQuery recurrencia 30d).
- Documentación automática en http://localhost:8000/docs
- Middleware de latencia, lazy loading RAG, umbral de distancia, cache KPIs y auditoría /ask.
"""
from __future__ import annotations

import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# Rutas relativas a la raíz que Docker ve (/app): así el código encuentra los archivos en cualquier contenedor
PROJECT_ROOT = Path(__file__).resolve().parent
LOGS_DIR = PROJECT_ROOT / "logs"
EMBEDDINGS_DIR = LOGS_DIR / "embeddings"  # Carpeta donde el DAG 3 escribe faiss.index y metadata.json
FAISS_INDEX_PATH = EMBEDDINGS_DIR / "faiss.index"
METADATA_PATH = EMBEDDINGS_DIR / "metadata.json"
AUDIT_LOG_PATH = LOGS_DIR / "ask_audit.log"
RAZONAMIENTO_JSON_PATH = LOGS_DIR / "razonamiento.json"
METRICS_JSON_PATH = LOGS_DIR / "metrics.json"

# Artefactos RAG (startup o lazy load en la primera llamada a /ask)
faiss_index = None
metadata_chunks: list[dict] = []
embedding_model = None
_rag_lazy_retry_done = False  # Una sola reintento en /ask si al inicio no estaban los archivos

# RAG: número de fragmentos y umbral de distancia (L2). Por encima = "no encontré información precisa"
TOP_K_RAG = 5
RAG_DISTANCE_THRESHOLD = 1.8  # Ajustar según calidad del índice; mayor = más permisivo

# Cache KPIs: TTL 1 hora (los datos se actualizan con el DAG 1 diario)
KPIS_CACHE_TTL_SEC = 3600
_kpis_cache: dict | None = None
_kpis_cache_time: float = 0


def _load_artefacts() -> bool:
    """
    Carga índice FAISS, metadata y modelo de embeddings.
    Retorna True si se cargaron correctamente, False en caso contrario (no lanza).
    """
    global faiss_index, metadata_chunks, embedding_model
    if not FAISS_INDEX_PATH.exists() or not METADATA_PATH.exists():
        return False
    try:
        import faiss
        from sentence_transformers import SentenceTransformer

        faiss_index = faiss.read_index(str(FAISS_INDEX_PATH))
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            metadata_chunks = json.load(f)
        embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        return True
    except Exception:
        return False


def _ensure_rag_loaded() -> bool:
    """
    Lazy loading: si los artefactos no estaban cargados, intenta cargarlos una vez más
    (por si el DAG 3 terminó después de arrancar la API). Retorna True si RAG está disponible.
    """
    global faiss_index, metadata_chunks, embedding_model, _rag_lazy_retry_done
    if faiss_index is not None and metadata_chunks and embedding_model is not None:
        return True
    if not _rag_lazy_retry_done and FAISS_INDEX_PATH.exists() and METADATA_PATH.exists():
        _rag_lazy_retry_done = True
        _load_artefacts()
    return faiss_index is not None and bool(metadata_chunks) and embedding_model is not None


def _write_ask_audit(pregunta: str, citas: list[dict], respuesta_preview: str) -> None:
    """Registro de auditoría: pregunta, citas devueltas y vista previa de respuesta (para análisis y ajuste del chunking)."""
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            record = {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "pregunta": pregunta[:500],
                "citas": [{"archivo": c["archivo"], "fragmento": c["fragmento"]} for c in citas],
                "respuesta_preview": respuesta_preview[:300] if respuesta_preview else "",
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass  # No fallar la petición por un error de log


def _write_razonamiento_grafo(pregunta: str, citas: list[dict]) -> None:
    """Actualiza razonamiento.json con nodos (pregunta + documentos) y enlaces para el grafo ECharts del dashboard."""
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        query_id = "query_1"
        query_label = (pregunta[:40] + "…") if len(pregunta) > 40 else pregunta
        nodes = [
            {"id": query_id, "name": query_label, "symbolSize": 50, "category": 0},
        ]
        seen = set()
        for c in citas:
            arch = c.get("archivo", "")
            if arch and arch not in seen:
                seen.add(arch)
                nodes.append({"id": arch, "name": arch, "symbolSize": 30, "category": 1})
        links = [{"source": query_id, "target": c.get("archivo", "")} for c in citas if c.get("archivo")]
        graph = {
            "nodes": nodes,
            "links": links,
            "categories": [{"name": "Pregunta"}, {"name": "Documento"}],
        }
        with open(RAZONAMIENTO_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(graph, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Intenta cargar artefactos al inicio; si fallan, la API arranca igual.
    /health indicará rag_cargado=false; /ask intentará cargar una vez más en la primera llamada (lazy loading).
    """
    EMBEDDINGS_DIR.mkdir(parents=True, exist_ok=True)  # Asegura que la carpeta exista físicamente
    if _load_artefacts():
        print("[OK] FAISS, metadata y modelo de embeddings cargados.")
    else:
        print("[WARN] Artefactos RAG no cargados al inicio. /health marcará rag_cargado=false. /ask intentará cargarlos en la primera llamada (lazy loading).")
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


def _write_metrics(latency_ms: float) -> None:
    """Persiste la última latencia en metrics.json para el gauge del dashboard."""
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_latency_ms": round(latency_ms, 2),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        with open(METRICS_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception:
        pass


# ----- Middleware de latencia -----
@app.middleware("http")
async def measure_latency(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"
    _write_metrics(duration_ms)
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
    y devuelve la respuesta con citas (archivo + fragmento). Si la distancia supera
    el umbral, responde que no encontró información precisa. Todas las consultas
    se registran en logs/ask_audit.log para auditoría.
    """
    if not _ensure_rag_loaded():
        raise HTTPException(
            status_code=503,
            detail="Artefactos RAG no disponibles. Ejecuta el DAG 3 (chunking_kb + refresh_embeddings). Si acabas de ejecutarlo, vuelve a llamar a /ask (lazy loading).",
        )

    query_embedding = embedding_model.encode([request.pregunta], convert_to_numpy=True).astype("float32")
    k = min(TOP_K_RAG, len(metadata_chunks))
    distances, indices = faiss_index.search(query_embedding, k)

    selected: list[dict] = []
    for i, idx in enumerate(indices[0]):
        if idx < 0 or idx >= len(metadata_chunks):
            continue
        dist = float(distances[0][i])
        if dist > RAG_DISTANCE_THRESHOLD:
            continue
        chunk = {**metadata_chunks[idx], "distance": dist}
        selected.append(chunk)

    if not selected:
        respuesta_texto = "No encontré información precisa en los manuales para tu pregunta."
        citas_model = []
        _write_ask_audit(request.pregunta, [], respuesta_texto)
        _write_razonamiento_grafo(request.pregunta, [])
        return AskResponse(respuesta=respuesta_texto, citas=citas_model)

    respuesta_texto = "\n\n".join(c["texto"] for c in selected)
    citas_model = [Cita(archivo=c["archivo"], fragmento=c["fragmento"]) for c in selected]
    citas_dict = [{"archivo": c["archivo"], "fragmento": c["fragmento"]} for c in selected]
    _write_ask_audit(request.pregunta, citas_dict, respuesta_texto)
    _write_razonamiento_grafo(request.pregunta, citas_dict)
    return AskResponse(respuesta=respuesta_texto, citas=citas_model)


# ----- GET /kpis (Analítica Transaccional, cache 1 h) -----
@app.get("/kpis")
async def kpis():
    """
    Consulta la vista v_kpi_recurrencia_30d en BigQuery (generada por DAG 1).
    Resultado cacheado 1 hora para reducir consultas y costes en GCP.
    """
    global _kpis_cache, _kpis_cache_time
    now = time.time()
    if _kpis_cache is not None and (now - _kpis_cache_time) < KPIS_CACHE_TTL_SEC:
        return _kpis_cache

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
    payload = {"kpis": data, "vista": view_id, "cached_at": now}
    _kpis_cache = payload
    _kpis_cache_time = now
    return payload


@app.get("/razonamiento")
async def razonamiento():
    """Devuelve el grafo (nodos, links, categories) para el dashboard ECharts. Se actualiza en cada POST /ask."""
    if not RAZONAMIENTO_JSON_PATH.exists():
        return {"nodes": [], "links": [], "categories": [{"name": "Pregunta"}, {"name": "Documento"}]}
    with open(RAZONAMIENTO_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/metrics")
async def metrics():
    """Devuelve la última latencia (ms) para el gauge del dashboard. Se actualiza en cada petición a la API."""
    if not METRICS_JSON_PATH.exists():
        return {"last_latency_ms": 0, "updated_at": None}
    with open(METRICS_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/dashboard", response_class=FileResponse)
async def dashboard():
    """Sirve el dashboard de grafo de razonamiento (ECharts)."""
    index_path = PROJECT_ROOT / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="index.html no encontrado.")
    return FileResponse(index_path)


@app.get("/health")
async def health():
    """Estado de la API y disponibilidad del RAG (tras startup o lazy load)."""
    rag_ok = _ensure_rag_loaded()
    return {"status": "ok", "rag_cargado": rag_ok}


if __name__ == "__main__":
    import uvicorn

    # host 0.0.0.0 para que el mundo exterior (navegador, otros hosts) pueda conectar al contenedor
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
