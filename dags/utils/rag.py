"""Funciones para DAG 3: Knowledge Base (chunking) y embeddings FAISS."""
from __future__ import annotations

import json
import os
from pathlib import Path

from utils import EMBEDDINGS_DIR, KB_CHUNKS_PATH, PATH_KB

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
        json.dump(
            [{"archivo": c["archivo"], "fragmento": c["fragmento"], "texto": c["texto"]} for c in chunks],
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"[OK] Embeddings: {len(chunks)} vectores -> FAISS + {metadata_path}")
