#!/usr/bin/env bash
# Copia faiss.index y metadata.json desde el contenedor de Airflow a logs/embeddings/
# Ejecutar desde la raíz del proyecto con el contenedor en marcha: bash copy_embeddings_from_container.sh

set -e
EMBEDDINGS_DIR="logs/embeddings"
CONTAINER="${1:-cala-airflow-scheduler}"

mkdir -p "$EMBEDDINGS_DIR"

if docker exec "$CONTAINER" test -f /opt/airflow/logs/embeddings/faiss.index 2>/dev/null; then
  docker cp "$CONTAINER:/opt/airflow/logs/embeddings/faiss.index" "$EMBEDDINGS_DIR/"
  echo "[OK] Copiado faiss.index a $EMBEDDINGS_DIR/"
else
  echo "[SKIP] faiss.index no existe en el contenedor. Ejecuta el DAG 3 (chunking_kb + refresh_embeddings) antes."
fi

if docker exec "$CONTAINER" test -f /opt/airflow/logs/embeddings/metadata.json 2>/dev/null; then
  docker cp "$CONTAINER:/opt/airflow/logs/embeddings/metadata.json" "$EMBEDDINGS_DIR/"
  echo "[OK] Copiado metadata.json a $EMBEDDINGS_DIR/"
else
  echo "[SKIP] metadata.json no existe en el contenedor. Ejecuta el DAG 3 antes."
fi
