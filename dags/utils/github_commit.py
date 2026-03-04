"""Commit automático de razonamiento.json y metrics.json al repositorio GitHub (PyGithub)."""
from __future__ import annotations

import os

from utils import METRICS_JSON_PATH, RAZONAMIENTO_JSON_PATH


def _commit_file(repo, path: str, content: str, msg_create: str, msg_update: str) -> bool:
    """Sube o actualiza un archivo en el repo. Retorna True si se hizo commit."""
    try:
        blob = repo.get_contents(path)
        repo.update_file(path, msg_update, content, blob.sha)
        print(f"[OK] Actualizado {path}")
        return True
    except Exception:
        repo.create_file(path, msg_create, content)
        print(f"[OK] Creado {path}")
        return True


def commit_razonamiento_json() -> None:
    """
    Lee logs/razonamiento.json (y opcionalmente logs/metrics.json) y hace commit al repo.
    Requiere: GITHUB_TOKEN, GITHUB_REPO (owner/repo). Si faltan, se omite sin error.
    """
    token = os.environ.get("GITHUB_TOKEN")
    repo_slug = os.environ.get("GITHUB_REPO", "").strip()
    if not token or not repo_slug:
        print("[SKIP] GITHUB_TOKEN o GITHUB_REPO no definidos; omitiendo commit.")
        return
    if not os.path.isfile(RAZONAMIENTO_JSON_PATH):
        print("[SKIP] No existe razonamiento.json; ejecutar al menos un POST /ask antes.")
        return
    try:
        from github import Github
    except ImportError:
        print("[SKIP] PyGithub no instalado; pip install PyGithub.")
        return
    try:
        with open(RAZONAMIENTO_JSON_PATH, "r", encoding="utf-8") as f:
            content_razonamiento = f.read()
    except Exception as e:
        print(f"[WARN] No se pudo leer razonamiento.json: {e}")
        return
    content_metrics = None
    if os.path.isfile(METRICS_JSON_PATH):
        try:
            with open(METRICS_JSON_PATH, "r", encoding="utf-8") as f:
                content_metrics = f.read()
        except Exception as e:
            print(f"[WARN] No se pudo leer metrics.json: {e}")
    try:
        g = Github(token)
        repo = g.get_repo(repo_slug)
        _commit_file(
            repo,
            "razonamiento.json",
            content_razonamiento,
            "chore: añadir grafo razonamiento (DAG 3)",
            "chore: actualizar grafo razonamiento (DAG 3)",
        )
        if content_metrics is not None:
            _commit_file(
                repo,
                "metrics.json",
                content_metrics,
                "chore: añadir métricas latencia (DAG 3)",
                "chore: actualizar métricas latencia (DAG 3)",
            )
    except Exception as e:
        print(f"[WARN] Error en commit a GitHub: {e}")
