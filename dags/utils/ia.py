"""Funciones para DAG 2: preprocesamiento IA (tensores, vocabulario). Sin torchtext; tokenización simple."""
from __future__ import annotations

import os
import re

import pandas as pd

from utils import ARTEFACTOS_DIR, PATH_ATENCIONES, PATH_CLIENTES, limpiar_y_estandarizar


def _tokenize_basic(text: str) -> list[str]:
    """Tokenización tipo basic_english: minúsculas y palabras (sin torchtext)."""
    if not text or not str(text).strip():
        return []
    return re.findall(r"\b\w+\b", str(text).lower())


def _build_vocab_from_texts(textos: list[str], unk_token: str = "<unk>") -> dict:
    """
    Construye vocabulario word -> idx con <unk> en 0.
    Retorna dict con 'word_to_idx', 'unk_idx' para guardar y reutilizar.
    """
    word_to_idx = {unk_token: 0}
    for texto in textos:
        if pd.isna(texto):
            continue
        for t in _tokenize_basic(str(texto)):
            if t not in word_to_idx:
                word_to_idx[t] = len(word_to_idx)
    return {"word_to_idx": word_to_idx, "unk_idx": 0}


def procesar_datos_ia() -> None:
    """95%/5% split, tokenización, vocabulario <unk>, guarda .pt en logs (sin torchtext)."""
    import torch

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

    vocab = _build_vocab_from_texts(train_textos)
    word_to_idx = vocab["word_to_idx"]
    unk_idx = vocab["unk_idx"]

    def texto_a_indices(textos):
        return [
            torch.tensor(
                [word_to_idx.get(t, unk_idx) for t in _tokenize_basic(str(txt))],
                dtype=torch.long,
            )
            for txt in textos
        ]

    train_indices = texto_a_indices(train_textos)
    val_indices = texto_a_indices(val_textos)
    os.makedirs(ARTEFACTOS_DIR, exist_ok=True)
    torch.save(vocab, os.path.join(ARTEFACTOS_DIR, "vocab.pt"))
    torch.save(
        {"train_indices": train_indices, "val_indices": val_indices},
        os.path.join(ARTEFACTOS_DIR, "indices.pt"),
    )
    print(
        f"[OK] IA: train={len(train_indices)}, val={len(val_indices)}, vocab_size={len(word_to_idx)}"
    )
