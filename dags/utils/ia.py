"""Funciones para DAG 2: preprocesamiento IA (PyTorch, tensores, vocabulario)."""
from __future__ import annotations

import os

import pandas as pd

from utils import ARTEFACTOS_DIR, PATH_ATENCIONES, PATH_CLIENTES, limpiar_y_estandarizar


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
    torch.save(
        {"train_indices": train_indices, "val_indices": val_indices},
        os.path.join(ARTEFACTOS_DIR, "indices.pt"),
    )
    print(f"[OK] IA: train={len(train_indices)}, val={len(val_indices)}, vocab_size={len(vocab)}")
