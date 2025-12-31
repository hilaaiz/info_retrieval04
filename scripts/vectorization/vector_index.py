from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any
from pathlib import Path
import re
import pickle

import numpy as np
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer
from scripts.common.models import Chunk


# =============================
# BM25 tokenization
# =============================
_TOKEN_RE = re.compile(r"[A-Za-z0-9']+")

def bm25_tokenize(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text)]


# =============================
# Vector Index (BM25 + Dense)
# =============================
class VectorIndex:
    """
    Holds ONLY vector representations:
    - BM25 index
    - Dense embedding matrix
    """

    def __init__(
        self,
        chunks: List[Chunk],
        dense_model_name: str = "intfloat/e5-base",
        device: str | None = None,
    ):
        self.chunks = chunks

        # ---------- BM25 ----------
        self.bm25_tokens = [bm25_tokenize(c.text) for c in chunks]
        self.bm25 = BM25Okapi(self.bm25_tokens)

        # ---------- Dense ----------
        self.dense_model = SentenceTransformer(dense_model_name, device=device)
        passages = [f"passage: {c.text}" for c in chunks]

        self.dense_matrix = self.dense_model.encode(
            passages,
            normalize_embeddings=True,
            show_progress_bar=True,
        ).astype(np.float32)

    # -----------------------------
    # Query encoders
    # -----------------------------
    def encode_query_dense(self, query: str) -> np.ndarray:
        return self.dense_model.encode(
            [f"query: {query}"],
            normalize_embeddings=True
        ).astype(np.float32)

    def bm25_scores(self, query: str) -> np.ndarray:
        tokens = bm25_tokenize(query)
        return np.array(self.bm25.get_scores(tokens), dtype=np.float32)

    # -----------------------------
    # Persistence
    # -----------------------------
    def save(self, path: str):
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @staticmethod
    def load(path: str) -> "VectorIndex":
        with open(path, "rb") as f:
            return pickle.load(f)


# =============================
# Chunk loader
# =============================
def load_chunks_from_dir(root_dir: str, chunking_name: str) -> List[Chunk]:
    chunks: List[Chunk] = []
    root = Path(root_dir)

    for doc_folder in sorted(root.iterdir()):
        if not doc_folder.is_dir():
            continue

        doc_id = doc_folder.name.replace("_chunks", "")

        for chunk_file in sorted(doc_folder.glob("*.txt")):
            text = chunk_file.read_text(encoding="utf-8").strip()
            if not text:
                continue

            chunks.append(
                Chunk(
                    chunk_id=chunk_file.stem,
                    doc_id=doc_id,
                    text=text,
                    meta={
                        "source_path": str(chunk_file),
                        "chunking": chunking_name,
                    },
                )
            )

    return chunks
