# retriever.py
# Implements Stage 3: Temporal-aware retrieval (Hard filter + Soft decay)

# import sys
# from pathlib import Path

# PROJECT_ROOT = Path(__file__).resolve().parents[2]
# sys.path.append(str(PROJECT_ROOT))

import re
import numpy as np
from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity
from scripts.vectorization.vector_index import VectorIndex

# timestamp_iso

# Paths to pre-built vector indexes
INDEX_PATHS = {
    "fixed_660": "vector_indexes/fixed_660/vector_index.pkl",
    "hierarchical": "vector_indexes/hierarchical/vector_index.pkl",
}


def extract_year_from_query(query: str):
    """
    Extracts a year (e.g., 2024) from the query text.
    Used for hard temporal filtering.
    """
    m = re.search(r"\b(19|20)\d{2}\b", query)
    return int(m.group()) if m else None


def hard_time_filter(chunks, scores, year):
    """
    Hard filtering strategy:
    Keep only chunks whose timestamp year == requested year.
    Returns indices to keep.
    """
    keep = []
    for i, c in enumerate(chunks):
        ts = c.meta.get("timestamp_iso")
        if ts is not None and ts.year == year:
            keep.append(i)
    return keep


def apply_time_decay(scores, chunks, query_time, alpha=0.3, lambd=0.5):
    """
    Soft time-decay scoring:
    Combines semantic similarity with temporal recency.
    """
    new_scores = []

    for s, c in zip(scores, chunks):
        ts = c.meta.get("timestamp")

        # If no timestamp, keep original score
        if ts is None:
            new_scores.append(s)
            continue

        # Time difference in years
        delta_years = abs((query_time - ts).days) / 365

        # Rational decay function
        time_score = 1 / (1 + lambd * delta_years)

        # Final combined score
        final_score = (1 - alpha) * s + alpha * time_score
        new_scores.append(final_score)

    return np.array(new_scores)


def retrieve(query: str, method: str, chunking_type: str, k: int):
    """
    Main retrieval function with temporal awareness (Stage 3).
    """
    # Load pre-built index
    index = VectorIndex.load(INDEX_PATHS[chunking_type])

    # Compute similarity scores
    bm25_scores = index.bm25_scores(query)
    q_vec = index.encode_query_dense(query)
    dense_scores = cosine_similarity(q_vec, index.dense_matrix)[0]

    # Choose retrieval method
    if method == "bm25":
        final_scores = bm25_scores
    elif method == "dense":
        final_scores = dense_scores
    elif method == "hybrid":
        norm_bm25 = (bm25_scores - bm25_scores.min()) / (bm25_scores.max() - bm25_scores.min() + 1e-6)
        final_scores = 0.3 * norm_bm25 + 0.7 * dense_scores
    else:
        raise ValueError("Unknown method")

    # ----- Stage 3 begins -----
    chunks = index.chunks

    # Hard temporal filtering
    year_constraint = extract_year_from_query(query)
    if year_constraint is not None:
        idx = hard_time_filter(chunks, final_scores, year_constraint)
        final_scores = final_scores[idx]
        chunks = [chunks[i] for i in idx]

    # Soft temporal decay
    query_time = datetime.now()
    final_scores = apply_time_decay(
        final_scores,
        chunks,
        query_time,
        alpha=0.3,
        lambd=0.5
    )

    # Rank and return Top-K
    top_idx = np.argsort(-final_scores)[:k]

    results = []
    for i in top_idx:
        c = chunks[i]
        results.append({
            "chunk_id": c.chunk_id,
            "text": c.text,
            "score": float(final_scores[i]),
            "method_used": method
        })

    return results
