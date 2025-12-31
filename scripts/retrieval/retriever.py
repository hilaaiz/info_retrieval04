# retriever.py
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scripts.vectorization.vector_index import VectorIndex
from scripts.common.models import Chunk


INDEX_PATHS = {
    "fixed_660": "vector_indexes/fixed_660/vector_index.pkl",
    "hierarchical": "vector_indexes/hierarchical/vector_index.pkl",
}


def retrieve(query: str, method: str, chunking_type: str, k: int):
    index = VectorIndex.load(INDEX_PATHS[chunking_type])

    # 1. חישוב ציוני BM25
    bm25_scores = index.bm25_scores(query)
    
    # 2. חישוב ציוני Dense (E5)
    q_vec = index.encode_query_dense(query)
    dense_scores = cosine_similarity(q_vec, index.dense_matrix)[0]

    # בחירת הדירוג הסופי לפי השיטה
    if method == "bm25":
        final_scores = bm25_scores
    elif method == "dense":
        final_scores = dense_scores
    elif method == "hybrid":
        # מימוש היברידי פשוט: נרמול ושילוב (או RRF)
        # כאן לצורך הפשטות נבצע שקלול מנורמל:
        norm_bm25 = (bm25_scores - np.min(bm25_scores)) / (np.max(bm25_scores) - np.min(bm25_scores) + 1e-6)
        final_scores = (0.3 * norm_bm25) + (0.7 * dense_scores)
    else:
        raise ValueError("Unknown method")

    # שליפת ה-top-k
    top_idx = np.argsort(-final_scores)[:k]

    # בניית התוצאות
    results = []
    for i in top_idx:
        c = index.chunks[i]
        results.append({
            "chunk_id": c.chunk_id,
            "text": c.text,
            "score": float(final_scores[i]),
            "method_used": method
        })
    return results

if __name__ == "__main__":
     query = "When was the defense budget discussed?"

     results = retrieve(
         query=query,
         method="bm25",          # או "dense"
         chunking_type="fixed_660",  # או "hierarchical"
         k=5
     )

     for r in results:
         print("DOC:", r["doc_id"])
         print("CHUNK:", r["chunk_id"])
         print("SCORE:", r["score"])
         print("SOURCE:", r["source_path"])
         print(r["text"][:300])
         print("-" * 50)
