from datetime import timedelta
from scripts.retriever import retrieve
from scripts.vectorization.vector_index import VectorIndex

INDEX_PATHS = {
    "fixed_660": "vector_indexes/fixed_660/vector_index.pkl",
    "hierarchical": "vector_indexes/hierarchical/vector_index.pkl",
}

def split_early_late(chunks, months=8):
    chunks = sorted(chunks, key=lambda c: c.meta["timestamp"])

    start = chunks[0].meta["timestamp"]
    end = chunks[-1].meta["timestamp"]

    early_end = start + timedelta(days=30 * months)
    late_start = end - timedelta(days=30 * months)

    early = [c for c in chunks if c.meta["timestamp"] <= early_end]
    late = [c for c in chunks if c.meta["timestamp"] >= late_start]

    return early, late


def evolution_retrieve(query, method, chunking_type, k=5):
    index = VectorIndex.load(INDEX_PATHS[chunking_type])

    early_chunks, late_chunks = split_early_late(index.chunks)

    early_results = retrieve(query, method, chunking_type, k)
    late_results = retrieve(query, method, chunking_type, k)

    return early_results, late_results


def build_evolution_prompt(query, early_chunks, late_chunks):
    prompt = f"""
QUESTION:
{query}

EARLY PERIOD:
"""
    for c in early_chunks:
        prompt += f"- {c['text']}\n"

    prompt += "\nLATE PERIOD:\n"

    for c in late_chunks:
        prompt += f"- {c['text']}\n"

    prompt += """
TASKS:
1. Describe the early position.
2. Describe the late position.
3. Explain how it changed.
4. Suggest reasons for the change.
"""
    return prompt
