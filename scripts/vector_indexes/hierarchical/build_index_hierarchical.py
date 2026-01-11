import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from scripts.vectorization.vector_index import VectorIndex, load_chunks_from_dir



if __name__ == "__main__":
    CHUNKS_DIR = "hierarchical_chunks"
    CHUNKING_NAME = "hierarchical"

    # תיקיית יעד
    OUTPUT_DIR = PROJECT_ROOT / "vector_indexes" / "hierarchical"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    OUTPUT_INDEX = OUTPUT_DIR / "vector_index.pkl"

    print("[LOAD] Loading chunks...")
    chunks = load_chunks_from_dir(CHUNKS_DIR, CHUNKING_NAME)
    print(f"[LOAD] {len(chunks)} chunks loaded")

    print("[BUILD] Building vector index (BM25 + Dense)...")
    index = VectorIndex(chunks)

    print("[SAVE] Saving index...")
    index.save(str(OUTPUT_INDEX))

    print("[DONE] Saved:", OUTPUT_INDEX)
