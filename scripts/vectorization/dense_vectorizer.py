from scripts.vectorization.vector_index import (
    VectorIndex,
    load_chunks_from_dir,
)

# =============================
# Build script
# =============================
if __name__ == "__main__":

    # -------- CONFIG --------
    CHUNKS_DIR = "chunks_660_output"      # או chunks_660_output
    CHUNKING_NAME = "fixed_660"           # או fixed_660
    OUTPUT_INDEX = "vector_index_fixed_660.pkl"
    # ------------------------

    print("[LOAD] Loading chunks...")
    chunks = load_chunks_from_dir(CHUNKS_DIR, CHUNKING_NAME)
    print(f"[LOAD] {len(chunks)} chunks loaded")

    print("[BUILD] Building vector index...")
    index = VectorIndex(chunks)

    print("[SAVE] Saving index...")
    index.save(OUTPUT_INDEX)

    print("[DONE] Vectorization complete")
