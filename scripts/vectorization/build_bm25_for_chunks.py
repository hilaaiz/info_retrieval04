"""
build_bm25_for_chunks.py
========================

מריץ BM25 על שני סוגי צ'אנקים:

1. chunks_output         – החלוקה הקבועה (660 מילים + overlap)
2. hierarchical_chunks   – החלוקה ההיררכית

שומר הכול תחת:
    bm25_chunks_outputs/
        fixed/
            X_bm25_chunks.npz
            chunks_metadata.csv
            ...
        hierarchical/
            X_bm25_chunks.npz
            chunks_metadata.csv
            ...
"""

from pathlib import Path

from bm25_core import (
    get_nltk_stopwords,
    load_chunk_documents,
    build_bm25_matrix,
    save_bm25_outputs,
)


def run_for_chunks(chunks_root: str | Path, out_parent: str | Path, subdir_name: str):
    """
    מריץ BM25 עבור תיקיית צ'אנקים אחת ושומר בתיקיית־בן בתוך out_parent.
    """
    chunks_root = Path(chunks_root)
    out_parent = Path(out_parent)
    output_folder = out_parent / subdir_name

    print("\n" + "=" * 80)
    print(f"🚀 Running BM25 for chunks in: {chunks_root}")
    print(f"   Output will be saved to: {output_folder}")
    print("=" * 80)

    # 1. Load chunks
    df_chunks = load_chunk_documents(chunks_root)
    if df_chunks.empty:
        print(f"❌ No chunks loaded from {chunks_root}. Skipping.")
        return

    df_chunks = df_chunks.reset_index(drop=True)
    df_chunks["row_index"] = df_chunks.index  # mapping row -> chunk

    # 2. Build BM25
    documents = df_chunks["text"].tolist()

    BM25_MIN_DF = 5
    BM25_MAX_DF = 0.95
    BM25_MAX_FEATURES = 20000

    nltk_stopwords = get_nltk_stopwords()

    X_bm25, feature_names, vectorizer, stats = build_bm25_matrix(
        documents=documents,
        stopwords_set=nltk_stopwords,
        min_df=BM25_MIN_DF,
        max_df=BM25_MAX_DF,
        max_features=BM25_MAX_FEATURES,
        matrix_name=f"BM25-CHUNKS-{subdir_name.upper()}",
    )

    # 3. Save outputs
    save_bm25_outputs(
        output_folder=output_folder,
        X_bm25=X_bm25,
        feature_names=feature_names,
        stats=stats,
        df_chunks=df_chunks,
    )


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║   BM25 for CHUNKS (fixed + hierarchical)                     ║
║   - Uses bm25_core.py                                        ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # תיקיית־על לכל הפלטים של BM25 לצ'אנקים
    DEFAULT_OUTPUT_ROOT = "bm25_chunks_outputs"

    # תיקיות הצ'אנקים:
    FIXED_CHUNKS_ROOT = "chunks_output"         # מהchunk_fixed_overlap
    HIER_CHUNKS_ROOT = "hierarchical_chunks"    # מהhierarchical_chunk

    # אפשר לשנות אם צריך, או להפוך לקלט מהמשתמש
    output_root = Path(DEFAULT_OUTPUT_ROOT)
    output_root.mkdir(parents=True, exist_ok=True)

    # 1. חלוקה קבועה (fixed / overlap)
    run_for_chunks(
        chunks_root=FIXED_CHUNKS_ROOT,
        out_parent=output_root,
        subdir_name="fixed",
    )

    # 2. חלוקה היררכית
    run_for_chunks(
        chunks_root=HIER_CHUNKS_ROOT,
        out_parent=output_root,
        subdir_name="hierarchical",
    )

    print("\n🎉 All BM25 chunk runs completed!")


if __name__ == "__main__":
    main()
