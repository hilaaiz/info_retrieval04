"""
bm25_core.py
============

כל הפונקציות והמחלקות שקשורות ל-BM25 וטעינת צ'אנקים.
אפשר להשתמש גם בתרגילים / סקריפטים אחרים.
"""

import os
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")

from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import save_npz

# NLTK stopwords
import nltk
from nltk.corpus import stopwords


# ----------------------------------------------------
# BM25 Transformer
# ----------------------------------------------------
class BM25Transformer:
    """
    BM25/Okapi Transformer
    """

    def __init__(self, k1=1.5, b=0.75):
        self.k1 = k1
        self.b = b

    def fit_transform(self, tf_matrix, doc_lengths, avg_doc_length, idf_vector):
        """
        tf_matrix: sparse matrix (n_docs x n_terms) from TfidfVectorizer
        doc_lengths: array of length n_docs – document lengths
        avg_doc_length: average document length
        idf_vector: vectorizer.idf_
        """
        bm25_matrix = tf_matrix.copy()

        for i in range(bm25_matrix.shape[0]):
            doc_len = doc_lengths[i]
            length_norm = 1 - self.b + self.b * (doc_len / avg_doc_length)

            row = bm25_matrix.getrow(i)
            row_data = row.data  # term frequencies (after tf-idf)

            # BM25 core formula (per term in this row)
            row_data = row_data * (self.k1 + 1) / (row_data + self.k1 * length_norm)

            col_indices = row.indices
            row_data = row_data * idf_vector[col_indices]

            # write back transformed row data
            bm25_matrix.data[bm25_matrix.indptr[i]:bm25_matrix.indptr[i + 1]] = row_data

        return bm25_matrix


# ----------------------------------------------------
# NLTK stopwords helpers
# ----------------------------------------------------
def download_nltk_data():
    """Verify NLTK stopwords are available; download if needed."""
    print("\n📥 Checking NLTK stopwords...")
    try:
        _ = stopwords.words("english")
        print("✅ NLTK stopwords already available")
    except LookupError:
        print("📥 Downloading NLTK stopwords...")
        nltk.download("stopwords", quiet=True)
        print("✅ Download completed!")


def get_nltk_stopwords():
    print("\n🛑 Loading NLTK stopwords...")
    download_nltk_data()
    sw = set(stopwords.words("english"))
    print(f"   • Loaded {len(sw)} stopwords (pure NLTK)")
    return sw


# ----------------------------------------------------
# Load CHUNK documents
# ----------------------------------------------------
def load_chunk_documents(chunks_root_folder: str | Path) -> pd.DataFrame:
    """
    Reads all chunk files from a folder like:

        chunks_root/
            UK_...txt_chunks/
                chunk_1.txt
                chunk_2.txt
            US_...txt_chunks/
                ...

    Returns DataFrame with:
        - text         : chunk text
        - country      : UK / US (inferred from original filename)
        - orig_file    : original document filename (without _chunks)
        - chunk_file   : chunk filename (e.g., 'chunk_1.txt')
        - chunk_path   : relative path to chunk file
        - chunk_id     : unique id (row index)
    """
    root = Path(chunks_root_folder)
    if not root.exists():
        raise FileNotFoundError(f"Chunks root folder not found: {root}")

    print(f"\n📂 Loading CHUNK documents from: {root}")

    rows = []
    # Expect subfolders: <original_filename>_chunks
    subdirs = [d for d in root.iterdir() if d.is_dir()]

    if not subdirs:
        print("⚠️ No subdirectories found. Did you point to the correct chunks folder?")
        print("   Expecting structure like: chunks_output/UK_XXXX.txt_chunks/")
        return pd.DataFrame()

    for subdir in tqdm(subdirs, desc="Traversing chunk folders"):
        # Example: 'UK_1994-01-01.txt_chunks'
        folder_name = subdir.name

        # Try to deduce original filename (without '_chunks')
        if folder_name.endswith("_chunks"):
            orig_filename = folder_name[:-7]  # strip '_chunks'
        else:
            orig_filename = folder_name

        # Infer country from original filename prefix
        if orig_filename.startswith("UK_"):
            country = "UK"
        elif orig_filename.startswith("US_"):
            country = "US"
        else:
            country = "UNKNOWN"

        # Iterate chunk files
        for chunk_file in sorted(subdir.glob("*.txt")):
            try:
                with open(chunk_file, "r", encoding="utf-8") as f:
                    text = f.read()
            except Exception as e:
                print(f"⚠️ Error reading {chunk_file}: {e}")
                continue

            if not text.strip():
                continue

            rows.append({
                "text": text,
                "country": country,
                "orig_file": orig_filename,
                "chunk_file": chunk_file.name,
                "chunk_path": str(chunk_file.relative_to(root)),
            })

    df = pd.DataFrame(rows)
    df = df.reset_index(drop=True)
    df["chunk_id"] = df.index  # unique id per chunk

    print(f"\n✅ Total chunks loaded: {len(df)}")
    print("   • Country counts:")
    print(df["country"].value_counts(dropna=False))

    return df


# ----------------------------------------------------
# Build TF-IDF + BM25 on all chunks
# ----------------------------------------------------
def build_bm25_matrix(
    documents,
    stopwords_set,
    min_df=5,
    max_df=0.95,
    max_features=20000,
    matrix_name="BM25-CHUNKS",
):
    """
    Build a BM25 matrix over all chunk texts.

    documents: list of chunk texts
    """
    print(f"\n{'='*70}")
    print(f"🔨 Building {matrix_name}")
    print(f"{'='*70}")

    vectorizer = TfidfVectorizer(
        min_df=min_df,
        max_df=max_df,
        max_features=max_features,
        stop_words=list(stopwords_set),
        lowercase=True,
        token_pattern=r"(?u)\b\w+\b",
        ngram_range=(1, 1),
        norm="l2",
        use_idf=True,
        smooth_idf=True,
    )

    print("\n🔄 Fitting TF-IDF vectorizer on ALL chunks...")
    tfidf_matrix = vectorizer.fit_transform(tqdm(documents, desc="Vectorizing chunks"))
    feature_names = vectorizer.get_feature_names_out()
    print(f"\n✅ TF-IDF created: shape={tfidf_matrix.shape}")

    # BM25
    print("\n🔄 Applying BM25 transformation on chunks...")
    doc_lengths = np.array(tfidf_matrix.sum(axis=1)).flatten()
    avg_doc_length = doc_lengths.mean()
    idf_vector = vectorizer.idf_

    bm25_matrix = BM25Transformer().fit_transform(
        tfidf_matrix, doc_lengths, avg_doc_length, idf_vector
    )

    stats = {
        "matrix_name": matrix_name,
        "num_documents": bm25_matrix.shape[0],
        "num_features": bm25_matrix.shape[1],
        "sparsity": (1 - bm25_matrix.nnz / (bm25_matrix.shape[0] * bm25_matrix.shape[1])) * 100,
        "non_zero_elements": bm25_matrix.nnz,
    }

    print("✅ BM25 matrix for chunks ready")
    print(f"   • Chunks:   {stats['num_documents']}")
    print(f"   • Features: {stats['num_features']}")
    print(f"   • Sparsity: {stats['sparsity']:.2f}%")

    return bm25_matrix, feature_names, vectorizer, stats


# ----------------------------------------------------
# Helper: save BM25 outputs
# ----------------------------------------------------
def save_bm25_outputs(output_folder: str | Path, X_bm25, feature_names, stats, df_chunks: pd.DataFrame):
    """
    שומר:
      - X_bm25_chunks.npz
      - chunks_metadata.csv
      - bm25_feature_names.txt
      - bm25_stats.csv
    """
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    print(f"\n💾 Saving outputs to: {output_folder}")

    # BM25 matrix
    save_npz(output_folder / "X_bm25_chunks.npz", X_bm25)

    # metadata
    df_chunks.to_csv(output_folder / "chunks_metadata.csv", index=False)

    # vocabulary
    with open(output_folder / "bm25_feature_names.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(feature_names))

    # stats
    pd.DataFrame([stats]).to_csv(output_folder / "bm25_stats.csv", index=False)

    print("✅ Saved:")
    print(f"   • X matrix: {output_folder / 'X_bm25_chunks.npz'}")
    print(f"   • metadata: {output_folder / 'chunks_metadata.csv'}")
    print(f"   • vocab:    {output_folder / 'bm25_feature_names.txt'}")
    print(f"   • stats:    {output_folder / 'bm25_stats.csv'}")
