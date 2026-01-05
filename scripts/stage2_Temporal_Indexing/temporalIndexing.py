import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

import pandas as pd
import numpy as np
from scipy.sparse import load_npz
import pickle
import json
from datetime import datetime
import re
import matplotlib.pyplot as plt
from collections import defaultdict, Counter
import logging

# For NLP date extraction
import spacy
from dateutil import parser as dateutil_parser

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 1. LOAD DATA
# ============================================================

def load_chunks_metadata():
    """Load chunks_metadata.csv from both chunking methods"""
    print("📂 Loading BM25 metadata...")
    
    df_fixed = pd.read_csv("bm25_chunks_outputs/fixed/chunks_metadata.csv")
    df_fixed['chunking_method'] = 'fixed_660'
    
    df_hier = pd.read_csv("bm25_chunks_outputs/hierarchical/chunks_metadata.csv")
    df_hier['chunking_method'] = 'hierarchical'
    
    df_all = pd.concat([df_fixed, df_hier], ignore_index=True)
    df_all = df_all.reset_index(drop=True)
    df_all['chunk_id'] = df_all.index

    df_all['country'] = df_all['orig_file'].str.extract(r'(uk|us)', expand=False).str.upper()
    df_all['country'] = df_all['country'].fillna('UNKNOWN')

    return df_all

def load_bm25_matrices():
    """Load BM25 sparse matrices"""
    print("📊 Loading BM25 matrices...")
    
    X_fixed = load_npz("bm25_chunks_outputs/fixed/X_bm25_chunks.npz")
    X_hier = load_npz("bm25_chunks_outputs/hierarchical/X_bm25_chunks.npz")
    
    return {
        'fixed_660': X_fixed,
        'hierarchical': X_hier
    }

def load_dense_embeddings():
    """Load dense embeddings"""
    print("🧠 Loading dense embeddings...")
    
    with open("vector_indexes/fixed_660/vector_index.pkl", "rb") as f:
        idx_fixed = pickle.load(f)
    
    with open("vector_indexes/hierarchical/vector_index.pkl", "rb") as f:
        idx_hier = pickle.load(f)
    
    return {
        'fixed_660': idx_fixed.dense_matrix,
        'hierarchical': idx_hier.dense_matrix
    }

# ============================================================
# 2. DATE EXTRACTION 
# ============================================================

EPOCH = datetime(1970, 1, 1)

def to_unix_seconds_safe(dt: datetime) -> int:
    """Convert datetime to Unix timestamp (supports pre-1970 dates)"""
    return int((dt - EPOCH).total_seconds())

def extract_date_from_filename(filename):
    """
    Extract date from filename patterns like:
    - uk_2023-07-03.txt → 2023-07-03
    - us_2024-01-15.pdf → 2024-01-15
    """
    if not filename:
        return None
    
    # Pattern: YYYY-MM-DD
    match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', filename)
    if match:
        year, month, day = match.groups()
        date_str = f"{year}-{month}-{day}"
        # Validate
        try:
            datetime.fromisoformat(date_str)
            return date_str
        except ValueError:
            return None
    
    return None


# ============================================================
# 3. TEMPORAL VECTOR INDEX CLASS
# ============================================================

class TemporalVectorIndex:
    """Unified temporal index with embeddings and temporal metadata"""
    
    def __init__(self):
        self.chunks = []
        self.extraction_log = []
    
    def add_chunk_with_temporal_metadata(self,
                                         chunk_id,
                                         text,
                                         source,
                                         country,
                                         chunking_method,
                                         embedding_method,
                                         bm25_vector=None,
                                         dense_vector=None):
        """Add chunk with automatic date extraction and temporal metadata"""
        
        # Extract date using filename
        timestamp_iso = extract_date_from_filename(source)
        strategy = "filename" if timestamp_iso else "none"
        confidence = 1.0 if timestamp_iso else 0     

        timestamp_unix = None
        extraction_success = False
        
        if timestamp_iso:
            try:
                dt = datetime.fromisoformat(timestamp_iso)
                timestamp_unix = to_unix_seconds_safe(dt)
                extraction_success = True
            except Exception as e:
                logger.warning(f"Error converting {timestamp_iso}: {e}")
                timestamp_unix = None
                extraction_success = False
        
        # Log extraction attempt
        self.extraction_log.append({
            'chunk_id': chunk_id,
            'success': extraction_success,
            'date': timestamp_iso,
            'confidence': confidence,
            'country': country,
            'source': source,
            'embedding_method': embedding_method
        })
        
        # Convert BM25 vector if provided
        bm25_dense = None
        if bm25_vector is not None:
            if hasattr(bm25_vector, 'toarray'):
                bm25_dense = bm25_vector.toarray().flatten().tolist()
            else:
                bm25_dense = bm25_vector.tolist() if isinstance(bm25_vector, np.ndarray) else bm25_vector
        
        # Convert dense vector if provided
        dense_list = None
        if dense_vector is not None:
            if isinstance(dense_vector, np.ndarray):
                dense_list = dense_vector.tolist()
            else:
                dense_list = dense_vector
        
        chunk_data = {
            'id': chunk_id,
            'text_preview': text[:300] + "..." if len(text) > 300 else text,
            'source': source,
            'country': country,
            'chunking_method': chunking_method,
            'embedding_method': embedding_method,
            'timestamp_iso': timestamp_iso,
            'timestamp_unix': timestamp_unix,
            'extraction_confidence': confidence,
            'embeddings_shape': {
                'bm25': len(bm25_dense) if bm25_dense else None,
                'dense_e5_base': len(dense_list) if dense_list else None
            }
        }
        
        self.chunks.append(chunk_data)
    
    def save_to_json(self, filepath):
        """Save temporal index to JSON"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.chunks, f, ensure_ascii=False, indent=2)
        print(f"✅ Saved temporal index to {filepath}")
    
    def save_extraction_log(self, filepath):
        """Save date extraction analysis"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        df_log = pd.DataFrame(self.extraction_log)
        df_log.to_csv(filepath, index=False)
        print(f"✅ Saved extraction log to {filepath}")
        
        # Print detailed summary
        success_count = df_log['success'].sum()
        total_count = len(df_log)
        success_rate = 100 * success_count / total_count if total_count > 0 else 0
        
        print("\n" + "="*70)
        print("📊 DATE EXTRACTION ANALYSIS")
        print("="*70)
        print(f"   Total chunks processed: {total_count}")
        print(f"   ✅ Successful: {success_count} ({success_rate:.1f}%)")
        print(f"   ❌ Failed: {total_count - success_count} ({100-success_rate:.1f}%)")
        
        print(f"\n   By embedding method:")
        for emb_method in df_log['embedding_method'].unique():
            method_data = df_log[df_log['embedding_method'] == emb_method]
            method_success = method_data['success'].sum()
            method_total = len(method_data)
            print(f"      • {emb_method}: {method_success}/{method_total} ({100*method_success/method_total:.1f}%)")
        
        print(f"\n   By country:")
        for country in df_log['country'].unique():
            country_data = df_log[df_log['country'] == country]
            country_success = country_data['success'].sum()
            country_total = len(country_data)
            print(f"      • {country}: {country_success}/{country_total} ({100*country_success/country_total:.1f}%)")
        
        print("="*70 + "\n")
    
    def save_to_parquet(self, filepath):
        """Save metadata to Parquet"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(self.chunks)
        df.to_parquet(filepath)
        print(f"✅ Saved metadata to {filepath}")
    
    def get_stats(self):
        """Print comprehensive statistics"""
        dates = [c['timestamp_iso'] for c in self.chunks if c['timestamp_iso']]
        
        print("\n" + "="*70)
        print("📊 TEMPORAL INDEX STATISTICS")
        print("="*70)
        print(f"   Total chunks: {len(self.chunks)}")
        print(f"   Chunks with valid dates: {len(dates)} ({100*len(dates)/len(self.chunks):.1f}%)")
        print(f"   Chunks without dates: {len(self.chunks) - len(dates)}")
        
        if dates:
            years = [d[:4] for d in dates]
            print(f"\n   📅 Date range: {min(dates)} to {max(dates)}")
            year_dist = dict(Counter(years))
            print(f"   📈 Year distribution:")
            for year in sorted(year_dist.keys()):
                print(f"      {year}: {year_dist[year]} chunks")
        
        print(f"\n   🌍 Country distribution:")
        countries = Counter([c['country'] for c in self.chunks])
        for country, count in sorted(countries.items()):
            print(f"      {country}: {count}")
        
        print(f"\n   🔀 Chunking methods:")
        methods = Counter([c['chunking_method'] for c in self.chunks])
        for method, count in sorted(methods.items()):
            print(f"      {method}: {count}")
        
        print(f"\n   🧠 Embedding methods:")
        embeddings = Counter([c['embedding_method'] for c in self.chunks])
        for emb, count in sorted(embeddings.items()):
            print(f"      {emb}: {count}")
        print("="*70 + "\n")

# ============================================================
# 4. VISUALIZATION
# ============================================================

def visualize_temporal_dist(temporal_index, output_path="stage2_outputs/temporal_distribution.png"):
    """Create histogram of temporal distribution"""
    
    years = defaultdict(int)
    
    for chunk in temporal_index.chunks:
        if chunk['timestamp_iso']:
            year = chunk['timestamp_iso'][:4]
            years[year] += 1
    
    if not years:
        print("⚠️ No valid dates to visualize")
        return
    
    sorted_years = sorted(years.items())
    x = [y for y, c in sorted_years]
    y = [c for y, c in sorted_years]
    
    plt.figure(figsize=(14, 6))
    plt.bar(x, y, color='steelblue', edgecolor='black', alpha=0.7)
    plt.xlabel('Year', fontsize=12, fontweight='bold')
    plt.ylabel('Number of Chunks', fontsize=12, fontweight='bold')
    plt.title('Temporal Distribution of Corpus (Stage 2)', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved temporal distribution plot to {output_path}")
    plt.show()



# ============================================================
# 5. MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 STAGE 2: TEMPORAL INDEXING WITH SEPARATED EMBEDDING METHODS")
    print("="*70)
    
    # Load data
    print("\n📥 LOADING DATA...")
    df_chunks = load_chunks_metadata()
    bm25_matrices = load_bm25_matrices()
    dense_embeddings = load_dense_embeddings()
    
    print(f"\n📊 Loaded data summary:")
    print(f"   • Total chunks: {len(df_chunks)}")
    print(f"   • Chunking methods: {df_chunks['chunking_method'].unique()}")
    print(f"   • Country distribution: {df_chunks['country'].value_counts().to_dict()}")
    
    # Build temporal index
    print(f"\n🔨 BUILDING TEMPORAL INDEX...")
    temporal_index = TemporalVectorIndex()
    
    for chunking_method in ['fixed_660', 'hierarchical']:
        print(f"\n   ⏳ Processing chunking method: {chunking_method}")
        
        df_subset = df_chunks[df_chunks['chunking_method'] == chunking_method].reset_index(drop=True)
        print(f"      Total chunks in this method: {len(df_subset)}")
        
        bm25_mat = bm25_matrices[chunking_method]
        dense_mat = dense_embeddings[chunking_method]
        
        # Process BM25 embeddings
        print(f"      ➡️ Processing BM25 embeddings...")
        for idx, row in df_subset.iterrows():
            if idx % 500 == 0:
                print(f"         BM25: Processing chunk {idx}/{len(df_subset)}", end='\r')
            
            bm25_row = bm25_mat[idx]
            
            temporal_index.add_chunk_with_temporal_metadata(
                chunk_id=f"{chunking_method}_bm25_{idx}",
                text=row['text'],
                source=row['orig_file'],
                country=row['country'],
                chunking_method=chunking_method,
                embedding_method='bm25',
                bm25_vector=bm25_row,
                dense_vector=None
            )
        print(f"      ✅ BM25 completed: {len(df_subset)} chunks added")
        
        # Process Dense embeddings
        print(f"      ➡️ Processing Dense embeddings...")
        for idx, row in df_subset.iterrows():
            if idx % 500 == 0:
                print(f"         Dense: Processing chunk {idx}/{len(df_subset)}", end='\r')
            
            dense_row = dense_mat[idx]
            
            temporal_index.add_chunk_with_temporal_metadata(
                chunk_id=f"{chunking_method}_dense_{idx}",
                text=row['text'],
                source=row['orig_file'],
                country=row['country'],
                chunking_method=chunking_method,
                embedding_method='dense_e5_base',
                bm25_vector=None,
                dense_vector=dense_row
            )
        print(f"      ✅ Dense completed: {len(df_subset)} chunks added")
    
    # Save outputs
    print(f"\n💾 SAVING OUTPUTS...")
    temporal_index.save_to_json("stage2_outputs/temporal_index_stage2.json")
    temporal_index.save_to_parquet("stage2_outputs/temporal_index_metadata.parquet")
    temporal_index.save_extraction_log("stage2_outputs/date_extraction_analysis.csv")
    
    # Statistics
    temporal_index.get_stats()
    
    # Visualization
    print("📈 CREATING VISUALIZATIONS...")
    visualize_temporal_dist(temporal_index)
    
    print("\n✅ STAGE 2 COMPLETED SUCCESSFULLY!")
    print(f"   📁 Output files saved to: stage2_outputs/")