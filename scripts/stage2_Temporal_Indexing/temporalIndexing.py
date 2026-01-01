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
# 1. ADVANCED DATE EXTRACTION (Regex → spaCy → dateutil)
# ============================================================

EPOCH = datetime(1970, 1, 1)

def to_unix_seconds_safe(dt: datetime) -> int:
    """Convert datetime to Unix timestamp (supports pre-1970 dates)"""
    return int((dt - EPOCH).total_seconds())

class DateExtractor:
    """Multi-strategy date extraction: Regex → spaCy → dateutil"""
    
    def __init__(self):
        """Initialize NLP model"""
        try:
            self.nlp = spacy.load("en_core_web_sm")
            self.spacy_available = True
            print("✅ spaCy model loaded successfully")
        except OSError:
            self.spacy_available = False
            print("⚠️ spaCy model not found. Install with: python -m spacy download en_core_web_sm")
    
    def extract_by_regex(self, text):
        """
        Strategy 1: Regex patterns for common date formats
        Returns: (date_str, confidence)
        """
        if not text or not isinstance(text, str):
            return None, 0
        
        # Pattern 1: "Month Day, Year" or "Month Day Year" (e.g., "January 6, 2021")
        pattern1 = r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(\d{4})'
        match = re.search(pattern1, text, re.IGNORECASE)
        if match:
            return match.group(0), 0.95  # High confidence
        
        # Pattern 2: "Day Month Year" (e.g., "6 January 2021")
        pattern2 = r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})'
        match = re.search(pattern2, text, re.IGNORECASE)
        if match:
            return match.group(0), 0.95
        
        # Pattern 3: "Month Year" (e.g., "June 2020")
        pattern3 = r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})'
        match = re.search(pattern3, text, re.IGNORECASE)
        if match:
            return match.group(0), 0.85  # Medium confidence (day unknown)
        
        # Pattern 4: ISO format "YYYY-MM-DD"
        pattern4 = r'(\d{4})[_/-](\d{2})[_/-](\d{2})'
        match = re.search(pattern4, text)
        if match:
            return match.group(0), 0.95
        
        # Pattern 5: Year only "YYYY"
        pattern5 = r'\b(19[89]\d{2}|20\d{2})\b'
        match = re.search(pattern5, text)
        if match:
            return match.group(0), 0.6  # Low confidence (year only)
        
        return None, 0
    
    def extract_by_spacy(self, text):
        """
        Strategy 2: spaCy NLP for DATE entity recognition
        Returns: (date_str, confidence)
        """
        if not self.spacy_available or not text:
            return None, 0
        
        try:
            doc = self.nlp(text[:500])  # Process first 500 chars only (speed)
            
            # Extract DATE entities
            date_entities = [ent.text for ent in doc.ents if ent.label_ == "DATE"]
            
            if date_entities:
                # Return the first/longest date found
                best_date = max(date_entities, key=len)
                return best_date, 0.80  # Medium-high confidence
            
            return None, 0
        except Exception as e:
            logger.warning(f"spaCy extraction failed: {e}")
            return None, 0
    
    def extract_by_dateutil(self, text):
        """
        Strategy 3: dateutil fuzzy parsing for ambiguous dates
        Returns: (date_str ISO format, confidence)
        """
        if not text:
            return None, 0
        
        try:
            # Extract potential date strings (5+ word sequences)
            sentences = text.split('.')
            for sentence in sentences[:3]:  # Check first 3 sentences only
                words = sentence.split()
                for i in range(len(words) - 2):
                    potential_date = ' '.join(words[i:i+4])
                    try:
                        parsed_date = dateutil_parser.parse(potential_date, fuzzy=True)
                        # Validate that date is reasonable (not in far future/past)
                        if 1900 <= parsed_date.year <= 2100:
                            return parsed_date.strftime('%Y-%m-%d'), 0.70
                    except (ValueError, TypeError):
                        continue
            
            return None, 0
        except Exception as e:
            logger.warning(f"dateutil extraction failed: {e}")
            return None, 0
    
    def normalize_date(self, date_str):
        """
        Normalize date string to ISO 8601 format (YYYY-MM-DD)
        """
        if not date_str:
            return None
        
        try:
            # Try direct ISO parsing
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                datetime.fromisoformat(date_str)
                return date_str
            
            # Try dateutil parser
            parsed = dateutil_parser.parse(date_str, fuzzy=False)
            return parsed.strftime('%Y-%m-%d')
        except Exception as e:
            logger.debug(f"Could not normalize {date_str}: {e}")
            return None
    
    def extract(self, text):
        """
        Main extraction method: tries Regex → spaCy → dateutil
        Returns: (date_iso, strategy_used, confidence)
        """
        if not text:
            return None, "none", 0
        
        # Strategy 1: Regex (fastest, most reliable)
        date_str, conf = self.extract_by_regex(text)
        if date_str:
            normalized = self.normalize_date(date_str)
            if normalized:
                return normalized, "regex", conf
        
        # Strategy 2: spaCy (good for named entities)
        if self.spacy_available:
            date_str, conf = self.extract_by_spacy(text)
            if date_str:
                normalized = self.normalize_date(date_str)
                if normalized:
                    return normalized, "spacy", conf
        
        # Strategy 3: dateutil (fuzzy, last resort)
        date_str, conf = self.extract_by_dateutil(text)
        if date_str:
            return date_str, "dateutil", conf
        
        return None, "failed", 0

# ============================================================
# 2. LOAD DATA
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
# 3. TEMPORAL VECTOR INDEX CLASS
# ============================================================

class TemporalVectorIndex:
    """Unified temporal index with embeddings and temporal metadata"""
    
    def __init__(self):
        self.chunks = []
        self.extraction_log = []
        self.date_extractor = DateExtractor()
    
    def add_chunk_with_temporal_metadata(self,
                                         chunk_id,
                                         text,
                                         source,
                                         country,
                                         chunking_method,
                                         bm25_vector,
                                         dense_vector):
        """Add chunk with automatic date extraction and temporal metadata"""
        
        # Extract date using multi-strategy approach
        timestamp_iso, strategy, confidence = self.date_extractor.extract(text)
        
        timestamp_unix = None
        if timestamp_iso:
            try:
                dt = datetime.fromisoformat(timestamp_iso)
                timestamp_unix = to_unix_seconds_safe(dt)
                extraction_success = True
            except Exception as e:
                logger.warning(f"Error converting {timestamp_iso}: {e}")
                timestamp_unix = None
                extraction_success = False
        else:
            extraction_success = False
        
        # Log extraction attempt
        self.extraction_log.append({
            'chunk_id': chunk_id,
            'success': extraction_success,
            'date': timestamp_iso,
            'strategy': strategy,
            'confidence': confidence,
            'country': country,
            'source': source
        })
        
        # Convert BM25 vector
        if hasattr(bm25_vector, 'toarray'):
            bm25_dense = bm25_vector.toarray().flatten().tolist()
        else:
            bm25_dense = bm25_vector.tolist() if isinstance(bm25_vector, np.ndarray) else bm25_vector
        
        # Convert dense vector
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
            'timestamp_iso': timestamp_iso,
            'timestamp_unix': timestamp_unix,
            'extraction_strategy': strategy,
            'extraction_confidence': confidence,
            'embeddings_shape': {
                'bm25': len(bm25_dense),
                'dense_e5_base': len(dense_list)
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
        
        print(f"\n   Strategies used (successful extractions):")
        successful = df_log[df_log['success'] == True]
        if len(successful) > 0:
            for strategy, count in successful['strategy'].value_counts().items():
                avg_conf = successful[successful['strategy'] == strategy]['confidence'].mean()
                print(f"      • {strategy}: {count} chunks (avg confidence: {avg_conf:.2f})")
        
        print(f"\n   Confidence distribution:")
        print(f"      High (>0.85): {len(successful[successful['confidence'] > 0.85])}")
        print(f"      Medium (0.65-0.85): {len(successful[(successful['confidence'] >= 0.65) & (successful['confidence'] <= 0.85)])}")
        print(f"      Low (<0.65): {len(successful[successful['confidence'] < 0.65])}")
        
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
    print("🚀 STAGE 2: TEMPORAL INDEXING WITH ADVANCED DATE EXTRACTION")
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
        print(f"\n   ⏳ Processing: {chunking_method}")
        
        df_subset = df_chunks[df_chunks['chunking_method'] == chunking_method].reset_index(drop=True)
        print(f"      Chunks in this method: {len(df_subset)}")
        
        bm25_mat = bm25_matrices[chunking_method]
        dense_mat = dense_embeddings[chunking_method]
        
        for idx, row in df_subset.iterrows():
            if idx % 500 == 0:
                print(f"      Processing chunk {idx}/{len(df_subset)}", end='\r')
            
            bm25_row = bm25_mat[idx]
            dense_row = dense_mat[idx]
            
            temporal_index.add_chunk_with_temporal_metadata(
                chunk_id=f"{chunking_method}_{idx}",
                text=row['text'],
                source=row['orig_file'],
                country=row['country'],
                chunking_method=chunking_method,
                bm25_vector=bm25_row,
                dense_vector=dense_row
            )
        
        print(f"      ✅ Completed {chunking_method}")
    
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
    print(f"   📁 Output files saved to: stage2_outputs_new/")