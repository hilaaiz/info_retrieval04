import json
import csv

from datetime import datetime
from scripts.retrieval.temporal_retrieval import temporal_retrieve
from scripts.retrieval.retriever import retrieve_eval
from scripts.evolution_prompt import run_evolution_llm, Chunk

# -------------------------
# File logging
# -------------------------
OUT_FILE = f"stage4_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
CSV_OUT = f"stage4_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

def log(msg=""):
    with open(OUT_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def write_csv_header():
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "corpus",
            "chunking_method",
            "embedding_method",
            "query",
            "period",
            "rank_in_period",
            "chunk_id",
            "timestamp_iso",
            "text_preview"
        ])

def log_chunks(title, chunks):
    log(title)
    if not chunks:
        log("  [NO CHUNKS RETURNED]")
    for c in chunks:
        log(f"- {c['timestamp_iso']} | {c['text_preview']}")

# -------------------------
# Configuration
# -------------------------
def is_corpus(chunk, corpus):
   return chunk["source"].lower().startswith(corpus.lower() + "_")

INDEX_PATH = "stage2_outputs/temporal_index_stage2.json"

CORPORA = ["UK", "US"]

SYSTEMS = [
    ("fixed_660", "bm25"),
    ("fixed_660", "dense_e5_base"),
    ("hierarchical", "bm25"),
    ("hierarchical", "dense_e5_base"),
]

K = 5
MONTHS = 8

# -------------------------
# Queries (Stage 4)
# -------------------------
QUERIES = [
    "What was the specific budget allocated to security in 2024?",
    "What is the current official position regarding the State of Israel?",
    "What is the current official position regarding Hamas/Gaza?",
    "Was the official position in the last quarter of 2023 supportive of the State of Israel?",
    "Was the official position in the last quarter of 2023 supportive of Hamas/Gaza?",
    "Has the official position in the last quarter of 2023 changed relative to the official position in the last quarter of 2025?",
    "How did the Prime Minister/President's rhetoric regarding the war between Israel and Hamas/Gaza change between his first and last speech?",
    "Who is the Minister of Defense/Secretary of Defense?",
    "What was the official position regarding Iran in 2023?",
    "What is the current official position regarding Iran?",
    "Was immigration policy stricter in 2025 than in 2023?",
    "How did climate policy rhetoric change between the earliest and latest documents?",
]
def append_chunks_to_csv(
    corpus,
    chunking_method,
    embedding_method,
    query,
    period,
    chunks
):
    with open(CSV_OUT, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for rank, c in enumerate(chunks, start=1):
            writer.writerow([
                corpus,
                chunking_method,
                embedding_method,
                query,
                period,
                rank,
                c["id"],
                c["timestamp_iso"],
                c["text_preview"]
            ])

# -------------------------
# Load index
# -------------------------
with open(INDEX_PATH, "r", encoding="utf-8") as f:
    ALL_CHUNKS = json.load(f)
write_csv_header()


print(f"Loaded {len(ALL_CHUNKS)} chunks")
log(f"Loaded {len(ALL_CHUNKS)} chunks")

# -------------------------
# Run Stage 4
# -------------------------
for corpus in CORPORA:
    print("\n" + "=" * 80)
    print(f"CORPUS: {corpus}")
    print("=" * 80)

    log("\n" + "=" * 80)
    log(f"CORPUS: {corpus}")
    log("=" * 80)

    corpus_chunks = [c for c in ALL_CHUNKS if is_corpus(c, corpus)]
    log(f"[DEBUG] corpus_chunks size: {len(corpus_chunks)}")

    for chunking_method, embedding_method in SYSTEMS:
        print("\n" + "-" * 80)
        print(f"SYSTEM: {chunking_method} + {embedding_method}")
        print("-" * 80)

        log("\n" + "-" * 80)
        log(f"SYSTEM: {chunking_method} + {embedding_method}")
        log("-" * 80)

        system_chunks = [
            c for c in corpus_chunks
            if c["chunking_method"] == chunking_method
            and c["embedding_method"] == embedding_method
        ]

        log(f"[DEBUG] system_chunks size: {len(system_chunks)}")

        for query in QUERIES:
            print("\nQUERY:", query)
            log(f"\nQUERY: {query}")

            early, late = temporal_retrieve(
                query,
                system_chunks,
                retrieve_eval,
                k=K,
                months=MONTHS,
                chunking_method=chunking_method,
                embedding_method=embedding_method,
            )
            append_chunks_to_csv(
                corpus,
                chunking_method,
                embedding_method,
                query,
                "EARLY",
                early
            )

            append_chunks_to_csv(
                corpus,
                chunking_method,
                embedding_method,
                query,
                "LATE",
                late
            )

            print("\nEARLY (old → new):")
            log_chunks("EARLY (old → new):", early)

            for c in early:
                print(f"- {c['timestamp_iso']} | {c['text_preview'][:120]}")

            print("\nLATE (new → old):")
            log_chunks("LATE (new → old):", late)

            for c in late:
                print(f"- {c['timestamp_iso']} | {c['text_preview'][:120]}")

            # -------- Optional LLM synthesis --------
            early_objs = [Chunk(c["id"], c["text_preview"]) for c in early]
            late_objs  = [Chunk(c["id"], c["text_preview"]) for c in late]

            answer = run_evolution_llm(query, early_objs, late_objs)

            print("\nEVOLUTION ANSWER:")
            print(answer)

            log("\nEVOLUTION ANSWER:")
            log(answer if answer else "[NO ANSWER]")
