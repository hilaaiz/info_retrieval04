import json
from datetime import datetime
import re
import csv


# We reuse your evaluation retriever (keyword overlap)
from scripts.retrieval.retriever import retrieve_eval  # used in run_temporal_queries.py :contentReference[oaicite:2]{index=2}


CSV_OUT = "stage3_comparison_results.csv"

def write_csv_header():
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "corpus",
            "chunking_method",
            "embedding_method",
            "query",
            "chunk_id",
            "timestamp_iso",
            "baseline_rank",
            "baseline_score",
            "temporal_rank",
            "temporal_score",
            "alpha",
            "lambda"
        ])
def append_rows_to_csv(corpus, chunking_method, embedding_method, query, rows, alpha, lambd):
    with open(CSV_OUT, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for r in rows:
            writer.writerow([
                corpus,
                chunking_method,
                embedding_method,
                query,
                r["chunk_id"],
                r["timestamp_iso"],
                r["baseline_rank"],
                r["baseline_score"],
                r["temporal_rank"],
                r["temporal_score"],
                alpha,
                lambd
            ])

INDEX_PATH = "stage2_outputs/temporal_index_stage2.json"  # created by temporalIndexing.py :contentReference[oaicite:3]{index=3}

CORPORA = ["UK", "US"]
SYSTEMS = [
    ("fixed_660", "bm25"),
    ("fixed_660", "dense_e5_base"),
    ("hierarchical", "bm25"),
    ("hierarchical", "dense_e5_base"),
]

K = 5

# -------- Stage 3 parameters --------
ALPHA = 0.3
LAMBDA = 0.5


def is_corpus(chunk, corpus):
    # same idea as your run_temporal_queries.py :contentReference[oaicite:4]{index=4}
    return chunk["source"].lower().startswith(corpus.lower() + "_")


def extract_year_from_query(query: str):
    m = re.search(r"\b(19|20)\d{2}\b", query)
    return int(m.group()) if m else None


def parse_chunk_dt(chunk):
    # stage2 stores ISO string like "2023-07-03" :contentReference[oaicite:5]{index=5}
    ts = chunk.get("timestamp_iso")
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def time_decay_score(sim_score: float, chunk_dt: datetime, query_dt: datetime,
                     alpha: float = ALPHA, lambd: float = LAMBDA) -> float:
    # Rational decay: 1 / (1 + lambda * Δt), with Δt in years
    delta_years = abs((query_dt - chunk_dt).days) / 365.25
    time_score = 1 / (1 + lambd * delta_years)
    return (1 - alpha) * sim_score + alpha * time_score


def score_baseline(query: str, chunks: list):
    # retrieve_eval returns top-k already, but Stage 3 needs scoring over a candidate set.
    # We'll score all chunks using the same signal as retrieve_eval:
    # count of query terms appearing in text_preview.
    query_terms = set(re.findall(r"\w+", query.lower()))
    scored = []
    for c in chunks:
        text = (c.get("text_preview") or "").lower()
        s = sum(1 for t in query_terms if t in text)
        if s > 0:
            scored.append((c, float(s)))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def apply_hard_year_filter(scored, year):
    if year is None:
        return scored
    out = []
    for c, s in scored:
        ts = c.get("timestamp_iso") or ""
        if ts.startswith(str(year)):
            out.append((c, s))
    return out


def topk_comparison_table(query: str, chunks: list, k: int = K):
    query_dt = datetime.now()
    year = extract_year_from_query(query)

    # 1) baseline scoring (semantic only proxy)
    baseline_scored = score_baseline(query, chunks)
    baseline_scored = apply_hard_year_filter(baseline_scored, year)

    # 2) temporal scoring (semantic + time decay)
    temporal_scored = []
    for c, s in baseline_scored:
        dt = parse_chunk_dt(c)
        if dt is None:
            final = s  # no timestamp -> no decay
        else:
            final = time_decay_score(s, dt, query_dt)
        temporal_scored.append((c, s, final))
    temporal_scored.sort(key=lambda x: x[2], reverse=True)

    # 3) take top-k
    baseline_top = baseline_scored[:k]
    temporal_top = temporal_scored[:k]

    # 4) build comparison rows by chunk id
    rows = {}
    for i, (c, s) in enumerate(baseline_top, start=1):
        cid = c["id"]
        rows[cid] = {
            "chunk_id": cid,
            "timestamp_iso": c.get("timestamp_iso"),
            "baseline_rank": i,
            "baseline_score": s,
            "temporal_rank": None,
            "temporal_score": None,
        }

    for i, (c, s, final) in enumerate(temporal_top, start=1):
        cid = c["id"]
        if cid not in rows:
            rows[cid] = {
                "chunk_id": cid,
                "timestamp_iso": c.get("timestamp_iso"),
                "baseline_rank": None,
                "baseline_score": None,
                "temporal_rank": i,
                "temporal_score": final,
            }
        else:
            rows[cid]["temporal_rank"] = i
            rows[cid]["temporal_score"] = final

    # return stable ordering: baseline rank first, then temporal-only rows
    def sort_key(r):
        br = r["baseline_rank"]
        tr = r["temporal_rank"]
        return (br if br is not None else 999, tr if tr is not None else 999)

    return sorted(rows.values(), key=sort_key)


def print_table(title, rows):
    print(title)
    print("chunk_id | timestamp | baseline_rank | baseline_score | temporal_rank | temporal_score")
    for r in rows:
        print(f"{r['chunk_id']} | {r['timestamp_iso']} | {r['baseline_rank']} | "
              f"{r['baseline_score']} | {r['temporal_rank']} | {r['temporal_score']}")


def main():
    with open(INDEX_PATH, "r", encoding="utf-8") as f:
        all_chunks = json.load(f)

    # Pick 1–2 queries to show in the report (you can add more)
    queries = [
        "What is the current official position regarding Hamas/Gaza?",
        "What was the specific budget allocated to security in 2024?",
    ]

    write_csv_header()

    for corpus in CORPORA:
        corpus_chunks = [c for c in all_chunks if is_corpus(c, corpus)]

        for chunking_method, embedding_method in SYSTEMS:
            system_chunks = [
                c for c in corpus_chunks
                if c["chunking_method"] == chunking_method
                and c["embedding_method"] == embedding_method
            ]

            print("\n" + "=" * 80)
            print(f"CORPUS={corpus} | SYSTEM={chunking_method}+{embedding_method} | alpha={ALPHA} lambda={LAMBDA}")
            print("=" * 80)

            for q in queries:
                rows = topk_comparison_table(q, system_chunks, k=K)

                append_rows_to_csv(
                    corpus=corpus,
                    chunking_method=chunking_method,
                    embedding_method=embedding_method,
                    query=q,
                    rows=rows,
                    alpha=ALPHA,
                    lambd=LAMBDA
                )



if __name__ == "__main__":
    main()
