# run_experiments.py
import csv
from datetime import datetime

from scripts.retrieval.retriever import retrieve
from scripts.generator import generate_answer

# === CONFIG ===
K_VALUES = [3, 5, 10]  # k1, k2, k3
CHUNKING_TYPES = ["fixed_660", "hierarchical"]
METHODS = ["bm25", "dense"]

OUTPUT_CSV = "experiment_results.csv"


def _adapt_chunks_for_generator(retrieved_chunks):
    """
    generator.py expects each chunk to contain:
    - 'text'
    - 'file_name' (for attribution)
    We'll set file_name=doc_id (original document id).
    """
    chunks_for_gen = []
    for c in retrieved_chunks:
        c2 = dict(c)
        c2["file_name"] = c.get("doc_id", "UNKNOWN")
        chunks_for_gen.append(c2)
    return chunks_for_gen


def _chunks_preview(chunks_for_gen, max_chars_per_chunk=300):
    parts = []
    for c in chunks_for_gen:
        preview = (c.get("text", "")[:max_chars_per_chunk]).replace("\n", " ").strip()
        parts.append(f"[{c.get('file_name','UNKNOWN')} | {c.get('chunk_id','?')}] {preview}")
    return "\n".join(parts)


def run_full_experiment_suite(queries, output_csv=OUTPUT_CSV):
    fieldnames = [
        "Timestamp",
        "Query",
        "Chunking",
        "Method",
        "K",
        "Num_Chunks",
        "Doc_IDs",         # original docs
        "Source_Paths",    # actual chunk file paths
        "Chunks_Preview",
        "Generated_Answer",
    ]

    with open(output_csv, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for query in queries:
            for chunking in CHUNKING_TYPES:
                for method in METHODS:
                    for k in K_VALUES:

                        # 1) Retrieval
                        retrieved = retrieve(query=query, method=method, chunking_type=chunking, k=k)

                        # 2) Adapt to generator format
                        chunks_for_gen = _adapt_chunks_for_generator(retrieved)

                        doc_ids = ", ".join(sorted(set(c.get("doc_id", "UNKNOWN") for c in retrieved)))
                        source_paths = ", ".join(sorted(set(c.get("source_path", "UNKNOWN") for c in retrieved)))

                        preview = _chunks_preview(chunks_for_gen)

                        # 3) Generation
                        try:
                            answer = generate_answer(query, chunks_for_gen)
                        except Exception as e:
                            answer = f"GENERATION_ERROR: {type(e).__name__}: {e}"

                        # 4) Save row
                        writer.writerow({
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Query": query,
                            "Chunking": chunking,
                            "Method": method,
                            "K": k,
                            "Num_Chunks": len(chunks_for_gen),
                            "Doc_IDs": doc_ids,
                            "Source_Paths": source_paths,
                            "Chunks_Preview": preview,
                            "Generated_Answer": answer,
                        })

                        print(f"✅ {chunking} | {method} | k={k} | {query[:60]}")

    print(f"\n🎉 Done! Results saved to: {output_csv}")


if __name__ == "__main__":
    queries = [
        # Factual (4)
       # "On what dates did the British Prime Minister deliver his speech on the defense budget?",
        #"What was the main argument regarding the immigration bill that was presented?",
        #"What three industrial sectors were mentioned as the main victims of the new trade policy that was presented?",
       # "What organizations were mentioned by the speakers as supporting the proposed reform of the health system?",

        # Conceptual (4)
       # "How does the rhetoric on climate change vary between different speakers; is the emphasis on economic opportunity or existential crisis?",
       # "What is the central tension that emerges from the speeches between the need for national security and the protection of citizens’ privacy in the digital age?",
       # "How is the state’s moral responsibility towards refugees and asylum seekers described, and what are the ethical (rather than economic) arguments given for and against their absorption?",
       # "In what ways did speakers link investment in education to reducing future crime, and was there consensus on this issue?",
       
       # --- שאילתות מהתרגיל (עברית ואנגלית) ---
        "מה היה התקציב הספציפי שהוקצה לביטחון בשנת 2024?", #[cite: 44]
        "What was the specific budget allocated to security in 2024?", #[cite: 45]
        "מהי העמדה הרשמית הנוכחית לגבי מדינת ישראל?", #[cite: 48]
        "What is the current official position regarding the State of Israel?", #[cite: 53]
        "Who is the Minister of Defense/Secretary of Defense?", #[cite: 65]
        "כיצד השתנתה הרטוריקה בנוגע למלחמה בין נאומו הראשון לאחרון?", #[cite: 60]
        
        # --- שאילתות נוספות ---
        "Who is the current Chancellor of the Exchequer / Secretary of the Treasury?", # קונפליקט
        "What was the exact unemployment rate mentioned in the government report from 2018?", # נקודה בזמן
        "What is the most recent policy update regarding carbon emission targets?", # סטטוס עדכני
        "How has the government's stance on digital transformation evolved between 2019 and 2024?" # אבולוציה
    ]

    run_full_experiment_suite(queries)
