import os
import re
import spacy

nlp = spacy.load("en_core_web_sm")


# --------------------------------------------------
# Split text into sentences
# --------------------------------------------------

def split_sentences(text):
    doc = nlp(text)
    return [s.text.strip() for s in doc.sents if s.text.strip()]


# --------------------------------------------------
# Hierarchical chunking
# --------------------------------------------------

def hierarchical_chunk(text):
    """
    Correct hierarchical chunking for Congressional Record:
    1. Detect ALL-CAPS headings → split into sections
    2. Split each section into paragraphs
    3. Split each paragraph into sentences
    """

    # 1. Detect ALL-CAPS HEADINGS
    heading_re = r"(?m)^(?=[A-Z][A-Z0-9 ,.'’\-]{8,})"

    # Split but KEEP each heading
    parts = re.split(heading_re, text)
    parts = [p.strip() for p in parts if p.strip()]

    chunks = []

    for section in parts:
        # 2. Split paragraphs
        paragraphs = [p.strip() for p in section.split("\n\n") if p.strip()]

        for para in paragraphs:
            sentences = split_sentences(para)
            if not sentences:
                continue

            chunk_text = "\n".join(sentences)

            # Skip useless chunks: dates, "of california", "in the house..."
            if len(chunk_text.split()) < 6:  # too small = metadata
                continue

            chunks.append(chunk_text)

    return chunks


# --------------------------------------------------
# Save chunks
# --------------------------------------------------

def save_chunks(chunks, output_dir, base_filename):
    os.makedirs(output_dir, exist_ok=True)

    for idx, chunk in enumerate(chunks, start=1):
        out_path = os.path.join(output_dir, f"{base_filename}_chunk_{idx}.txt")
        with open(out_path, "w", encoding="utf8") as f:
            f.write(chunk)


# --------------------------------------------------
# Runner
# --------------------------------------------------

def run_chunker(input_folder="US_cleaned_text", output_folder="hierarchical_chunks"):

    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(input_folder):
        if not filename.endswith(".txt"):
            continue

        print(f"Processing: {filename}")
        path = os.path.join(input_folder, filename)

        with open(path, "r", encoding="utf8") as f:
            text = f.read()

        chunks = hierarchical_chunk(text)
        print(f"  → {len(chunks)} chunks created")

        save_chunks(
            chunks,
            output_dir=os.path.join(output_folder, filename + "_chunks"),
            base_filename=filename.replace(".txt", "")
        )


if __name__ == "__main__":
    run_chunker()
