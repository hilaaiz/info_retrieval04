import os
import spacy

###############################################
# Load optimized spaCy model
###############################################

nlp = spacy.load(
    "en_core_web_sm",
    disable=["ner", "parser", "tagger", "lemmatizer"]
)

# Sentence splitter only
nlp.add_pipe("sentencizer")

# Allow large files
nlp.max_length = 3_000_000


###############################################
# Utility functions
###############################################

def split_to_sentences(text):
    doc = nlp(text)
    return [sent.text.strip() for sent in doc.sents if sent.text.strip()]


def count_words(sentence):
    return len(sentence.split())


###############################################
# Chunking Method 1 (fixed size + overlap)
###############################################

def chunk_fixed_overlap(text, max_words_per_chunk=660, overlap_sentences=3):
    print("  Splitting sentences...")
    sentences = split_to_sentences(text)
    print(f"  → {len(sentences)} sentences")

    chunks = []
    i = 0
    n = len(sentences)

    while i < n:
        prev_i = i  # 🔥 progress guard

        # Build overlap from previous chunk
        if chunks:
            prev_chunk = chunks[-1]
            overlap = (
                prev_chunk[-overlap_sentences:]
                if len(prev_chunk) >= overlap_sentences
                else prev_chunk
            )
        else:
            overlap = []

        current_chunk = overlap.copy()
        current_word_count = sum(count_words(s) for s in current_chunk)

        # Trim overlap if too large
        while current_word_count > max_words_per_chunk and len(current_chunk) > 1:
            removed = current_chunk.pop(0)
            current_word_count -= count_words(removed)

        # Fill chunk
        while i < n:
            s = sentences[i]
            w = count_words(s)

            if w > max_words_per_chunk:
                if not current_chunk:
                    current_chunk.append(s)
                    i += 1
                break

            if current_word_count + w <= max_words_per_chunk:
                current_chunk.append(s)
                current_word_count += w
                i += 1
            else:
                break

        # 🔥 HARD SAFETY — GUARANTEE PROGRESS
        if i == prev_i:
            current_chunk.append(sentences[i])
            i += 1

        chunks.append(current_chunk)

    return chunks


###############################################
# Main processing loop
###############################################

if __name__ == "__main__":
    input_folder = "clean_txt"
    output_folder = "chunks_660_output"

    os.makedirs(output_folder, exist_ok=True)

    global_chunk_id = 1

    for filename in sorted(os.listdir(input_folder)):
        if not filename.lower().endswith((".txt", ".md")):
            continue

        print("Processing:", filename)

        file_path = os.path.join(input_folder, filename)
        with open(file_path, "r", encoding="utf8") as f:
            text = f.read()

        # Safety guard
        if len(text) > 2_900_000:
            print(f"  ⚠️ Skipped (too large): {filename}")
            continue
        
        print("  Chunking...")

        chunks = chunk_fixed_overlap(text)
        print(f"  → {len(chunks)} chunks created")

        file_output_dir = os.path.join(output_folder, f"{filename}_chunks")
        os.makedirs(file_output_dir, exist_ok=True)

        base_name = os.path.splitext(filename)[0]

        for idx, chunk in enumerate(chunks, start=1):
            chunk_text = "\n".join(chunk)
            chunk_filename = f"{base_name}_chunk_{idx}.txt"
            chunk_path = os.path.join(file_output_dir, chunk_filename)

            with open(chunk_path, "w", encoding="utf8") as out:
                out.write(chunk_text)


            global_chunk_id += 1

    print("\nAll done.")
