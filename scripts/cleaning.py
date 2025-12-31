import os
import re
from pathlib import Path
from nltk.corpus import stopwords
import nltk
import shutil 
import string 

# -------------------------------------------------------------
# PATH SETUP
# -------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent 
ROOT_DIR = BASE_DIR.parent 

INPUT_FOLDER = ROOT_DIR / "UK_british_debates_text_files_normalize" 
OUTPUT_FOLDER = ROOT_DIR / "allData_punc_cleaned" 

OUTPUT_FOLDER.mkdir(exist_ok=True)


# -------------------------------------------------------------
# PRE-REQUISITES (הורדת משאבי NLTK)
# -------------------------------------------------------------
try:
    nltk.data.find('corpora/stopwords')
except LookupError: 
    print("Downloading nltk stopwords resource...")
    nltk.download('stopwords', quiet=True)
except Exception as e:
    print(f"Warning: Failed to download nltk stopwords, may affect cleanup quality: {e}")


# -------------------------------------------------------------
# רשימות מילים להסרה
# -------------------------------------------------------------
ENGLISH_STOPWORDS = set(stopwords.words('english'))

REVEALING_WORDS = {
    'uk', 'us', 'usa', 'united', 'kingdom', 'states', 'britain', 
    'america', 'congress', 'parliament', 'uks', 'uss', 
    'mr', 'ms', 'mrs', 'speaker', 'hon', 'honorable', 'sir', 
    'doctor', 'deputy', 'superintendent', 'charles'
}

ALL_WORDS_TO_REMOVE = ENGLISH_STOPWORDS.union(REVEALING_WORDS)
print(f"Loaded {len(ALL_WORDS_TO_REMOVE)} total words for semantic removal.")


# -------------------------------------------------------------
# פונקציית הניקוי החדשה (שומרת פיסוק + מטפלת בגרש קניין)
# -------------------------------------------------------------
def perform_enhanced_cleanup_preserve_punc(text):
    """
    מבצע ניקוי חזק, מטפל בגרש קניין (כולל מעוקל), ושומר על כל סימני הפיסוק.
    """
    
    # 1. המרה לאותיות קטנות
    text = text.lower()
    
    # 2. **תיקון קריטי: טיפול בגרש קניין (כולל מעוקל)**
    # מפריד את 's או s' (כמו uk's, uk’s) מהמילה, והופך אותם ל-tokens נפרדים.
    # המטרה היא להשאיר את 'uk' לבד כדי שיסונן על ידי ALL_WORDS_TO_REMOVE.
    
    # מפריד גרשים מעוקלים וישרים לפני s: 's
    text = re.sub(r"([a-z]+)['’]s", r"\1 's", text) 
    # מפריד גרשים ישרים ומעוקלים אחרונים
    text = re.sub(r"([a-z]+)s['’]", r"\1s '", text)
    
    # 3. הפרדת סימני פיסוק מהמילים
    # מוסיף רווח לפני ואחרי כל סימן פיסוק (כולל נקודות, פסיקים וכו')
    # נשתמש ב-string.punctuation כדי לתפוס את הגרש הישר והמעוקל (אם שרדו)
    text = re.sub(r'([%s])' % re.escape(string.punctuation), r' \1 ', text)
    
    # 4. הסרת נקודות שעדיין חושפות קלאס (כמו m.p.s) - הופך אותן לרווחים
    text = re.sub(r'\s*([a-z]\s*\.\s*){2,}', ' ', text)
    
    # 5. פיצול לטוקנים, סינון מילים (השארת פיסוק)
    tokens = text.split()
    
    filtered_tokens = []
    
    for word in tokens:
        # סינון: שמור רק מילים שאינן ברשימת ההסרה ושאינן קצרות מדי
        if word in string.punctuation or word == "'s" or (word not in ALL_WORDS_TO_REMOVE and len(word) > 1):
            filtered_tokens.append(word)

    # 6. חיבור מחדש
    cleaned_text = ' '.join(filtered_tokens)
    
    # 7. ניקוי רווחים מיותרים (הסרת רווח לפני סימני פיסוק ושורות ריקות מרובות)
    cleaned_text = re.sub(r'\s([%s])' % re.escape(string.punctuation), r'\1', cleaned_text)
    cleaned_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', cleaned_text)
    
    return cleaned_text.strip()


# -------------------------------------------------------------
# עיבוד הקבצים
# -------------------------------------------------------------
print(f"\n=== Starting Enhanced Cleanup from {INPUT_FOLDER.name} to {OUTPUT_FOLDER.name} ===")
processed_count = 0

for filename in os.listdir(INPUT_FOLDER):
    if filename.endswith('.txt'):
        input_path = INPUT_FOLDER / filename
        output_path = OUTPUT_FOLDER / filename
        
        try:
            with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
                raw_text = f.read()
            
            cleaned_text = perform_enhanced_cleanup_preserve_punc(raw_text)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_text)
            
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            
print(f"\n✅ Enhanced cleanup complete. {processed_count} files processed and saved to {OUTPUT_FOLDER.name}.")