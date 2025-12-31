import requests
import time
import sys

# הגדרת קידוד למניעת שגיאות בטרמינל
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# רשימת 10 המפתחות שלך
API_KEYS = [
    "AIzaSyD6MpbWm_P9uvS2jyvh2_9HsgaHs9J4_y0", "AIzaSyCG3p4SsxcnAsSCXNrwvw-XrEeb1w17eSQ", "AIzaSyBQpHRIHwCiBz9UXzwQfaSOSi1KSxxxS2g", "AIzaSyBa1zk3VaYLne78e7LXPRr5LUIHzD2kRJw", "AIzaSyC8SmXGheHNGqXsPRANbpk8RfUZP9s4gEA",
    "AIzaSyCmSBioz03yeRc0WtWc1jPX1z88zEpd8Nc", "AIzaSyBjsBQp2vEWxAmMwC5MYPYGWEhOK8pnJhM", "AIzaSyBwTc599DUcfGENp6N03lg0m8em3EfIuvE", "AIzaSyDIKJAh9OdB06uGh0wbQWY18yr3byOMD8M", "AIzaSyCH40QbD8mbZY2x4k5fIB49ZV9cRfe3GUs",
    "AIzaSyBGcV84OCbb3LQcFRpqMlLwS-Cij1kDVt4","AIzaSyBJAd4qufgITJoJ6T62AmYvRrE6QED9psI"
]

# המשתנה שזוכר איפה עצרנו (נשמר בין קריאה לקריאה)
current_key_index = 0

def generate_answer(query, retrieved_chunks):
    global current_key_index
    
    # 1. בניית הקונטקסט (נשמר בדיוק כפי שהיה)
    context_text = ""
    for chunk in retrieved_chunks:
        context_text += f"\n--- Source File: {chunk.get('file_name', 'unknown')} ---\n"
        context_text += chunk.get('text', '') + "\n"

    # 2. הפרומפט המקורי והחזק שלנו (הבטחת איכות)
    system_prompt = (
        "You are an expert information extraction assistant. "
        "Your task is to answer the user's question based ONLY on the provided document chunks. "
        "CRITICAL RULES:\n"
        "1. Zero External Knowledge: Do not use any information that is not explicitly mentioned in the provided chunks.\n"
        "2. Handling Missing Info: If the provided chunks do not contain enough information to answer the question, "
        "state: 'I cannot answer this question based on the provided documents'.\n"
        "3. Source Attribution: For every fact you state, you must mention the source file name.\n"
        "4. Handling Noise: If some chunks are irrelevant to the query, ignore them.\n"
        "5. Consistency: If different chunks provide conflicting information, report both views."
    )

    # 3. לוגיקת הניסיונות עם ניצול מפתחות סדרתי
    retries = 15 
    for i in range(retries):
        if current_key_index >= len(API_KEYS):
            return "Error: All API keys in the pool have been exhausted."

        current_key = API_KEYS[current_key_index]
        
        # כתובת ה-URL המדויקת לגרסה 2.5 פריוויו
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={current_key}"

        # ה-Payload המקורי (שמפריד בין הנחיות מערכת לתוכן)
        payload = {
            "contents": [
                {
                    "parts": [{"text": f"Context:\n{context_text}\n\nQuestion: {query}"}]
                }
            ],
            "systemInstruction": {
                "parts": [{"text": system_prompt}]
            },
            "generationConfig": {
                "temperature": 0
            }
        }

        try:
            response = requests.post(url, json=payload)

            # אם הצלחנו - מעולה! מחזירים את התשובה ונשארים עם אותו מפתח לפעם הבאה
            if response.status_code == 200:
                result = response.json()
                return result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', "No response.")

            # אם נגמרה המכסה (429) או שהמפתח נחסם (403)
            if response.status_code in [429, 403]:
                print(f"Key {current_key_index + 1} hit a limit (Status {response.status_code}). Switching to next key...")
                current_key_index += 1 # "שורפים" את המפתח ועוברים לבא
                
                # המתנה קצרה של Exponential Backoff רק כדי לא להציף את המפתח החדש מיד
                wait_time = min(2**i, 10) 
                time.sleep(wait_time)
                continue 
            
            else:
                return f"Error {response.status_code}: {response.text}"

        except Exception as e:
            time.sleep(1)
            continue

    return "Failed to get response after trying to exhaust keys."

if __name__ == "__main__":
    # בדיקה ידנית
    mock_chunks = [{"text": "Speech on July 3rd about defense budget.", "file_name": "uk_2023.txt"}]
    print("--- Running Combined Logic & Prompt Test ---")
    print(generate_answer("What was the speech about?", mock_chunks))
