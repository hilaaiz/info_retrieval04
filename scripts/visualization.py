import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# 1. טעינת הנתונים
# וודא שהקובץ experiment_results.csv נמצא באותה תיקייה
df = pd.read_csv("experiment_results.csv")

# --- הכנת נתונים (Preprocessing) ---

# א. חישוב אורך תשובה במילים
df['Answer_Word_Count'] = df['Generated_Answer'].apply(lambda x: len(str(x).split()))

# ב. זיהוי "אי-מענה" (Failure Rate)
# מחפשים ביטויים של חוסר מידע בתשובה
failure_keywords = ["cannot answer", "not mentioned", "no information", "not found"]
df['Is_Failure'] = df['Generated_Answer'].str.lower().apply(
    lambda x: any(k in str(x) for k in failure_keywords)
)

# ג. יצירת עמודת זמן (Latency) - אם אין עמודה כזו, ניצור נתונים דמויים להמחשה
# (בדו"ח האמיתי תשתמש בעמודת הזמן שתיעדת ב-Timestamp או שתמדוד זמן ריצה)
if 'Latency' not in df.columns:
    # יצירת ערכים רנדומליים הגיוניים: Dense איטי יותר מ-BM25, ו-K גדול מאט את ה-LLM
    df['Latency'] = np.random.uniform(1.5, 3.0, len(df))
    df.loc[df['Method'] == 'dense', 'Latency'] += 1.2
    df.loc[df['K'] == 10, 'Latency'] += 2.0

# --- יצירת הויזואליזציות ---

plt.figure(figsize=(20, 15))
sns.set_theme(style="whitegrid")

# תרשים 1: השוואת זמני תגובה (Latency)
plt.subplot(2, 2, 1)
sns.barplot(data=df, x='K', y='Latency', hue='Method', palette='muted')
plt.title('1. Average Latency: BM25 vs Dense (by K)')
plt.ylabel('Time (Seconds)')

# תרשים 2: אחוזי "אי-מענה" (Failure Rate) לפי Chunking
plt.subplot(2, 2, 2)
failure_counts = df.groupby(['Chunking', 'Is_Failure']).size().unstack().fillna(0)
failure_counts_pct = failure_counts.div(failure_counts.sum(axis=1), axis=0) * 100
failure_counts_pct.plot(kind='bar', stacked=True, ax=plt.gca(), color=['#4CAF50', '#F44336'])
plt.title('2. Success vs Failure Rate by Chunking Type')
plt.ylabel('Percentage (%)')
plt.legend(['Success', 'Failure'], loc='upper right')

# תרשים 3: מפת חום (Heatmap) של צפיפות מסמכים
plt.subplot(2, 2, 3)
# ניקח את ה-Doc_ID הראשון שמופיע בכל שורה כדי לפשט את המפה
df['Primary_Doc'] = df['Doc_IDs'].apply(lambda x: str(x).split(',')[0].strip())
# ניצור מטריצה של שאילתות מול מסמכים
ct = pd.crosstab(df['Query'].str[:30], df['Primary_Doc']) # קיצור השאילתה לתצוגה
sns.heatmap(ct, annot=True, cmap="YlGnBu", cbar=True)
plt.title('3. Document Retrieval Density Heatmap')
plt.xlabel('Original Document ID')
plt.ylabel('Query (Truncated)')

# תרשים 4: השפעת K על אורך התשובה
plt.subplot(2, 2, 4)
sns.regplot(data=df, x='K', y='Answer_Word_Count', scatter_kws={'alpha':0.5}, line_kws={'color':'red'})
sns.boxplot(data=df, x='K', y='Answer_Word_Count', palette='Set3', ax=plt.gca(), boxprops=dict(alpha=.3))
plt.title('4. Impact of K on Answer Verbosity (Word Count)')
plt.ylabel('Number of Words')

plt.tight_layout()
plt.savefig('rag_full_analysis.png', dpi=300)
plt.show()

