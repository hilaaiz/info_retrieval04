import requests

API_KEY = "PASTE_ONE_KEY_HERE"  # better: use env var

url = f"https://generativelanguage.googleapis.com/v1beta/models?key={API_KEY}"
r = requests.get(url)
r.raise_for_status()
data = r.json()

for m in data.get("models", []):
    name = m.get("name", "")
    methods = m.get("supportedGenerationMethods", [])
    if "generateContent" in methods:
        print(name, "->", methods)
