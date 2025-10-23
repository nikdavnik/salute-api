import os
import json
import mysql.connector
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from dotenv import load_dotenv
import time # <-- New import for time-based cache

load_dotenv()
app = FastAPI()

# ✅ Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or ["http://localhost:8080"] for stricter control
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CACHING CONFIGURATION ---
# Stores keypoints for words (where frame is None).
# Format: { "word": {"timestamp": 1678886400.0, "data": [...] } }
WORD_CACHE = {}
CACHE_TIMEOUT_SECONDS = 86400 # Cache entries expire after 3 seconds

# Database configuration
DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "myuser"),
    "password": os.getenv("DB_PASSWORD", "mypassword"),
    "database": os.getenv("DB_NAME", "mydb"),
    "port": os.getenv("DB_PORT", "3306"),
    "charset": "utf8mb4",
    "use_unicode": True,
}

# API key
API_KEY = os.getenv("API_KEY", "changeme")

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

def get_conn():
    return mysql.connector.connect(**DB_CONF)

@app.get("/api/keypoints/{word}")
def get_keypoints(
    word: str,
    frame: Optional[int] = None,
    _: None = Depends(verify_api_key),
):
    # --- 1. CACHE LOOKUP ---
    # Only cache requests that retrieve ALL frames for a word (frame is None)
    if frame is None:
        cache_entry = WORD_CACHE.get(word)
        current_time = time.time()
        
        if cache_entry and (current_time - cache_entry["timestamp"] < CACHE_TIMEOUT_SECONDS):
            print(f"✅ Cache Hit: Serving '{word}' from memory.")
            return cache_entry["data"]
        
        print(f"🟡 Cache Miss/Expired: Querying database for '{word}'.")
        # Proceed to DB query if not in cache or expired
        
    # --- 2. DATABASE QUERY ---
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    if frame is not None:
        # If a specific frame is requested, query the DB directly (do not cache this result)
        cur.execute(
            "SELECT frame_number, keypoints FROM words WHERE word = %s AND frame_number = %s",
            (word, frame),
        )
    else:
        # Fetch all frames (This is the cacheable request)
        cur.execute(
            "SELECT frame_number, keypoints FROM words WHERE word = %s ORDER BY frame_number",
            (word,),
        )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # --- 3. POST-PROCESSING ---
    # Convert JSON strings back into Python objects
    for r in rows:
        r["keypoints"] = json.loads(r["keypoints"])

    # --- 4. CACHE STORE ---
    # If we successfully fetched all frames (frame is None), store the result in the cache
    if frame is None and rows:
        WORD_CACHE[word] = {
            "timestamp": time.time(),
            "data": rows
        }
        print(f"💾 Cache Stored: '{word}' added/updated.")

    return rows
