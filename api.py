import os
import json
import mysql.connector
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from dotenv import load_dotenv

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
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    if frame is not None:
        cur.execute(
            "SELECT frame_number, keypoints FROM words WHERE word = %s AND frame_number = %s",
            (word, frame),
        )
    else:
        cur.execute(
            "SELECT frame_number, keypoints FROM words WHERE word = %s ORDER BY frame_number",
            (word,),
        )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    for r in rows:
        r["keypoints"] = json.loads(r["keypoints"])

    return rows
