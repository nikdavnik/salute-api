import os
import json
import mysql.connector
from fastapi import FastAPI, Depends, HTTPException, Header
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

# Database configuration from env
DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "myuser"),
    "password": os.getenv("DB_PASSWORD", "mypassword"),
    "database": os.getenv("DB_NAME", "mydb"),
    "port": os.getenv("DB_PORT", "3306"),
    "charset": "utf8mb4",
    "use_unicode": True,
}

# API Key from env
API_KEY = os.getenv("API_KEY", "changeme")  # set in .env or App Platform settings

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

def get_conn():
    return mysql.connector.connect(**DB_CONF)

@app.get("/api/keypoints/{word}")
def get_keypoints(
    word: str,
    frame: Optional[int] = None,
    _: None = Depends(verify_api_key),  # enforce API key
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
