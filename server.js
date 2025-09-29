# api.py
from fastapi import FastAPI, Query
import mysql.connector
import json
from typing import Optional

app = FastAPI()

DB_CONF = {
    "host": "localhost",
    "user": "myuser",
    "password": "mypassword",
    "database": "mydb",
    "charset": "utf8mb4",
    "use_unicode": True
}

def get_conn():
    return mysql.connector.connect(**DB_CONF)

@app.get("/api/keypoints/{word}")
def get_keypoints(word: str, frame: Optional[int] = None):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    if frame is not None:
        cur.execute(
            "SELECT frame_number, keypoints FROM words WHERE word = %s AND frame_number = %s",
            (word, frame)
        )
    else:
        cur.execute(
            "SELECT frame_number, keypoints FROM words WHERE word = %s ORDER BY frame_number",
            (word,)
        )
    
    rows = cur.fetchall()
    cur.close()
    conn.close()

    for r in rows:
        r["keypoints"] = json.loads(r["keypoints"])
    
    return rows
