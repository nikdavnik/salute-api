import os
import json
import mysql.connector
from fastapi import FastAPI
from typing import Optional

app = FastAPI()

from dotenv import load_dotenv
load_dotenv()

DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "myuser"),
    "password": os.getenv("DB_PASSWORD", "mypassword"),
    "database": os.getenv("DB_NAME", "mydb"),
    "port": os.getenv("DB_PORT", "3306"),
    "charset": "utf8mb4",
    "use_unicode": True,
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
