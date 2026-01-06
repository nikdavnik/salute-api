import os
import time
import json
import gzip
from typing import Optional, List, Any, Dict

from fastapi import FastAPI, Depends, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response
from dotenv import load_dotenv

import mysql.connector
from mysql.connector import pooling

# Try to use orjson for speed; fall back to built-in json
try:
    import orjson  # type: ignore
    _HAS_ORJSON = True
except Exception:
    _HAS_ORJSON = False

load_dotenv("valkey.env")

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip middleware for automatic compression (for clients that support it).
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Database config
DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "myuser"),
    "password": os.getenv("DB_PASSWORD", "mypassword"),
    "database": os.getenv("DB_NAME", "mydb"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "charset": "utf8mb4",
    "use_unicode": True,
    "connection_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", 10)),
}

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", 10))
POOL: Optional[pooling.MySQLConnectionPool] = None
try:
    POOL = pooling.MySQLConnectionPool(
        pool_name="keypoints_pool",
        pool_size=POOL_SIZE,
        pool_reset_session=True,
        **DB_CONF
    )
    print(f"✅ MySQL pool created (size={POOL_SIZE}).")
except Exception as e:
    POOL = None
    print(f"⚠️ Failed to create MySQL pool: {e}")


API_KEY = os.getenv("API_KEY", "changeme")


def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")


def get_conn():
    """
    Get a connection from the pool if available, else create a new connection (fallback).
    Caller must call conn.close() to return to pool.
    """
    if POOL is not None:
        try:
            return POOL.get_connection()
        except Exception as e:
            print("⚠️ Pool get_connection failed:", e)
    try:
        return mysql.connector.connect(**DB_CONF)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")


# Helpers for JSON (serialize/deserialize) with orjson fallback
def dumps_json_bytes(obj: Any) -> bytes:
    if _HAS_ORJSON:
        return orjson.dumps(obj)
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def loads_json(s: bytes) -> Any:
    if _HAS_ORJSON:
        return orjson.loads(s)
    return json.loads(s.decode("utf-8"))


def round_keypoints(rows: List[Dict[str, Any]], decimals: Optional[int]) -> None:
    """
    In-place rounding of numeric values inside 'keypoints' fields to reduce payload size.
    Expects keypoints to be either:
      - JSON array of arrays: [[x,y,...],[x,y,...],...]
      - or arbitrary nested lists/dicts (we handle lists of lists and numbers)
    """
    if decimals is None:
        return
    for r in rows:
        kp = r.get("keypoints")
        if kp is None:
            continue
        if isinstance(kp, (list, tuple)):
            def round_val(v):
                try:
                    return round(float(v), decimals)
                except Exception:
                    return v
            def recurse(value):
                if isinstance(value, (list, tuple)):
                    return [recurse(x) for x in value]
                elif isinstance(value, dict):
                    return {k: recurse(v) for k, v in value.items()}
                else:
                    return round_val(value)
            r["keypoints"] = recurse(kp)


@app.get("/api/keypoints/{word}")
def get_keypoints(
    word: str,
    frame: Optional[int] = Query(None, description="Specific frame number to retrieve"),
    limit: Optional[int] = Query(None, description="Limit number of frames returned (for pagination)"),
    round_decimals: Optional[int] = Query(3, description="Round floats to this many decimals to shrink payload; set -1 to disable"),
    _: None = Depends(verify_api_key),
):
    """
    Returns keypoints for a given word.
    - If frame is provided: returns only that frame (not cached).
    - If frame is None and limit is None: fetch everything.
    """
    t_start = time.perf_counter()

    # Build and execute DB query
    t_db_conn_start = time.perf_counter()
    conn = get_conn()
    t_db_conn_after = time.perf_counter()

    cur = conn.cursor(dictionary=True)
    query = "SELECT frame_number, keypoints FROM words WHERE word = %s"
    params = [word]
    if frame is not None:
        query += " AND frame_number = %s"
        params.append(frame)
    query += " ORDER BY frame_number"
    if limit is not None and frame is None:
        query += " LIMIT %s"
        params.append(limit)

    t_query_start = time.perf_counter()
    try:
        cur.execute(query, tuple(params))
    except Exception as e:
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Query error: {e}")
    t_query_after = time.perf_counter()

    rows = cur.fetchall()
    t_fetch_after = time.perf_counter()
    cur.close()
    conn.close()
    t_db_done = time.perf_counter()

    print(
        f"timing: db_connect={(t_db_conn_after - t_db_conn_start):.4f}s "
        f"query={(t_query_after - t_query_start):.4f}s fetch={(t_fetch_after - t_query_after):.4f}s "
        f"db_total={(t_db_done - t_db_conn_start):.4f}s"
    )

    # Post-process: keypoints field contains JSON string in DB -> decode per-row
    t_decode_start = time.perf_counter()
    for r in rows:
        kp_raw = r.get("keypoints")
        if isinstance(kp_raw, (bytes, bytearray)):
            try:
                kp_raw = kp_raw.decode("utf-8")
            except Exception:
                kp_raw = None
        if isinstance(kp_raw, str):
            try:
                r["keypoints"] = json.loads(kp_raw)
            except Exception:
                r["keypoints"] = []
    t_decode_after = time.perf_counter()
    print(f"timing: json_decode={(t_decode_after - t_decode_start):.4f}s")

    # Optionally round to reduce payload
    if isinstance(round_decimals, int) and round_decimals >= 0:
        t_round_start = time.perf_counter()
        round_keypoints(rows, round_decimals)
        t_round_after = time.perf_counter()
        print(f"timing: rounding={(t_round_after - t_round_start):.4f}s")

    t_total = time.perf_counter() - t_start
    print(f"timing: total={(t_total):.4f}s")

    try:
        if _HAS_ORJSON:
            return Response(content=orjson.dumps(rows), media_type="application/json")
        else:
            return Response(content=json.dumps(rows, ensure_ascii=False), media_type="application/json")
    except Exception as e:
        print("⚠️ Response serialization failed, falling back to default response:", e)
        return rows
