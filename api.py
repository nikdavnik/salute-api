import os
import json
import time
from typing import Optional

import mysql.connector
from mysql.connector import pooling
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import redis
from redis.exceptions import ConnectionError

# Load environment variables
load_dotenv("valkey.env")
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# REDIS/VALKEY CONFIG
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_USERNAME = os.getenv("REDIS_USERNAME", None)
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
# longer cache TTL by default
CACHE_TIMEOUT_SECONDS = int(os.getenv("CACHE_TIMEOUT_SECONDS", 86400))

REDIS_CLIENT = None
IS_CACHE_AVAILABLE = False
try:
    REDIS_CLIENT = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=2,   # lower timeout so calls fail fast
        socket_connect_timeout=2,
        ssl=True,
        ssl_cert_reqs=None,
    )
    REDIS_CLIENT.ping()
    print("✅ Successfully connected to Redis/Valkey cache.")
    IS_CACHE_AVAILABLE = True
except Exception as e:
    print(f"⚠️ Failed to connect to Redis/Valkey: {e}")
    REDIS_CLIENT = None
    IS_CACHE_AVAILABLE = False

# DATABASE CONFIG
DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "myuser"),
    "password": os.getenv("DB_PASSWORD", "mypassword"),
    "database": os.getenv("DB_NAME", "mydb"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "charset": "utf8mb4",
    "use_unicode": True,
    # Optional: set a connect timeout so pool operations fail fast
    "connection_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", 10)),
}

# Create a connection pool once at startup
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", 10))
try:
    POOL = pooling.MySQLConnectionPool(
        pool_name="mypool",
        pool_size=POOL_SIZE,
        pool_reset_session=True,
        **DB_CONF
    )
    print(f"✅ DB pool created (size={POOL_SIZE}).")
except Exception as e:
    POOL = None
    print(f"⚠️ Could not create DB pool: {e}")

API_KEY = os.getenv("API_KEY", "changeme")


def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")


def get_conn():
    if POOL is None:
        # Fallback: try a direct connection (not recommended for production)
        try:
            return mysql.connector.connect(**DB_CONF)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database connection error: {e}")
    try:
        return POOL.get_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB pool error: {e}")


@app.get("/api/keypoints/{word}")
def get_keypoints(word: str, frame: Optional[int] = None, _: None = Depends(verify_api_key)):
    t0 = time.perf_counter()

    is_cacheable_request = frame is None and IS_CACHE_AVAILABLE

    # 1) CACHE LOOKUP
    try:
        t_cache_start = time.perf_counter()
        if is_cacheable_request and REDIS_CLIENT:
            cached_json = REDIS_CLIENT.get(word)
            t_cache_after = time.perf_counter()
            print(f"timing: cache_lookup={t_cache_after - t_cache_start:.4f}s")
            if cached_json:
                print(f"✅ Cache Hit: Serving '{word}' from Redis.")
                print(f"timing: total={(time.perf_counter()-t0):.4f}s")
                return json.loads(cached_json)
        else:
            t_cache_after = time.perf_counter()
            print(f"timing: cache_lookup_skipped={(t_cache_after - t_cache_start):.4f}s")
    except Exception as e:
        print(f"⚠️ Redis read error for '{word}': {e}")

    # 2) DATABASE QUERY
    t_db_connect_start = time.perf_counter()
    conn = get_conn()
    t_db_connected = time.perf_counter()
    cur = conn.cursor(dictionary=True)

    query = "SELECT frame_number, keypoints FROM words WHERE word = %s"
    params = [word]
    if frame is not None:
        query += " AND frame_number = %s"
        params.append(frame)
    query += " ORDER BY frame_number"

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
    conn.close()  # returns connection to pool
    t_db_done = time.perf_counter()

    print(f"timing: db_connect={t_db_connected - t_db_connect_start:.4f}s query_execute={t_query_after - t_query_start:.4f}s fetch={t_fetch_after - t_query_after:.4f}s db_total={t_db_done - t_db_connect_start:.4f}s")

    # 3) POST-PROCESSING (JSON decode)
    t_decode_start = time.perf_counter()
    for r in rows:
        try:
            r["keypoints"] = json.loads(r["keypoints"])
        except Exception:
            r["keypoints"] = []
    t_decode_after = time.perf_counter()
    print(f"timing: json_decode={(t_decode_after - t_decode_start):.4f}s")

    # 4) CACHE STORE
    if is_cacheable_request and rows and REDIS_CLIENT:
        try:
            t_cache_write_start = time.perf_counter()
            REDIS_CLIENT.set(word, json.dumps(rows), ex=CACHE_TIMEOUT_SECONDS)
            t_cache_write_after = time.perf_counter()
            print(f"timing: cache_write={(t_cache_write_after - t_cache_write_start):.4f}s")
        except Exception as e:
            print(f"⚠️ Redis write error for '{word}': {e}")

    t_total = time.perf_counter() - t0
    print(f"timing: total={t_total:.4f}s")
    return rows
