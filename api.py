import os
import json
import mysql.connector
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from dotenv import load_dotenv
import redis 
from redis.exceptions import ConnectionError 

# Load environment variables from valkey.env
load_dotenv("valkey.env")
app = FastAPI()

# ✅ Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- REDIS/VALKEY CACHING CONFIGURATION ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_USERNAME = os.getenv("REDIS_USERNAME", None) # <-- CRITICAL: Now loads the 'default' username
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Cache entries expire after 5 minutes (300 seconds)
CACHE_TIMEOUT_SECONDS = 300 

# Initialize the Redis client globally
try:
    # --- FIX: ADD USERNAME AND ENSURE SSL IS CONFIGURED ---
    REDIS_CLIENT = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0, # Standard practice, ensures connection to the default database
        username=REDIS_USERNAME, # <-- This is the key fix for DigitalOcean's ACL
        password=REDIS_PASSWORD, 
        decode_responses=True,
        socket_timeout=5,
        ssl=True, # MANDATORY: DigitalOcean requires SSL/TLS
        ssl_cert_reqs=None, # Allows connection without a local CA certificate file
    )
    
    # Test the connection immediately
    REDIS_CLIENT.ping()
    print("✅ Successfully connected to Redis/Valkey cache.")
    IS_CACHE_AVAILABLE = True
except ConnectionError as e:
    print(f"⚠️ Failed to connect to Redis/Valkey at {REDIS_HOST}:{REDIS_PORT}. Falling back to DB-only operation. Error: {e}")
    # Log the full exception for detailed debugging if it fails
    import sys
    print(f"Full connection error details: {sys.exc_info()[1]}")
    REDIS_CLIENT = None
    IS_CACHE_AVAILABLE = False


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
    # Placeholder for actual DB connection
    try:
        # Note: If this is also a DigitalOcean connection, it may need SSL configuration too
        return mysql.connector.connect(**DB_CONF)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

@app.get("/api/keypoints/{word}")
def get_keypoints(
    word: str,
    frame: Optional[int] = None,
    _: None = Depends(verify_api_key),
):
    # Only cache requests that retrieve ALL frames for a word (frame is None)
    is_cacheable_request = frame is None and IS_CACHE_AVAILABLE
    
    # --- 1. CACHE LOOKUP (Redis/Valkey) ---
    if is_cacheable_request:
        try:
            # Attempt to get the JSON string from Redis
            cached_json = REDIS_CLIENT.get(word) 
            
            if cached_json:
                print(f"✅ Cache Hit: Serving '{word}' from Redis/Valkey.")
                # Deserialize the JSON string back to a Python list
                return json.loads(cached_json)
            
            print(f"🟡 Cache Miss: Querying database for '{word}'.")
        except Exception as e:
            # Log cache error but proceed to DB
            print(f"⚠️ Redis/Valkey read error for '{word}': {e}")
        
    # --- 2. DATABASE QUERY (Placeholder logic) ---
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    query = "SELECT frame_number, keypoints FROM words WHERE word = %s"
    params = [word]

    if frame is not None:
        query += " AND frame_number = %s"
        params.append(frame)
    
    query += " ORDER BY frame_number"
    
    cur.execute(query, tuple(params))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # --- 3. POST-PROCESSING ---
    # Convert JSON strings back into Python objects
    for r in rows:
        try:
            r["keypoints"] = json.loads(r["keypoints"])
        except json.JSONDecodeError:
            print(f"⚠️ JSON Decode Error for word '{word}' frame {r.get('frame_number')}")
            r["keypoints"] = []

    # --- 4. CACHE STORE (Redis/Valkey) ---
    if is_cacheable_request and rows:
        try:
            # Serialize the Python list of dicts to a JSON string
            json_to_cache = json.dumps(rows)
            
            # Store the JSON string in Redis, setting an expiration time (EX)
            REDIS_CLIENT.set(word, json_to_cache, ex=CACHE_TIMEOUT_SECONDS)
            print(f"💾 Cache Stored: '{word}' added/updated in Redis/Valkey.")
        except Exception as e:
            # Log cache error but return result anyway
            print(f"⚠️ Redis/Valkey write error for '{word}': {e}")

    return rows
