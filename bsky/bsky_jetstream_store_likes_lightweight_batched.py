#!/usr/bin/env python3

import os
import json
from datetime import datetime
import redis
from mysql.connector import pooling

# Debug mode
DEBUG = False  # Set to False to disable debug logs
MAX_TABLE_SIZE_GB = 40  # Set maximum allowed table size before exiting

def debug_log(message):
    if DEBUG:
        print(message)

# Load DB connection info from environment variables
dbconfig = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

# Create MySQL Connection Pool
debug_log("Initializing MySQL Connection Pool...")
conn_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **dbconfig)

def get_connection():
    return conn_pool.get_connection()

def get_table_size():
    """Fetches the table size in gigabytes."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 / 1024, 4) AS size_gb 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = 'bsky_firehose_likes_light'
    """, (dbconfig["database"],))
    size_gb = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return size_gb if size_gb else 0.0

def main():
    # Connect to Redis
    debug_log("Connecting to Redis...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # Connect to MySQL
    debug_log("Connecting to MySQL...")
    conn = get_connection()
    cursor = conn.cursor()

    # Create the table if not exists
    debug_log("Ensuring table 'bsky_firehose_likes_light' exists...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bsky_firehose_likes_light (
        did CHAR(32),
        record_created_at DATETIME,
        PRIMARY KEY (did, record_created_at)
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    debug_log("Ensured table 'bsky_firehose_likes_light' exists.")

    debug_log("Consumer started. Listening on Redis queue 'bsky_like_queue'...")

    BATCH_SIZE = 10  # Define batch size
    batch_count = 0
    batch = []

    try:
        while True:
            debug_log("Waiting for items from Redis queue...")
            items = r.blpop("bsky_like_queue", timeout=5)
            if items is None:
                debug_log("No items found in queue, retrying...")
                continue

            queue_name, message_str = items
            debug_log(f"Received message: {message_str}")
            try:
                data = json.loads(message_str)
            except json.JSONDecodeError:
                debug_log("JSONDecodeError: Skipping malformed message.")
                continue

            did = data.get("did")
            kind = data.get("kind")
            debug_log(f"Processing DID={did}, kind={kind}")

            if kind != "commit":
                debug_log("Skipping and discarding non-commit item.")
                continue  # Properly discard non-commit items and move on

            commit_obj = data.get("commit", {})
            commit_op = commit_obj.get("operation")

            if commit_obj.get("collection") != "app.bsky.feed.like":
                debug_log("Skipping non-like collection.")
                continue

            if commit_op == "create":
                record_obj = commit_obj.get("record", {})
                record_created_at = record_obj.get("createdAt").replace('T', ' ').split('.')[0]
                batch.append((did, record_created_at))
                debug_log(f"Queued 'create' for DID={did}")

            elif commit_op == "delete":
                debug_log(f"Ignoring delete operation for DID={did}")
                continue

            if len(batch) >= BATCH_SIZE:
                debug_log(f"Processing batch of {len(batch)} records...")
                insert_stmt = """
                    INSERT INTO bsky_firehose_likes_light (did, record_created_at)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE record_created_at = VALUES(record_created_at)
                """
                try:
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.executemany(insert_stmt, batch)
                    conn.commit()
                    #print(f"Successfully inserted {len(batch)} records.")
                    batch_count += 1
                except Exception as e:
                    print(f"Error inserting batch: {e}")
                finally:
                    cursor.close()
                    conn.close()

                batch.clear()  # Reset batch after insertion

                if batch_count % 1000 == 0:
                    table_size = get_table_size()
                    print(f"SQL Table size: {table_size} GB")
                    if table_size >= MAX_TABLE_SIZE_GB:
                        raise RuntimeError(f"Table size limit exceeded: {table_size} GB (Max: {MAX_TABLE_SIZE_GB} GB)")
    finally:
        print("Shutting down. Closing MySQL connection pool.")

if __name__ == "__main__":
    main()
