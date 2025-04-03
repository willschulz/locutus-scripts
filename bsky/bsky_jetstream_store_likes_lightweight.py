#!/usr/bin/env python3

import os
import json
import time

import redis
from mysql.connector import pooling

# Load DB connection info from environment variables
dbconfig = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

# Create MySQL Connection Pool
print("Initializing MySQL Connection Pool...")
conn_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **dbconfig)

def get_connection():
    return conn_pool.get_connection()

def main():
    # Connect to Redis
    print("Connecting to Redis...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # Connect to MySQL
    print("Connecting to MySQL...")
    conn = get_connection()
    cursor = conn.cursor()

    # Create the table if not exists
    print("Ensuring table 'bsky_firehose_likes_light' exists...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bsky_firehose_likes_light (
        id INT AUTO_INCREMENT PRIMARY KEY,
        did VARCHAR(255),
        record_created_at VARCHAR(50),
        deleted_at TIMESTAMP NULL DEFAULT NULL,
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    print("Ensured table 'bsky_firehose_likes_light' exists.")

    print("Consumer started. Listening on Redis queue 'bsky_like_queue'...")

    BATCH_SIZE = 10  # Define batch size
    batch = []

    try:
        while True:
            items = r.blpop("bsky_like_queue", timeout=5)
            if items is None:
                continue

            while len(batch) < BATCH_SIZE and items:
                queue_name, message_str = items
                try:
                    data = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                did = data.get("did")
                kind = data.get("kind")

                if kind != "commit":
                    continue

                commit_obj = data.get("commit", {})
                commit_op = commit_obj.get("operation")

                if commit_obj.get("collection") != "app.bsky.feed.like":
                    continue

                if commit_op == "create":
                    record_obj = commit_obj.get("record", {})
                    record_created_at = record_obj.get("createdAt")
                    batch.append((did, record_created_at, None))

                elif commit_op == "delete":
                    batch.append((did, None, "CURRENT_TIMESTAMP"))

                if len(batch) >= BATCH_SIZE:
                    break

                items = r.blpop("bsky_like_queue", timeout=1)  # Fetch next item

            if batch:
                insert_stmt = """
                    INSERT INTO bsky_firehose_likes_light (did, record_created_at, deleted_at)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE deleted_at = VALUES(deleted_at)
                """
                try:
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.executemany(insert_stmt, batch)
                    conn.commit()
                    print(f"Processed {len(batch)} records.")
                except Exception as e:
                    print(f"Error inserting batch: {e}")
                finally:
                    cursor.close()
                    conn.close()

                batch.clear()  # Reset batch after insertion
    finally:
        print("Shutting down. Closing MySQL connection pool.")

if __name__ == "__main__":
    main()
