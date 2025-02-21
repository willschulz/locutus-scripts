#!/usr/bin/env python3

import os
import json
import time

import redis
from mysql.connector import pooling

dbconfig = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

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

    while True:
        item = r.blpop("bsky_like_queue", timeout=5)
        if item is None:
            continue

        queue_name, message_str = item
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
        commit_rkey = commit_obj.get("rkey")

        if commit_obj.get("collection") != "app.bsky.feed.like":
            continue

        if commit_op == "create":
            record_obj = commit_obj.get("record", {})
            record_created_at = record_obj.get("createdAt")
            insert_stmt = """
                INSERT INTO bsky_firehose_likes_light (did, record_created_at, deleted_at)
                VALUES (%s, %s, NULL)
            """
            try:
                cursor.execute(insert_stmt, (did, record_created_at))
                conn.commit()
                #print(f"Inserted like with DID={did} to DB.")
            except Exception as e:
                print(f"Error inserting record: {e}")

        elif commit_op == "delete":
            delete_stmt = """
                UPDATE bsky_firehose_likes_light 
                SET deleted_at = CURRENT_TIMESTAMP 
                WHERE did = %s
            """
            try:
                cursor.execute(delete_stmt, (did,))
                conn.commit()
                #print(f"Marked like with DID={did} as deleted in DB.")
            except Exception as e:
                print(f"Error marking record as deleted: {e}")

        time.sleep(0.01)

if __name__ == "__main__":
    main()
