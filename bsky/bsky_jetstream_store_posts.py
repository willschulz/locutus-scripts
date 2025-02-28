#!/usr/bin/env python3

import os
import random
import json
import time

import redis
import mysql.connector


def main():
    # 1) Load DB connection info from environment variables
    db_host = os.getenv("DB_HOST")
    db_port = int(os.getenv("DB_PORT"))
    db_database = os.getenv("DB_DATABASE")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")

    # 2) Connect to Redis
    print("Connecting to Redis...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # 3) Connect to MySQL
    print("Connecting to MySQL...")
    conn = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_database
    )
    cursor = conn.cursor()

    # 4) Create the table if not exists
    print("Ensuring table 'bsky_firehose_posts' exists...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bsky_firehose_posts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        did VARCHAR(255),
        kind VARCHAR(50),
        time_us BIGINT,
        commit_rev VARCHAR(255),
        commit_operation VARCHAR(50),
        commit_collection VARCHAR(255),
        commit_rkey VARCHAR(255),
        commit_cid VARCHAR(255),
        record_created_at VARCHAR(50),  -- or DATETIME if you'll parse date/time
        record_text TEXT,
        record_langs TEXT,
        original_json JSON,
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    print("Ensured table 'bsky_firehose_posts' exists.")

    # 5) Set probability for insertion
    PROBABILITY = 1  # 0.1% by default

    print("Consumer started. Listening on Redis queue 'bsky_post_queue'...")

    while True:
        # BLPOP (blocking pop) waits until there is an item
        item = r.blpop("bsky_post_queue", timeout=5)
        if item is None:
            # No item in 5 seconds, loop again
            continue

        # item is a tuple (queue_name, message_str)
        queue_name, message_str = item
        try:
            data = json.loads(message_str)
        except json.JSONDecodeError:
            # Malformed JSON, skip
            continue        

        # Flatten out relevant fields
        did = data.get("did")
        time_us = data.get("time_us")
        kind = data.get("kind")

        if kind != "commit":
            # skip other kinds if you want
            continue

        commit_obj = data.get("commit", {})
        commit_rev = commit_obj.get("rev")
        commit_op = commit_obj.get("operation")
        if commit_op != "create":
            # skip updates if you want
            continue

        commit_coll = commit_obj.get("collection")
        commit_rkey = commit_obj.get("rkey")
        commit_cid = commit_obj.get("cid")

        record_obj = commit_obj.get("record", {})
        record_created_at = record_obj.get("createdAt")  # e.g. 2025-02-17T19:34:22.452Z
        record_text = record_obj.get("text")
        record_langs = record_obj.get("langs")

        # Probability check
        if random.random() < PROBABILITY:
            insert_stmt = """
                INSERT INTO bsky_firehose_posts (
                    did, kind, time_us,
                    commit_rev, commit_operation, commit_collection,
                    commit_rkey, commit_cid,
                    record_created_at, record_text, record_langs, original_json
                )
                VALUES (%s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s)
            """

            try:
                cursor.execute(insert_stmt, (
                    did,
                    kind,
                    time_us,
                    commit_rev,
                    commit_op,
                    commit_coll,
                    commit_rkey,
                    commit_cid,
                    record_created_at,
                    record_text,
                    json.dumps(record_langs) if record_langs else None,
                    message_str  # store original JSON
                ))
                conn.commit()
                print(f"Inserted post with DID={did} to DB (probability check passed).")
            except Exception as e:
                print(f"Error inserting record: {e}")
                print(f"Record text: {message_str}")
                print(f"Record text: {message_str}")
        else:
            print("Skipped insert (did not pass probability).")

        time.sleep(0.01)  # optional: small sleep to reduce tight loop

if __name__ == "__main__":
    main()
