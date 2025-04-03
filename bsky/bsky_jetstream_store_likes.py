#!/usr/bin/env python3

import os
import random
import json
import time

import redis
import mysql.connector

def main():
    # Load DB connection info from environment variables
    db_host = os.getenv("DB_HOST")
    db_port = int(os.getenv("DB_PORT"))
    db_database = os.getenv("DB_DATABASE")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")

    # Connect to Redis
    print("Connecting to Redis...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # Connect to MySQL
    print("Connecting to MySQL...")
    conn = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_database
    )
    cursor = conn.cursor()

    # Create the table if not exists
    print("Ensuring table 'bsky_firehose_likes' exists...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bsky_firehose_likes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        did VARCHAR(255),
        kind VARCHAR(50),
        time_us BIGINT,
        commit_rev VARCHAR(255),
        commit_operation VARCHAR(50),
        commit_collection VARCHAR(255),
        commit_rkey VARCHAR(255),
        commit_cid VARCHAR(255),
        record_created_at VARCHAR(50),
        subject_cid VARCHAR(255),
        subject_uri TEXT,
        original_json JSON,
        deleted_at TIMESTAMP NULL DEFAULT NULL,
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    print("Ensured table 'bsky_firehose_likes' exists.")

    PROBABILITY = 1  # Adjust sampling rate if needed

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
        time_us = data.get("time_us")
        kind = data.get("kind")

        if kind != "commit":
            continue

        commit_obj = data.get("commit", {})
        commit_rev = commit_obj.get("rev")
        commit_op = commit_obj.get("operation")
        commit_coll = commit_obj.get("collection")
        commit_rkey = commit_obj.get("rkey")
        commit_cid = commit_obj.get("cid")

        if commit_coll != "app.bsky.feed.like":
            continue

        if commit_op == "create":
            record_obj = commit_obj.get("record", {})
            record_created_at = record_obj.get("createdAt")
            subject_obj = record_obj.get("subject", {})
            subject_cid = subject_obj.get("cid")
            subject_uri = subject_obj.get("uri")

            if random.random() < PROBABILITY:
                insert_stmt = """
                    INSERT INTO bsky_firehose_likes (
                        did, kind, time_us, commit_rev, commit_operation, commit_collection,
                        commit_rkey, commit_cid, record_created_at, subject_cid, subject_uri, original_json, deleted_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
                """
                try:
                    cursor.execute(insert_stmt, (
                        did, kind, time_us, commit_rev, commit_op, commit_coll,
                        commit_rkey, commit_cid, record_created_at, subject_cid, subject_uri, message_str
                    ))
                    conn.commit()
                    print(f"Inserted like with DID={did} to DB.")
                except Exception as e:
                    print(f"Error inserting record: {e}")

        elif commit_op == "delete":
            delete_stmt = """
                UPDATE bsky_firehose_likes 
                SET deleted_at = CURRENT_TIMESTAMP 
                WHERE commit_rkey = %s
            """
            try:
                cursor.execute(delete_stmt, (commit_rkey,))
                conn.commit()
                print(f"Marked like with rkey={commit_rkey} as deleted in DB.")
            except Exception as e:
                print(f"Error marking record as deleted: {e}")

        time.sleep(0.01)

if __name__ == "__main__":
    main()
