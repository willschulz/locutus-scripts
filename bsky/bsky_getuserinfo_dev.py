import mysql.connector
import os
import requests
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Database connection details
db_host = os.getenv("DB_HOST")
db_port = int(os.getenv("DB_PORT"))
db_database = os.getenv("DB_DATABASE")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

# API base URL
BASE_URL = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"

# === SCALING CONTROL: Adjust MAX_DIDs here ===
MAX_DIDS = 100000  # Set to None or remove limit to process all rows
BATCH_SIZE = 1000  # Number of DIDs processed per batch
RETRY_LIMIT = 3  # Max retries for failed API requests

try:
    # Connect to the database
    conn = mysql.connector.connect(
        host=db_host,
        port=db_port,
        database=db_database,
        user=db_username,
        password=db_password
    )
    cursor = conn.cursor()
    logging.info("Connected to the database.")

    # Create table for storing user profile JSON (if it doesn't exist)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS userinfo_json_likes_99pctl (
        id INT AUTO_INCREMENT PRIMARY KEY,
        actor_did VARCHAR(255) NOT NULL UNIQUE,
        profile_json JSON NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Count total unprocessed DIDs, applying MAX_DIDs limit if set
    count_query = """
        SELECT COUNT(*)
        FROM dids_likes_99pctl
        WHERE did NOT IN (SELECT actor_did FROM userinfo_json_likes_99pctl)
    """
    if MAX_DIDS:
        count_query += f" LIMIT {MAX_DIDS}"
    
    cursor.execute(count_query)
    total_unprocessed = cursor.fetchone()[0]
    logging.info(f"Total unprocessed DIDs (limited to {MAX_DIDS if MAX_DIDS else 'ALL'}): {total_unprocessed}")

    # Process in batches
    offset = 0
    while offset < total_unprocessed:
        # Fetch a batch of unprocessed DIDs, respecting MAX_DIDs limit
        batch_query = f"""
            SELECT did
            FROM dids_likes_99pctl
            WHERE did NOT IN (SELECT actor_did FROM userinfo_json_likes_99pctl)
            LIMIT {min(BATCH_SIZE, MAX_DIDS - offset) if MAX_DIDS else BATCH_SIZE} OFFSET {offset}
        """

        cursor.execute(batch_query)
        dids = [row[0] for row in cursor.fetchall()]

        if not dids:
            break  # No more unprocessed DIDs left

        logging.info(f"Processing batch {offset // BATCH_SIZE + 1}, {len(dids)} DIDs...")

        insert_data = []
        for did in dids:
            retries = 0
            while retries < RETRY_LIMIT:
                try:
                    response = requests.get(BASE_URL, params={"actor": did})
                    if response.status_code == 200:
                        profile_data = response.json()
                        insert_data.append((did, json.dumps(profile_data)))
                        break  # Successful request, exit retry loop
                    else:
                        logging.warning(f"Failed to fetch profile for {did}: {response.status_code}")
                except Exception as e:
                    logging.error(f"Error processing DID {did}: {e}")

                retries += 1
                time.sleep(1)  # Wait before retrying

            time.sleep(0.1)  # Avoid hitting API rate limits

        # Insert collected data into the database
        if insert_data:
            insert_query = """
            INSERT INTO userinfo_json_likes_99pctl (actor_did, profile_json)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE profile_json = VALUES(profile_json);
            """
            cursor.executemany(insert_query, insert_data)
            conn.commit()
            logging.info(f"Inserted/Updated {len(insert_data)} records.")

        # Move to next batch
        offset += BATCH_SIZE

        # Stop early if we reached MAX_DIDS
        if MAX_DIDS and offset >= MAX_DIDS:
            logging.info(f"Reached MAX_DIDS limit ({MAX_DIDS}). Stopping early.")
            break

except mysql.connector.Error as db_err:
    logging.error(f"Database error: {db_err}")

finally:
    # Close the database connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    logging.info("Database connection closed.")

logging.info("Profile fetching and database insertion complete.")
