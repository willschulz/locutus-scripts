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
    CREATE TABLE IF NOT EXISTS userinfo_json_likes_75pctl (
        id INT AUTO_INCREMENT PRIMARY KEY,
        actor_did VARCHAR(255) NOT NULL UNIQUE,
        profile_json JSON NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Fetch list of DIDs from 'dids_likes_75pctl' table
    cursor.execute("SELECT did FROM dids_likes_75pctl")
    dids = [row[0] for row in cursor.fetchall()]
    logging.info(f"Fetched {len(dids)} DIDs from the database.")

    # API base URL
    BASE_URL = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"

    # Prepare data for batch insertion
    insert_data = []
    for did in dids:
        try:
            response = requests.get(BASE_URL, params={"actor": did})
            if response.status_code == 200:
                profile_data = response.json()
                insert_data.append((did, json.dumps(profile_data)))

            else:
                logging.warning(f"Failed to fetch profile for {did}: {response.status_code}")

            # Sleep briefly to avoid hitting API rate limits
            time.sleep(0.1)

        except Exception as e:
            logging.error(f"Error processing DID {did}: {e}")

    # Insert collected data into the database
    if insert_data:
        insert_query = """
        INSERT INTO userinfo_json_likes_75pctl (actor_did, profile_json)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE profile_json = VALUES(profile_json);
        """
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        logging.info(f"Inserted/Updated {len(insert_data)} records into the database.")

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
