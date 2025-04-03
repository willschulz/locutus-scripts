import mysql.connector
import os
import json
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

    # Create the output table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bsky_userinfo_likes_99pctl (
        id INT AUTO_INCREMENT PRIMARY KEY,
        actor_did VARCHAR(255) NOT NULL UNIQUE,
        handle VARCHAR(255) NOT NULL,
        display_name VARCHAR(255),
        avatar TEXT,
        banner TEXT,
        description TEXT,
        labels JSON,
        created_at DATETIME NOT NULL,
        indexed_at DATETIME,
        posts_count INT NOT NULL,
        follows_count INT NOT NULL,
        followers_count INT NOT NULL,
        pinned_post_cid VARCHAR(255),
        pinned_post_uri TEXT,
        created_at_db TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    logging.info("Table `bsky_userinfo_likes_99pctl` is ready.")

    # Fetch all raw JSON data
    cursor.execute("SELECT actor_did, profile_json FROM userinfo_json_likes_99pctl")
    rows = cursor.fetchall()
    logging.info(f"Fetched {len(rows)} records for processing.")

    insert_data = []
    
    for actor_did, profile_json in rows:
        try:
            data = json.loads(profile_json)

            # Extract fields
            handle = data.get("handle", "")
            display_name = data.get("displayName", "")
            avatar = data.get("avatar")
            banner = data.get("banner")
            description = data.get("description")
            labels = json.dumps(data.get("labels", []))  # Store as JSON

            created_at = data.get("createdAt")
            indexed_at = data.get("indexedAt")

            posts_count = data.get("postsCount", 0)
            follows_count = data.get("followsCount", 0)
            followers_count = data.get("followersCount", 0)

            # Extract pinned post data
            pinned_post = data.get("pinnedPost", {})
            pinned_post_cid = pinned_post.get("cid")
            pinned_post_uri = pinned_post.get("uri")

            # Append data for batch insert
            insert_data.append((
                actor_did, handle, display_name, avatar, banner, description, labels,
                created_at, indexed_at, posts_count, follows_count, followers_count,
                pinned_post_cid, pinned_post_uri
            ))

        except Exception as e:
            logging.error(f"Error processing actor_did {actor_did}: {e}")

    # Insert data into the new table
    if insert_data:
        insert_query = """
        INSERT INTO bsky_userinfo_likes_99pctl (
            actor_did, handle, display_name, avatar, banner, description, labels,
            created_at, indexed_at, posts_count, follows_count, followers_count,
            pinned_post_cid, pinned_post_uri
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            handle = VALUES(handle),
            display_name = VALUES(display_name),
            avatar = VALUES(avatar),
            banner = VALUES(banner),
            description = VALUES(description),
            labels = VALUES(labels),
            created_at = VALUES(created_at),
            indexed_at = VALUES(indexed_at),
            posts_count = VALUES(posts_count),
            follows_count = VALUES(follows_count),
            followers_count = VALUES(followers_count),
            pinned_post_cid = VALUES(pinned_post_cid),
            pinned_post_uri = VALUES(pinned_post_uri);
        """
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        logging.info(f"Inserted/Updated {len(insert_data)} records.")

except mysql.connector.Error as db_err:
    logging.error(f"Database error: {db_err}")

finally:
    # Close database connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    logging.info("Database connection closed.")

logging.info("Parsing and insertion complete.")
