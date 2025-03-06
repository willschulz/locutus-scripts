# database.py
import os
import mysql.connector
import pandas as pd

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE")
    )

def get_bot_data(bot_id):
    dbconn = get_db_connection()
    bot_data = pd.read_sql_query(f"SELECT * FROM mirror_accounts WHERE id = {bot_id}", dbconn)
    dbconn.close()
    return bot_data

def get_favoritisms(user_id):
    dbconn = get_db_connection()
    favoritisms = pd.read_sql_query(f"SELECT * FROM favoritisms WHERE user_id = {user_id}", dbconn)
    dbconn.close()
    return favoritisms

#could get memories in future