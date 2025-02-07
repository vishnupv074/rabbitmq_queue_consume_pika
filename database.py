import logging
import sqlite3
import os
from pymongo import MongoClient


# Load environment variables
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "mydatabase")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "tasks")
DB_NAME = os.getenv("DB_NAME", "failed_messages.db")


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class MongoDB:
    """Singleton MongoDB connection handler to avoid resource leaks."""
    _client = None

    @classmethod
    def get_client(cls):
        """Get a single MongoDB client instance."""
        if cls._client is None:
            cls._client = MongoClient(MONGO_URI)
        return cls._client

    @classmethod
    def close_client(cls):
        """Close MongoDB connection when no longer needed."""
        if cls._client:
            cls._client.close()
            cls._client = None
            logging.info("MongoDB connection closed.")


def update_mongo(data):
    """Update MongoDB collection with processed task data."""
    try:
        client = MongoDB.get_client()
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        result = collection.update_one(
            {"order_id": data["order_id"]},  # Find by order ID
            {"$set": {"status": "processed", "retry_count": data.get("retry_count", 0)}},
            upsert=True  # Create if not exists
        )
        logging.info(f"MongoDB update successful: {result.modified_count} document(s) updated")
    except Exception as e:
        logging.error(f"Failed to update MongoDB: {e}")


def init_db():
    """Initialize the SQLite database with a structured table."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS failed_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            user_id INTEGER,
            amount REAL,
            status TEXT,
            retry_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()


def save_to_database(message):
    """Save structured failed messages to SQLite."""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO failed_messages (order_id, user_id, amount, status, retry_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message.get("order_id"),
                message.get("user_id"),
                message.get("amount"),
                message.get("status"),
                message.get("retry_count", 0),
            ),
        )

        conn.commit()
        conn.close()
        print(f"Saved failed message: {message}")

    except Exception as e:
        print(f"Error saving to DB: {e}")


# Initialize DB on startup
init_db()
