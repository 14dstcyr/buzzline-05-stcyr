"""
consumer_stcyr.py
Custom consumer for Buzzline P5
Reads JSON messages from a Kafka topic and stores processed results in SQLite.
"""

import os
import json
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

# Import helpers from utils
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# ---------------------------
# Load environment
# ---------------------------
load_dotenv()

DB_PATH = Path("data/processed.db")
TABLE_NAME = "processed_messages"

KAFKA_TOPIC = os.getenv("BUZZ_TOPIC", "buzzline")
CONSUMER_GROUP = os.getenv("BUZZ_GROUP", "stcyr_consumer_group")

# ---------------------------
# SQLite setup
# ---------------------------
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                author TEXT,
                category TEXT,
                sentiment REAL,
                sentiment_bucket TEXT,
                message_length INTEGER,
                keyword_mentioned TEXT,
                raw_message TEXT
            )
            """
        )
        conn.commit()
    logger.info(f"Database ready at {DB_PATH}")

def insert_row(row):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {TABLE_NAME} (
                ts, author, category, sentiment, sentiment_bucket,
                message_length, keyword_mentioned, raw_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("timestamp"),
                row.get("author"),
                row.get("category"),
                row.get("sentiment"),
                row.get("sentiment_bucket"),
                row.get("message_length"),
                row.get("keyword_mentioned"),
                row.get("message"),
            ),
        )
        conn.commit()

# ---------------------------
# Processing helpers
# ---------------------------
def bucketize_sentiment(s):
    if s is None:
        return "unknown"
    if s > 0.20:
        return "positive"
    if s < -0.20:
        return "negative"
    return "neutral"

# ---------------------------
# Main loop
# ---------------------------
def main():
    init_db()
    consumer = create_kafka_consumer(topic_provided=KAFKA_TOPIC)

    logger.info(f"Listening on topic={KAFKA_TOPIC}, group={CONSUMER_GROUP}")

    for record in consumer:
        try:
            payload = record.value
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8", errors="replace")
            if isinstance(payload, str):
                msg = json.loads(payload)
            else:
                msg = payload

            # Derive new field
            msg["sentiment_bucket"] = bucketize_sentiment(msg.get("sentiment"))

            insert_row(msg)
            logger.info(
                f"Stored message ts={msg.get('timestamp')} "
                f"author={msg.get('author')} bucket={msg['sentiment_bucket']}"
            )
        except Exception as e:
            logger.exception(f"Error processing message: {e}")


if __name__ == "__main__":
    main()
