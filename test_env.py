# test_env.py - quick test for .env values

from utils.utils_config import get_kafka_broker_address
import os

def main():
    # Test the custom getter from utils_config.py
    broker = get_kafka_broker_address()
    print(f"KAFKA_BROKER_ADDRESS from utils_config: {broker}")

    # Direct check using os.getenv
    buzz_topic = os.getenv("BUZZ_TOPIC", "not found")
    sqlite_db = os.getenv("SQLITE_DB", "not found")

    print(f"BUZZ_TOPIC from .env: {buzz_topic}")
    print(f"SQLITE_DB from .env: {sqlite_db}")

if __name__ == "__main__":
    main()
