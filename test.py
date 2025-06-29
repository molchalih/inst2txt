import os
import sqlite3
import logging
from db_manager import InstagramDataManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def setup_test_database():
    """
    Deletes the old database and creates a new one populated with test users.
    """
    db_path = "data/instagram_data.db"
    
    # 1. Delete the existing database file
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            logger.info(f"Deleted existing database at {db_path}")
        except OSError as e:
            logger.error(f"Error deleting database file {db_path}: {e}")
            return

    # 2. Initialize the database and schema
    # Instantiating the manager will create the .db file and tables
    logger.info("Initializing new empty database...")
    data_manager = InstagramDataManager(db_path=db_path)
    
    # 3. Populate with test users
    test_usernames = [
        "theolefirenko",
        "recider",
        "leoo.films"
    ]
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            users_to_insert = [(user,) for user in test_usernames]
            cursor.executemany(
                "INSERT INTO instagram_accounts (username) VALUES (?)",
                users_to_insert
            )
            conn.commit()
            logger.info(f"Successfully inserted {len(users_to_insert)} test users into the database.")
            
            # Verify insertion
            cursor.execute("SELECT username FROM instagram_accounts")
            inserted_users = [row[0] for row in cursor.fetchall()]
            logger.info(f"Current users in database: {inserted_users}")

    except Exception as e:
        logger.error(f"An error occurred during test data population: {e}")


if __name__ == "__main__":
    setup_test_database() 