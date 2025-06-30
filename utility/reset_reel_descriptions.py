import sqlite3
import logging

# --- Configuration ---
DB_PATH = "data/instagram_data.db"
CUTOFF_DATE = "2025-06-25"

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def reset_descriptions_for_old_users():
    """
    Resets the model description fields for reels belonging to users
    created before a specified cutoff date.
    """
    conn = None
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logger.info(f"Successfully connected to the database at {DB_PATH}")

        # 1. Find all users created before the cutoff date
        logger.info(f"Finding users created before {CUTOFF_DATE}...")
        cursor.execute("SELECT insta_id FROM instagram_accounts WHERE created_at < ?", (CUTOFF_DATE,))
        
        # We need to fetch the results and format them correctly for the IN clause
        results = cursor.fetchall()
        if not results:
            logger.info("No users found created before the specified date. No action taken.")
            return

        # Extract just the IDs from the tuples returned by fetchall
        user_ids_to_reset = [row[0] for row in results]
        logger.info(f"Found {len(user_ids_to_reset)} users to process.")
        
        # 2. Reset the descriptions for the reels of those users
        # Create a string of '?' placeholders for the IN clause
        placeholders = ', '.join('?' for _ in user_ids_to_reset)
        
        update_query = f"""
            UPDATE reels
            SET
                model_description_text = '',
                model_description_processed = 0
            WHERE
                user_pk IN ({placeholders})
        """
        
        logger.info("Resetting model descriptions for the reels of these users...")
        cursor.execute(update_query, user_ids_to_reset)
        
        # Report how many rows were affected
        updated_reels_count = cursor.rowcount
        logger.info(f"Successfully reset descriptions for {updated_reels_count} reels.")

        # Commit the transaction
        conn.commit()
        logger.info("Database transaction committed successfully.")

    except sqlite3.Error as e:
        logger.error(f"A database error occurred: {e}")
        if conn:
            # Roll back any changes if an error occurs
            conn.rollback()
            logger.error("Transaction has been rolled back.")
    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    reset_descriptions_for_old_users() 