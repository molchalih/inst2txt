import logging
import sqlite3
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "data/instagram_data.db"
NEW_CSV_PATH = "data/new.csv"

def extract_username_from_url(url: str) -> str:
    """Extract username from Instagram URL."""
    try:
        # Handles URLs like https://www.instagram.com/username/ or /username/
        return url.strip().rstrip('/').split('/')[-1]
    except Exception:
        return ""

def process_line(line: str) -> tuple[str, str]:
    """Cleans a line from the CSV and returns a (username, url) tuple."""
    line = line.strip()
    if line.startswith('@'):
        line = line[1:]
    
    if 'instagram.com' in line:
        url = line
        username = extract_username_from_url(url)
    else:
        # It's just a username
        username = line
        url = f"https://www.instagram.com/{username}/"
        
    return username, url

def main():
    """
    Reads usernames from data/new.csv and adds them to the database if they don't exist.
    """
    if not os.path.exists(NEW_CSV_PATH):
        logger.error(f"Error: The file {NEW_CSV_PATH} was not found.")
        return

    if not os.path.exists(DB_PATH):
        logger.error(f"Error: Database {DB_PATH} not found. Please run a main script first to initialize it.")
        return

    conn = None
    try:
        with open(NEW_CSV_PATH, 'r') as f:
            lines = f.readlines()
    except Exception as e:
        logger.error(f"Error reading {NEW_CSV_PATH}: {e}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get existing usernames to avoid duplicates
        cursor.execute("SELECT username FROM instagram_accounts")
        existing_usernames = {row[0] for row in cursor.fetchall()}
        logger.info(f"Found {len(existing_usernames)} existing users in the database.")
        
        new_users_to_add = []
        for line in lines:
            if not line.strip():
                continue
            
            username, url = process_line(line)
            if username and username not in existing_usernames:
                new_users_to_add.append((username, url))
                existing_usernames.add(username) # Add to set to handle duplicates within the CSV itself

        if not new_users_to_add:
            logger.info("No new users to add. Database is already up to date with new.csv.")
            return

        # Add new users to the database
        cursor.executemany(
            "INSERT OR IGNORE INTO instagram_accounts (username, url) VALUES (?, ?)",
            new_users_to_add
        )
        conn.commit()
        logger.info(f"Successfully added {cursor.rowcount} new users to the database.")
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 