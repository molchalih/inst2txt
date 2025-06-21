import sqlite3
import pandas as pd
import os
from typing import List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramDataManager:
    def __init__(self, db_path: str = "instagram_data.db", csv_path: str = "data.csv"):
        self.db_path = db_path
        self.csv_path = csv_path
        self.init_database()
        self.migrate_schema()
    
    def init_database(self):
        """Initialize the SQLite database with the required table structure."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS instagram_accounts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT UNIQUE NOT NULL,
                        url TEXT,
                        insta_id TEXT,
                        followers INTEGER,
                        following INTEGER,
                        reels_id TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
                logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def migrate_schema(self):
        """Ensure all required columns exist and migrate usernames if needed."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(instagram_accounts)")
                columns = [row[1] for row in cursor.fetchall()]
                # Add missing columns
                if 'username' not in columns:
                    cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN username TEXT")
                    logger.info("Added 'username' column to instagram_accounts table")
                if 'insta_id' not in columns:
                    cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN insta_id TEXT")
                    logger.info("Added 'insta_id' column to instagram_accounts table")
                if 'followers' not in columns:
                    cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN followers INTEGER")
                    logger.info("Added 'followers' column to instagram_accounts table")
                if 'following' not in columns:
                    cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN following INTEGER")
                    logger.info("Added 'following' column to instagram_accounts table")
                if 'reels_id' not in columns:
                    cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN reels_id TEXT")
                    logger.info("Added 'reels_id' column to instagram_accounts table")
                if 'url' not in columns:
                    cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN url TEXT")
                    logger.info("Added 'url' column to instagram_accounts table")
                # Migrate usernames from url if needed
                cursor.execute("SELECT id, url, username FROM instagram_accounts")
                rows = cursor.fetchall()
                for row in rows:
                    row_id, url, username = row
                    if (not username or username.strip() == "") and url:
                        extracted = self.extract_username_from_url(url)
                        cursor.execute("UPDATE instagram_accounts SET username = ? WHERE id = ?", (extracted, row_id))
                        logger.info(f"Migrated username for row id {row_id} from url {url}")
                # Ensure username is unique
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_username_unique ON instagram_accounts(username)")
                conn.commit()
        except Exception as e:
            logger.error(f"Error migrating database schema: {e}")
            raise
    
    def read_csv_data(self) -> List[str]:
        """Read Instagram usernames from the CSV file, deduplicated."""
        try:
            if not os.path.exists(self.csv_path):
                logger.warning(f"CSV file {self.csv_path} not found")
                return []
            
            df = pd.read_csv(self.csv_path, header=None)
            urls = df[0].drop_duplicates().tolist()  # Deduplicate URLs
            usernames = [self.extract_username_from_url(url) for url in urls]
            usernames = list(dict.fromkeys(usernames))  # Remove duplicates, preserve order
            logger.info(f"Read {len(usernames)} unique usernames from CSV file")
            return usernames
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return []
    
    def get_database_usernames(self) -> List[str]:
        """Get all usernames from the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT username FROM instagram_accounts")
                usernames = [row[0] for row in cursor.fetchall()]
                logger.info(f"Retrieved {len(usernames)} usernames from database")
                return usernames
        except Exception as e:
            logger.error(f"Error reading from database: {e}")
            return []
    
    def extract_username_from_url(self, url: str) -> str:
        """Extract username from Instagram URL."""
        try:
            username = url.rstrip('/').split('/')[-1]
            return username
        except:
            return ""
    
    def sync_csv_to_database(self):
        """Sync CSV data to the database, adding new usernames and updating existing ones."""
        try:
            csv_usernames = self.read_csv_data()
            if not csv_usernames:
                logger.warning("No usernames found in CSV file")
                return
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT username FROM instagram_accounts")
                existing_usernames = {row[0] for row in cursor.fetchall()}
                new_usernames = [username for username in csv_usernames if username not in existing_usernames]
                if new_usernames:
                    for username in new_usernames:
                        cursor.execute('''
                            INSERT OR IGNORE INTO instagram_accounts (username, url, insta_id, followers, following, reels_id)
                            VALUES (?, NULL, NULL, NULL, NULL, NULL)
                        ''', (username,))
                    conn.commit()
                    logger.info(f"Added {len(new_usernames)} new usernames to database (duplicates ignored)")
                else:
                    logger.info("Database is already up to date with CSV data")
        except Exception as e:
            logger.error(f"Error syncing CSV to database: {e}")
            raise
    
    def check_sync_status(self) -> Tuple[bool, dict]:
        """
        Check if CSV and database are in sync.
        Returns (is_sync, sync_info)
        """
        try:
            csv_usernames = set(self.read_csv_data())
            db_usernames = set(self.get_database_usernames())
            csv_only = csv_usernames - db_usernames
            db_only = db_usernames - csv_usernames
            is_sync = len(csv_only) == 0 and len(db_only) == 0
            sync_info = {
                'is_sync': is_sync,
                'csv_count': len(csv_usernames),
                'db_count': len(db_usernames),
                'csv_only': list(csv_only),
                'db_only': list(db_only),
                'total_differences': len(csv_only) + len(db_only)
            }
            return is_sync, sync_info
        except Exception as e:
            logger.error(f"Error checking sync status: {e}")
            return False, {'error': str(e)}
    
    def ensure_sync(self):
        """Ensure CSV and database are in sync, performing sync if needed."""
        logger.info("Checking CSV and database sync status...")
        is_sync, sync_info = self.check_sync_status()
        if sync_info.get('error'):
            logger.error(f"Error during sync check: {sync_info['error']}")
            return False
        if is_sync:
            logger.info("✅ CSV and database are in sync")
            logger.info(f"   - CSV usernames: {sync_info['csv_count']}")
            logger.info(f"   - Database usernames: {sync_info['db_count']}")
            return True
        else:
            logger.warning("⚠️  CSV and database are out of sync")
            logger.info(f"   - CSV usernames: {sync_info['csv_count']}")
            logger.info(f"   - Database usernames: {sync_info['db_count']}")
            logger.info(f"   - Usernames only in CSV: {len(sync_info['csv_only'])}")
            logger.info(f"   - Usernames only in database: {len(sync_info['db_only'])}")
            logger.info("🔄 Syncing CSV to database...")
            self.sync_csv_to_database()
            is_sync_after, sync_info_after = self.check_sync_status()
            if is_sync_after:
                logger.info("✅ Sync completed successfully")
                return True
            else:
                logger.error("❌ Sync failed")
                return False

    def update_account_fields(self, username: str, followers: Optional[int] = None, following: Optional[int] = None, reels_id: Optional[str] = None, insta_id: Optional[str] = None):
        """Update followers, following, reels_id, and/or insta_id for a given account by username."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                updates = []
                params = []
                if followers is not None:
                    updates.append("followers = ?")
                    params.append(followers)
                if following is not None:
                    updates.append("following = ?")
                    params.append(following)
                if reels_id is not None:
                    updates.append("reels_id = ?")
                    params.append(reels_id)
                if insta_id is not None:
                    updates.append("insta_id = ?")
                    params.append(insta_id)
                if not updates:
                    logger.warning("No fields to update for account.")
                    return
                params.append(username)
                sql = f"UPDATE instagram_accounts SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE username = ?"
                cursor.execute(sql, params)
                conn.commit()
                logger.info(f"Updated account {username}: {', '.join(updates)}")
        except Exception as e:
            logger.error(f"Error updating account fields: {e}")
            raise

    def remove_duplicate_usernames(self):
        """Remove duplicate usernames, keeping only the row with the lowest id for each username."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Find duplicate usernames
                cursor.execute('''
                    SELECT username FROM instagram_accounts
                    WHERE username IS NOT NULL AND username != ''
                    GROUP BY username HAVING COUNT(*) > 1
                ''')
                duplicates = [row[0] for row in cursor.fetchall()]
                if not duplicates:
                    logger.info("No duplicate usernames found.")
                    return
                for username in duplicates:
                    # Get all ids for this username, ordered by id
                    cursor.execute('''
                        SELECT id FROM instagram_accounts WHERE username = ? ORDER BY id ASC
                    ''', (username,))
                    ids = [row[0] for row in cursor.fetchall()]
                    # Keep the first id, delete the rest
                    ids_to_delete = ids[1:]
                    if ids_to_delete:
                        cursor.executemany('''
                            DELETE FROM instagram_accounts WHERE id = ?
                        ''', [(i,) for i in ids_to_delete])
                        logger.info(f"Removed {len(ids_to_delete)} duplicate(s) for username '{username}'")
                conn.commit()
        except Exception as e:
            logger.error(f"Error removing duplicate usernames: {e}")
            raise 