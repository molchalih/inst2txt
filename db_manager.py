import sqlite3
import pandas as pd
import os
from typing import List, Tuple, Optional
import logging
from datetime import datetime
import json
import numpy as np
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramDataManager:
    def __init__(self, db_path: str = "data/instagram_data.db", csv_path: str = "data/data.csv"):
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
                        insta_id TEXT,
                        username TEXT UNIQUE NOT NULL,
                        full_name TEXT,
                        url TEXT,
                        profile_pic_url TEXT,
                        biography TEXT,
                        city_name TEXT,
                        follower_count INTEGER,
                        following_count INTEGER,
                        followers_list TEXT,
                        following_list TEXT,
                        reels_list TEXT,
                        reels_selected_list TEXT,
                        aesthetic_profile_text TEXT,
                        aesthetic_profile_embedding TEXT,
                        all_reels_fetched_hiker INTEGER DEFAULT 0,
                        all_following_fetched_hiker INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        kmeans_cluster INTEGER,
                        hdbscan_cluster INTEGER,
                        is_noise_point INTEGER DEFAULT 0,
                        umap_x REAL,
                        umap_y REAL,
                        followed_creators_with_reels_selected_list TEXT
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS reels (
                        pk TEXT PRIMARY KEY,
                        id TEXT UNIQUE NOT NULL,
                        user_pk TEXT NOT NULL,
                        code TEXT NOT NULL,
                        taken_at TIMESTAMP,
                        comment_count INTEGER,
                        like_count INTEGER,
                        play_count INTEGER,
                        video_duration REAL,
                        thumbnail_url TEXT,
                        video_url TEXT,
                        caption TEXT,
                        downloaded INTEGER DEFAULT 0,
                        video_unavailable INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        no_audio INTEGER DEFAULT 0,
                        audio_type TEXT,
                        audio_content TEXT,
                        caption_english TEXT,
                        caption_english_short TEXT,
                        model_description_text TEXT,
                        model_description_embeddings BLOB,
                        model_description_processed TEXT,
                        audio_content_short TEXT
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS following (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_pk TEXT NOT NULL,
                        following_pk TEXT NOT NULL,
                        following_username TEXT,
                        following_full_name TEXT,
                        following_profile_pic_url TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_pk, following_pk)
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS requests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        email TEXT NOT NULL,
                        instagram_url TEXT NOT NULL,
                        timestamp DATETIME NOT NULL
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
                # Ensure username is unique, useful for older DBs.
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
        except Exception:
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

                # Filter out empty strings and users that already exist.
                new_usernames = [
                    (user,) for user in csv_usernames 
                    if user and user not in existing_usernames
                ]

                if new_usernames:
                    cursor.executemany(
                        "INSERT OR IGNORE INTO instagram_accounts (username) VALUES (?)",
                        new_usernames
                    )
                    logger.info(f"Committed {len(new_usernames)} new usernames to the database.")
                else:
                    logger.info("Database is already up to date with CSV data.")
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

    def update_account_fields(self, username: str, follower_count: Optional[int] = None, following_count: Optional[int] = None, reels_list: Optional[str] = None, reels_selected_list: Optional[str] = None, insta_id: Optional[str] = None, all_reels_fetched_hiker: Optional[bool] = None, all_following_fetched_hiker: Optional[bool] = None):
        """Update specific fields for a given account by username."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                updates = []
                params = []
                if follower_count is not None:
                    updates.append("follower_count = ?")
                    params.append(follower_count)
                if following_count is not None:
                    updates.append("following_count = ?")
                    params.append(following_count)
                if reels_list is not None:
                    updates.append("reels_list = ?")
                    params.append(reels_list)
                if reels_selected_list is not None:
                    updates.append("reels_selected_list = ?")
                    params.append(reels_selected_list)
                if insta_id is not None:
                    updates.append("insta_id = ?")
                    params.append(insta_id)
                if all_reels_fetched_hiker is not None:
                    updates.append("all_reels_fetched_hiker = ?")
                    params.append(1 if all_reels_fetched_hiker else 0)
                if all_following_fetched_hiker is not None:
                    updates.append("all_following_fetched_hiker = ?")
                    params.append(1 if all_following_fetched_hiker else 0)
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

    def upsert_account(self, username: str, insta_id: str, follower_count: int, following_count: int, full_name: Optional[str] = None, url: Optional[str] = None, profile_pic_url: Optional[str] = None, biography: Optional[str] = None, city_name: Optional[str] = None, followers_list: Optional[str] = None, following_list: Optional[str] = None, reels_list: Optional[str] = None, reels_selected_list: Optional[str] = None, aesthetic_profile_text: Optional[str] = None, aesthetic_profile_embedding: Optional[str] = None):
        """Insert or update an Instagram account."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # The order of columns in the INSERT statement must match the order of values in the tuple.
                columns = [
                    'username', 'insta_id', 'follower_count', 'following_count', 'full_name', 'url',
                    'profile_pic_url', 'biography', 'city_name', 'followers_list', 'following_list', 
                    'reels_list', 'reels_selected_list', 'aesthetic_profile_text', 'aesthetic_profile_embedding'
                ]
                values = [
                    username, insta_id, follower_count, following_count, full_name, url,
                    profile_pic_url, biography, city_name, followers_list, following_list, 
                    reels_list, reels_selected_list, aesthetic_profile_text, aesthetic_profile_embedding
                ]

                cursor.execute(f'''
                    INSERT INTO instagram_accounts ({', '.join(columns)})
                    VALUES ({', '.join(['?'] * len(columns))})
                    ON CONFLICT(username) DO UPDATE SET
                        insta_id = excluded.insta_id,
                        follower_count = excluded.follower_count,
                        following_count = excluded.following_count,
                        full_name = excluded.full_name,
                        url = excluded.url,
                        profile_pic_url = excluded.profile_pic_url,
                        biography = excluded.biography,
                        city_name = excluded.city_name,
                        followers_list = excluded.followers_list,
                        following_list = excluded.following_list,
                        reels_list = excluded.reels_list,
                        reels_selected_list = excluded.reels_selected_list,
                        aesthetic_profile_text = excluded.aesthetic_profile_text,
                        aesthetic_profile_embedding = excluded.aesthetic_profile_embedding,
                        updated_at = CURRENT_TIMESTAMP
                ''', values)
                conn.commit()
                logger.info(f"Upserted account {username} successfully.")
        except Exception as e:
            logger.error(f"Error upserting account {username}: {e}")
            raise

    def get_user_insta_id(self, username: str) -> Optional[str]:
        """Get the insta_id for a given username."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT insta_id FROM instagram_accounts WHERE username = ?", (username,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except Exception as e:
            logger.error(f"Error getting insta_id for user {username}: {e}")
            raise

    def count_reels_for_user(self, user_pk: str) -> int:
        """Count the number of reels for a given user_pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM reels WHERE user_pk = ?", (user_pk,))
                count = cursor.fetchone()[0]
                logger.info(f"Found {count} reels for user_pk {user_pk}.")
                return count
        except Exception as e:
            logger.error(f"Error counting reels for user {user_pk}: {e}")
            raise

    def delete_reels_for_user(self, user_pk: str):
        """Delete all reels for a given user_pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM reels WHERE user_pk = ?", (user_pk,))
                conn.commit()
                logger.info(f"Deleted {cursor.rowcount} reels for user_pk {user_pk}.")
        except Exception as e:
            logger.error(f"Error deleting reels for user {user_pk}: {e}")
            raise

    def get_top_reels(self, user_pk: str, limit: int = 5) -> List[str]:
        """Get the top N most viewed reels for a given user_pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pk FROM reels
                    WHERE user_pk = ?
                    ORDER BY CAST(play_count AS INTEGER) DESC
                    LIMIT ?
                """, (user_pk, limit))
                reels = [row[0] for row in cursor.fetchall()]
                logger.info(f"Retrieved top {len(reels)} reels for user_pk {user_pk}.")
                return reels
        except Exception as e:
            logger.error(f"Error getting top reels for user {user_pk}: {e}")
            raise

    def save_reels(self, reels_data: List[dict], user_pk: str):
        """Save a list of reels to the database."""
        if not reels_data:
            logger.info("No reels data to save.")
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                reels_to_insert = []
                for reel in reels_data:
                    taken_at_unix = reel.get('taken_at')
                    taken_at_datetime = None
                    if taken_at_unix:
                        try:
                            # Convert Unix timestamp to datetime string
                            taken_at_datetime = datetime.fromtimestamp(int(taken_at_unix)).strftime('%Y-%m-%d %H:%M:%S')
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Could not convert timestamp {taken_at_unix} for reel {reel.get('pk')}: {e}")

                    reels_to_insert.append((
                        reel.get('pk'),
                        reel.get('id'),
                        user_pk,
                        reel.get('code'),
                        taken_at_datetime,
                        reel.get('comment_count'),
                        reel.get('like_count'),
                        reel.get('play_count'),
                        reel.get('video_duration'),
                        reel.get('thumbnail_url'),
                        reel.get('video_url'),
                        reel.get('caption', {}).get('text') if isinstance(reel.get('caption'), dict) else None
                    ))

                cursor.executemany('''
                    INSERT INTO reels (
                        pk, id, user_pk, code, taken_at, comment_count, 
                        like_count, play_count, video_duration, thumbnail_url, video_url, caption
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pk) DO UPDATE SET
                        comment_count = excluded.comment_count,
                        like_count = excluded.like_count,
                        play_count = excluded.play_count,
                        thumbnail_url = excluded.thumbnail_url,
                        video_url = excluded.video_url,
                        caption = excluded.caption
                ''', reels_to_insert)
                
                conn.commit()
                logger.info(f"Attempted to save/update {len(reels_to_insert)} reels. {cursor.rowcount} rows were affected.")
        except Exception as e:
            logger.error(f"Error saving reels to database: {e}")
            raise

    def get_user_hiker_status(self, username: str) -> Tuple[int, bool]:
        """Get reels count and hiker fetch status for a user."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT insta_id, all_reels_fetched_hiker FROM instagram_accounts WHERE username = ?", (username,))
                result = cursor.fetchone()
                if not result:
                    return 0, False  # User not in DB
                
                user_pk, all_reels_fetched_hiker = result
                all_reels_fetched_hiker = bool(all_reels_fetched_hiker) if all_reels_fetched_hiker is not None else False
    
                if not user_pk:
                    return 0, all_reels_fetched_hiker
    
                cursor.execute("SELECT COUNT(*) FROM reels WHERE user_pk = ?", (user_pk,))
                count = cursor.fetchone()[0]
                return count, all_reels_fetched_hiker
        except Exception as e:
            logger.error(f"Error getting hiker status for user {username}: {e}")
            return 0, False

    def get_user_following_hiker_status(self, username: str) -> bool:
        """Check if all following for a user have been fetched."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT all_following_fetched_hiker FROM instagram_accounts WHERE username = ?", (username,))
                result = cursor.fetchone()
                if result and result[0]:
                    return True
                return False
        except Exception as e:
            logger.error(f"Error getting following fetch status for user {username}: {e}")
            return False

    def get_hiker_processing_status_for_all_users(self) -> List[Tuple[str, str, int, int, int]]:
        """
        Gets processing status for all users for Hiker.
        Returns a list of tuples: (username, insta_id, all_reels_fetched_hiker, all_following_fetched_hiker, reel_count)
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT
                        ia.username,
                        ia.insta_id,
                        ia.all_reels_fetched_hiker,
                        ia.all_following_fetched_hiker,
                        IFNULL(r.reel_count, 0) as reel_count
                    FROM
                        instagram_accounts ia
                    LEFT JOIN (
                        SELECT
                            user_pk,
                            COUNT(*) as reel_count
                        FROM
                            reels
                        GROUP BY
                            user_pk
                    ) r ON ia.insta_id = r.user_pk
                """)
                users_status = cursor.fetchall()
                logger.info(f"Retrieved hiker processing status for {len(users_status)} users.")
                return users_status
        except Exception as e:
            logger.error(f"Error getting hiker processing status for all users: {e}")
            return []

    def save_following(self, following_data: List[dict], user_pk: str):
        """Save a list of following to the database."""
        if not following_data:
            logger.info("No following data to save.")
            return

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                following_to_insert = []
                for person in following_data:
                    following_to_insert.append((
                        user_pk,
                        str(person.get('pk')),
                        person.get('username'),
                        person.get('full_name'),
                        str(person.get('profile_pic_url')),
                    ))

                cursor.executemany('''
                    INSERT OR IGNORE INTO following (
                        user_pk, following_pk, following_username, following_full_name, following_profile_pic_url
                    ) VALUES (?, ?, ?, ?, ?)
                ''', following_to_insert)

                conn.commit()
                logger.info(f"Attempted to save {len(following_to_insert)} following. {cursor.rowcount} new following were inserted.")
        except Exception as e:
            logger.error(f"Error saving following to database: {e}")
            raise

    def fill_missing_reels_selected_list(self, top_n: int = 5):
        """Fill missing reels_selected_list for users who have reels but no selected reels."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Find users with missing reels_selected_list and at least one reel
                cursor.execute('''
                    SELECT ia.username, ia.insta_id
                    FROM instagram_accounts ia
                    JOIN reels r ON ia.insta_id = r.user_pk
                    WHERE ia.reels_selected_list IS NULL OR ia.reels_selected_list = ''
                    GROUP BY ia.insta_id
                ''')
                users = cursor.fetchall()
                logger.info(f"Found {len(users)} users with missing reels_selected_list.")
                for username, insta_id in users:
                    # Get top reels for this user
                    top_reels = self.get_top_reels(insta_id, limit=top_n)
                    if top_reels:
                        reels_selected_list_json = json.dumps(top_reels)
                        self.update_account_fields(username=username, reels_selected_list=reels_selected_list_json)
                        logger.info(f"Filled reels_selected_list for {username} with top {len(top_reels)} reels.")
        except Exception as e:
            logger.error(f"Error filling missing reels_selected_list: {e}")

    def filter_reels_by_status(self, reel_pks: List[str]) -> set:
        """
        Given a list of reel PKs, returns a set of PKs that are already
        downloaded or marked as unavailable.
        """
        if not reel_pks:
            return set()
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                placeholders = ','.join('?' for _ in reel_pks)
                query = f"""
                    SELECT pk FROM reels
                    WHERE pk IN ({placeholders})
                    AND (downloaded = 1 OR video_unavailable = 1)
                """
                cursor.execute(query, tuple(reel_pks))
                reels_to_skip = {row[0] for row in cursor.fetchall()}
                return reels_to_skip
        except Exception as e:
            logger.error(f"Error filtering reels by status: {e}")
            return set()

    def mark_reel_as_downloaded(self, pk: str):
        """Mark a reel as downloaded by its pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE reels SET downloaded = 1 WHERE pk = ?", (pk,))
                conn.commit()
                logger.info(f"Marked reel {pk} as downloaded.")
        except Exception as e:
            logger.error(f"Error marking reel {pk} as downloaded: {e}")
            raise

    def is_reel_downloaded(self, pk: str) -> bool:
        """Check if a reel is marked as downloaded."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT downloaded FROM reels WHERE pk = ?", (pk,))
                result = cursor.fetchone()
                return bool(result and result[0])
        except Exception as e:
            logger.error(f"Error checking if reel {pk} is downloaded: {e}")
            return False

    def get_reel_video_url(self, pk: str) -> Optional[str]:
        """Get the video_url for a given reel pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT video_url FROM reels WHERE pk = ?", (pk,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except Exception as e:
            logger.error(f"Error getting video_url for reel {pk}: {e}")
            return None

    def get_reel_thumbnail_url(self, pk: str) -> Optional[str]:
        """Get the thumbnail_url for a given reel pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT thumbnail_url FROM reels WHERE pk = ?", (pk,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except Exception as e:
            logger.error(f"Error getting thumbnail_url for reel {pk}: {e}")
            return None

    def get_all_selected_reels(self):
        """Return a list of (username, reels_selected_list) for all users with non-empty reels_selected_list."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT username, reels_selected_list FROM instagram_accounts WHERE reels_selected_list IS NOT NULL AND reels_selected_list != ''")
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching users with reels_selected_list: {e}")
            return []

    def get_all_selected_reel_pks(self) -> List[str]:
        """Get all unique reel PKs from all users' reels_selected_list."""
        users = self.get_all_selected_reels()
        all_pks = set()
        for _, reels_selected_list in users:
            try:
                pks = json.loads(reels_selected_list)
                if isinstance(pks, list):
                    all_pks.update(pks)
            except (json.JSONDecodeError, TypeError):
                continue
        return list(all_pks)

    def get_reels_for_music_analysis(self, reel_pks: List[str]) -> List[str]:
        """
        Given a list of reel PKs, returns a list of PKs that need music analysis
        (no audio_type and no_audio flag is not set to 1).
        """
        if not reel_pks:
            return []
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                placeholders = ','.join('?' for _ in reel_pks)
                query = f"""
                    SELECT pk FROM reels
                    WHERE pk IN ({placeholders})
                    AND (audio_type IS NULL OR audio_type = '')
                    AND (no_audio = 0 OR no_audio IS NULL)
                """
                cursor.execute(query, tuple(reel_pks))
                reels_to_process = [row[0] for row in cursor.fetchall()]
                logger.info(f"Found {len(reels_to_process)} reels requiring music analysis out of {len(reel_pks)} candidates.")
                return reels_to_process
        except Exception as e:
            logger.error(f"Error filtering reels for music analysis: {e}")
            return []

    def mark_reel_as_unavailable(self, pk: str):
        """Mark a reel as unavailable by its pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE reels SET video_unavailable = 1 WHERE pk = ?", (pk,))
                conn.commit()
                logger.info(f"Marked reel {pk} as video_unavailable.")
        except Exception as e:
            logger.error(f"Error marking reel {pk} as unavailable: {e}")
            raise

    def is_reel_unavailable(self, pk: str) -> bool:
        """Check if a reel is marked as unavailable."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT video_unavailable FROM reels WHERE pk = ?", (pk,))
                result = cursor.fetchone()
                return bool(result and result[0])
        except Exception as e:
            logger.error(f"Error checking if reel {pk} is unavailable: {e}")
            return False

    def set_no_audio_flag(self, pk: str):
        """Set the no_audio flag in the reels table for the given pk. Adds the column if it doesn't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE reels SET no_audio = 1 WHERE pk = ?", (pk,))
                conn.commit()
                logger.info(f"Set no_audio flag for reel {pk}.")
        except Exception as e:
            logger.error(f"Error setting no_audio flag for reel {pk}: {e}")
            raise

    def set_audio_info(self, pk: str, audio_type: str, audio_content: Optional[str] = None):
        """Set the audio_type and audio_content fields for a reel by pk. Adds columns if they don't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE reels SET audio_type = ?, audio_content = ? WHERE pk = ?", (audio_type, audio_content, pk))
                conn.commit()
                logger.info(f"Set audio_type={audio_type}, audio_content={audio_content} for reel {pk}.")
        except Exception as e:
            logger.error(f"Error setting audio info for reel {pk}: {e}")
            raise

    def set_caption_english(self, pk: str, caption_english: str):
        """Set the caption_english field for a reel by pk. Adds the column if it doesn't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(reels)")
                columns = [row[1] for row in cursor.fetchall()]
                if 'caption_english' not in columns:
                    cursor.execute("ALTER TABLE reels ADD COLUMN caption_english TEXT")
                cursor.execute("UPDATE reels SET caption_english = ? WHERE pk = ?", (caption_english, pk))
                conn.commit()
                logger.info(f"Set caption_english for reel {pk}.")
        except Exception as e:
            logger.error(f"Error setting caption_english for reel {pk}: {e}")
            raise

    def get_selected_reels_with_captions(self):
        """Return a list of (pk, caption, caption_english) for all reels in selected_reels lists of all users."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT reels_selected_list FROM instagram_accounts WHERE reels_selected_list IS NOT NULL AND reels_selected_list != ''")
                all_pks = set()
                for (reels_selected_list,) in cursor.fetchall():
                    try:
                        pks = json.loads(reels_selected_list)
                        if isinstance(pks, list):
                            all_pks.update(pks)
                    except Exception:
                        continue
                if not all_pks:
                    return []
                placeholders = ','.join('?' for _ in all_pks)
                cursor.execute(f"SELECT pk, caption, caption_english FROM reels WHERE pk IN ({placeholders})", tuple(all_pks))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching selected reels with captions: {e}")
            return []

    def get_followed_creators_with_reels_selected_list(self):
        """For each user with a non-empty reels_selected_list, get the list of insta_id's of other such users they follow."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT
                        ia1.username AS follower_username,
                        ia1.insta_id AS follower_insta_id,
                        json_group_array(ia2.insta_id) AS followed_creators_insta_ids
                    FROM
                        instagram_accounts ia1
                    JOIN
                        following f ON ia1.insta_id = f.user_pk
                    JOIN
                        instagram_accounts ia2 ON f.following_pk = ia2.insta_id
                    WHERE
                        ia1.reels_selected_list IS NOT NULL AND ia1.reels_selected_list != ''
                        AND ia2.reels_selected_list IS NOT NULL AND ia2.reels_selected_list != ''
                    GROUP BY
                        ia1.insta_id
                ''')
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting followed creators with reels_selected_list: {e}")
            return []

    def add_followed_creators_with_reels_selected_list_column(self):
        """Column is now created in init_database. This function is for backward compatibility."""
        pass

    def update_followed_creators_with_reels_selected_list(self, insta_id: str, followed_list_json: str):
        """Update the followed_creators_with_reels_selected_list field for a user by insta_id."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE instagram_accounts SET followed_creators_with_reels_selected_list = ?, updated_at = CURRENT_TIMESTAMP WHERE insta_id = ?",
                    (followed_list_json, insta_id)
                )
                conn.commit()
                logger.info(f"Updated followed_creators_with_reels_selected_list for insta_id {insta_id}.")
        except Exception as e:
            logger.error(f"Error updating followed_creators_with_reels_selected_list for insta_id {insta_id}: {e}")
            raise

    def get_speech_reels_to_process(self, batch_size: int = 10) -> List[Tuple[str, str]]:
        """Get reels with audio_type 'speech' that need processing."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pk, video_url FROM reels 
                    WHERE audio_type = 'speech' 
                    AND (audio_content IS NULL OR audio_content = '')
                    AND video_unavailable = 0
                    LIMIT ?
                """, (batch_size,))
                reels = cursor.fetchall()
                logger.info(f"Found {len(reels)} reels with speech to process")
                return reels
        except Exception as e:
            logger.error(f"Error getting speech reels to process: {e}")
            return []

    def get_speech_processing_stats(self) -> dict:
        """Get statistics about speech processing."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Count reels by audio_type
                cursor.execute("""
                    SELECT audio_type, COUNT(*) as count 
                    FROM reels 
                    WHERE audio_type IS NOT NULL 
                    GROUP BY audio_type
                """)
                audio_type_stats = cursor.fetchall()
                
                # Count reels with no_audio flag
                cursor.execute("SELECT COUNT(*) FROM reels WHERE no_audio = 1")
                no_audio_count = cursor.fetchone()[0]
                
                # Count reels with speech content
                cursor.execute("SELECT COUNT(*) FROM reels WHERE audio_type = 'speech' AND audio_content IS NOT NULL AND audio_content != ''")
                speech_with_content = cursor.fetchone()[0]
                
                # Count reels with speech but no content (pending processing)
                cursor.execute("SELECT COUNT(*) FROM reels WHERE audio_type = 'speech' AND (audio_content IS NULL OR audio_content = '')")
                speech_pending = cursor.fetchone()[0]
                
                stats = {
                    'no_audio_count': no_audio_count,
                    'speech_with_content': speech_with_content,
                    'speech_pending': speech_pending,
                    'audio_type_stats': audio_type_stats
                }
                
                logger.info("=== Speech Processing Statistics ===")
                logger.info(f"Reels with no_audio flag: {no_audio_count}")
                logger.info(f"Reels with speech content: {speech_with_content}")
                logger.info(f"Reels with speech pending processing: {speech_pending}")
                
                for audio_type, count in audio_type_stats:
                    logger.info(f"Reels with audio_type '{audio_type}': {count}")
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting speech processing stats: {e}")
            return {}

    def mark_reel_as_no_audio_and_clear_type(self, pk: str):
        """Mark a reel as no_audio and clear the audio_type."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE reels SET no_audio = 1, audio_type = '' WHERE pk = ?", (pk,))
                conn.commit()
                logger.info(f"Marked reel {pk} as no_audio and cleared audio_type")
        except Exception as e:
            logger.error(f"Error marking reel {pk} as no_audio: {e}")
            raise

    def get_reel_info(self, reel_id: str) -> Optional[dict]:
        """Get complete reel information by ID for video processing."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pk, user_pk, code, caption, caption_english, caption_english_short,
                           audio_type, audio_content, audio_content_short, video_url
                    FROM reels 
                    WHERE pk = ?
                """, (reel_id,))
                
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"Reel with ID {reel_id} not found in database")
                    return None
                
                return dict(row)
        except Exception as e:
            logger.error(f"Error getting reel info for {reel_id}: {e}")
            return None

    def get_selected_reels_list(self) -> List[str]:
        """Get the list of selected reel IDs from ALL users in instagram_accounts table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT reels_selected_list FROM instagram_accounts WHERE reels_selected_list IS NOT NULL AND reels_selected_list != ''")
                rows = cursor.fetchall()
                
                all_reels = []
                for row in rows:
                    if row[0]:
                        try:
                            user_reels = json.loads(row[0])
                            if isinstance(user_reels, list):
                                all_reels.extend(user_reels)
                        except json.JSONDecodeError:
                            logger.error("Failed to parse reels_selected_list JSON")
                            continue
                
                unique_reels = list(set(all_reels))
                logger.info(f"Found {len(unique_reels)} unique reels from {len(rows)} users")
                return unique_reels
        except Exception as e:
            logger.error(f"Error getting selected reels list: {e}")
            return []

    def get_reels_without_description(self, reel_ids: List[str]) -> List[str]:
        """Get reel IDs that don't have model_description_text yet."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in reel_ids])
                cursor.execute(f"""
                    SELECT pk FROM reels 
                    WHERE pk IN ({placeholders})
                    AND (model_description_text IS NULL OR model_description_text = '')
                """, reel_ids)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting reels without description: {e}")
            return []

    def set_model_description(self, pk: str, description: str):
        """Set the model_description_text field for a reel by pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE reels SET model_description_text = ? WHERE pk = ?",
                    (description, pk)
                )
                conn.commit()
                logger.info(f"Set model_description_text for reel {pk}")
        except Exception as e:
            logger.error(f"Error setting model_description_text for reel {pk}: {e}")
            raise

    def ensure_embeddings_column(self):
        """Column is now created in init_database. This function is for backward compatibility."""
        pass

    def get_reels_for_embedding_generation(self) -> List[Tuple[str, str]]:
        """Get reels that need embedding generation (have descriptions but no embeddings)."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pk, 
                           COALESCE(model_description_processed, model_description_text) as description
                    FROM reels 
                    WHERE ((model_description_processed IS NOT NULL AND model_description_processed != '')
                       OR (model_description_text IS NOT NULL AND model_description_text != ''))
                    AND (model_description_embeddings IS NULL OR model_description_embeddings = '')
                """)
                
                reels = cursor.fetchall()
                logger.info(f"Found {len(reels)} reels needing embedding generation")
                return reels
        except Exception as e:
            logger.error(f"Error getting reels for embedding generation: {e}")
            return []

    def save_embedding(self, pk: str, embedding_blob: bytes):
        """Save embedding blob for a reel by pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE reels SET model_description_embeddings = ? WHERE pk = ?", (embedding_blob, pk))
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving embedding for reel {pk}: {e}")
            raise

    def ensure_processed_column(self):
        """Column is now created in init_database. This function is for backward compatibility."""
        pass

    def get_reels_for_processing(self) -> List[Tuple[str, str]]:
        """Get reels that have a model description but no processed description yet."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pk, model_description_text
                    FROM reels
                    WHERE model_description_text IS NOT NULL 
                      AND (model_description_processed IS NULL OR model_description_processed = '')
                """)
                reels = cursor.fetchall()
                logger.info(f"Found {len(reels)} reels for processing")
                return reels
        except Exception as e:
            logger.error(f"Error getting reels for processing: {e}")
            return []

    def save_processed_description(self, pk: str, processed_description: str):
        """Save processed description for a reel by pk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE reels SET model_description_processed = ? WHERE pk = ?", (processed_description, pk))
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving processed description for reel {pk}: {e}")
            raise

    def get_creator_profiles(self) -> Tuple[dict, dict]:
        """Get creator profiles by aggregating reels into creator profiles by averaging their embeddings."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT user_pk, pk, model_description_embeddings
                    FROM reels 
                    WHERE model_description_embeddings IS NOT NULL 
                    AND model_description_embeddings != ''
                    AND user_pk IS NOT NULL
                    ORDER BY user_pk
                """)
                
                reels_data = cursor.fetchall()
                logger.info(f"Found {len(reels_data)} reels with embeddings")
                
                creator_reels = defaultdict(list)
                for user_pk, reel_pk, embedding_blob in reels_data:
                    embedding = self._load_embedding_from_blob(embedding_blob)
                    if embedding is not None:
                        creator_reels[user_pk].append((reel_pk, embedding))
                
                logger.info(f"Found {len(creator_reels)} creators with embeddings")
                
                creator_profiles = {}
                creator_stats = {}
                
                for user_pk, reels in creator_reels.items():
                    if len(reels) < 1:
                        continue
                    
                    embeddings = [embedding for _, embedding in reels]
                    reel_pks = [pk for pk, _ in reels]
                    
                    avg_embedding = np.mean(embeddings, axis=0)
                    
                    creator_profiles[user_pk] = avg_embedding
                    creator_stats[user_pk] = {
                        'reel_count': len(reels),
                        'reel_pks': reel_pks
                    }
                
                logger.info(f"Created profiles for {len(creator_profiles)} creators")
                return creator_profiles, creator_stats
                
        except Exception as e:
            logger.error(f"Error getting creator profiles: {e}")
            raise

    def _load_embedding_from_blob(self, blob_data):
        """Convert blob data back to numpy array"""
        if blob_data is None:
            return None
        try:
            embedding_array = np.frombuffer(blob_data, dtype=np.float32)
            return embedding_array.reshape(1, -1)
        except Exception as e:
            logger.error(f"Error loading embedding from blob: {e}")
            return None

    def ensure_clustering_columns(self):
        """Columns are now created in init_database. This function is for backward compatibility."""
        pass

    def save_clustering_results(self, kmeans_results: Optional[dict] = None, hdbscan_results: Optional[dict] = None):
        """Save clustering results to the instagram_accounts table."""
        try:
            self.ensure_clustering_columns()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if kmeans_results:
                    for user_pk, data in kmeans_results.items():
                        cluster_id = data['cluster']
                        cursor.execute("UPDATE instagram_accounts SET kmeans_cluster = ? WHERE insta_id = ?", (cluster_id, user_pk))
                    logger.info(f"Saved K-means clustering results for {len(kmeans_results)} creators")
                
                if hdbscan_results:
                    for user_pk, data in hdbscan_results.items():
                        cluster_id = data['cluster']
                        is_noise = 1 if data['is_noise'] else 0
                        cursor.execute("UPDATE instagram_accounts SET hdbscan_cluster = ?, is_noise_point = ? WHERE insta_id = ?", (cluster_id, is_noise, user_pk))
                    logger.info(f"Saved HDBSCAN clustering results for {len(hdbscan_results)} creators")
                
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving clustering results: {e}")
            raise

    def save_umap_coordinates(self, creator_coordinates: dict):
        """Save UMAP coordinates for creators to the instagram_accounts table."""
        try:
            self.ensure_clustering_columns()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for user_pk, coordinates in creator_coordinates.items():
                    x, y = coordinates
                    cursor.execute("UPDATE instagram_accounts SET umap_x = ?, umap_y = ? WHERE insta_id = ?", (float(x), float(y), user_pk))
                
                conn.commit()
                logger.info(f"Saved UMAP coordinates for {len(creator_coordinates)} creators")
        except Exception as e:
            logger.error(f"Error saving UMAP coordinates: {e}")
            raise

    def get_umap_coordinates(self) -> dict:
        """Get UMAP coordinates for all creators from the instagram_accounts table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT insta_id, umap_x, umap_y FROM instagram_accounts WHERE umap_x IS NOT NULL AND umap_y IS NOT NULL AND insta_id IS NOT NULL")
                
                coordinates = {insta_id: (x, y) for insta_id, x, y in cursor.fetchall()}
                
                logger.info(f"Retrieved UMAP coordinates for {len(coordinates)} creators")
                return coordinates
        except Exception as e:
            logger.error(f"Error getting UMAP coordinates: {e}")
            return {}

    def get_clustering_stats(self) -> dict:
        """Get statistics about clustering results from instagram_accounts table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT kmeans_cluster, COUNT(*) as count FROM instagram_accounts WHERE kmeans_cluster IS NOT NULL GROUP BY kmeans_cluster ORDER BY kmeans_cluster")
                kmeans_stats = cursor.fetchall()
                
                cursor.execute("SELECT hdbscan_cluster, COUNT(*) as count FROM instagram_accounts WHERE hdbscan_cluster IS NOT NULL GROUP BY hdbscan_cluster ORDER BY hdbscan_cluster")
                hdbscan_stats = cursor.fetchall()
                
                cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE is_noise_point = 1")
                noise_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE kmeans_cluster IS NOT NULL")
                creators_with_kmeans = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE hdbscan_cluster IS NOT NULL")
                creators_with_hdbscan = cursor.fetchone()[0]
                
                stats = {
                    'kmeans_stats': kmeans_stats,
                    'hdbscan_stats': hdbscan_stats,
                    'noise_count': noise_count,
                    'creators_with_kmeans': creators_with_kmeans,
                    'creators_with_hdbscan': creators_with_hdbscan
                }
                
                logger.info("=== Clustering Statistics ===")
                logger.info(f"Creators with K-means clustering: {creators_with_kmeans}")
                logger.info(f"Creators with HDBSCAN clustering: {creators_with_hdbscan}")
                logger.info(f"Noise points: {noise_count}")
                
                logger.info("K-means cluster sizes:")
                for cluster_id, count in kmeans_stats:
                    logger.info(f"  Cluster {cluster_id}: {count} creators")
                
                logger.info("HDBSCAN cluster sizes:")
                for cluster_id, count in hdbscan_stats:
                    logger.info(f"  Cluster {cluster_id if cluster_id != -1 else 'Noise'}: {count} creators")
                
                return stats
        except Exception as e:
            logger.error(f"Error getting clustering stats: {e}")
            return {} 