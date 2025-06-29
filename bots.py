import logging
import random
import time
import json
from pathlib import Path
from db_manager import InstagramDataManager
from bot_manager import BotManager
from instagrapi import Client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_user_reels(username: str, cl: Client, data_manager: InstagramDataManager, reels_to_fetch: int = 48):
    """
    Main process for fetching and storing data for a single Instagram user.
    """
    logger.info(f"Starting process for user: {username}")

    # 1. Fetch user info
    try:
        user_info = cl.user_info_by_username_v1(username)
        pk = user_info.pk
    except Exception as e:
        logger.error(f"Could not retrieve user info for {username}: {e}")
        return

    # 2. Save user profile info to the database
    data_manager.upsert_account(
        username=user_info.username,
        insta_id=user_info.pk,
        follower_count=user_info.follower_count,
        following_count=user_info.following_count,
        full_name=user_info.full_name,
        url=f"https://www.instagram.com/{user_info.username}/",
        profile_pic_url=str(user_info.profile_pic_url),
        biography=user_info.biography
    )
    logger.info(f"Successfully upserted info for {user_info.username} (user_pk: {pk})")

    # 3. Check for reels and fetch if necessary
    existing_reels_count = data_manager.count_reels_for_user(pk)
    if existing_reels_count >= reels_to_fetch:
        logger.info(f"Found {existing_reels_count} reels for {username}, which meets the requirement of {reels_to_fetch}. Skipping fetch.")
    else:
        if existing_reels_count > 0:
            logger.info(f"Found {existing_reels_count} reels, which is less than requested {reels_to_fetch}. Deleting and re-fetching.")
            data_manager.delete_reels_for_user(pk)

        logger.info(f"Fetching {reels_to_fetch} reels for {username}...")
        try:
            reels = cl.user_clips_v1(pk, amount=reels_to_fetch)
            reels_data = [reel.model_dump(mode='json') for reel in reels]
            data_manager.save_reels(reels_data, pk)
            logger.info(f"Successfully saved {len(reels_data)} reels.")
        except Exception as e:
            logger.error(f"Could not fetch or save reels for {username}: {e}")
            return

    # 4. Get top reels and update the user's account
    top_reels_pks = data_manager.get_top_reels(user_pk=pk, limit=5)
    if top_reels_pks:
        reels_selected_list_json = json.dumps(top_reels_pks)
        data_manager.update_account_fields(
            username=username,
            reels_selected_list=reels_selected_list_json
        )
        logger.info(f"Updated account {username} with top {len(top_reels_pks)} reels.")

    logger.info(f"Process completed successfully for user: {username}")
    

def main():
    """Main application entry point."""
    logger.info("🚀 Starting Instagram Data Manager...")
    
    try:
        # Initialize managers
        data_manager = InstagramDataManager()
        bot_manager = BotManager()
        reels_to_fetch = 60 # Configuration for this run

        # Get an authenticated client before processing users
        logger.info("Attempting to log in with a bot...")
        cl = bot_manager.get_bot_client(bot_index=0)
        if not cl:
            logger.error("❌ Login failed. Cannot get an Instagram client. Aborting script.")
            return
        
        logger.info("✅ Login successful. Starting user processing.")

        # Ensure database is set up and synced
        sync_success = data_manager.ensure_sync()
        if not sync_success:
            logger.error("❌ Failed to start application due to sync issues. Please check CSV data.")
            return

        # Get all usernames from the database
        usernames = data_manager.get_database_usernames()
        if not usernames:
            logger.warning("No usernames found in the database to process.")
            return
        
        logger.info(f"Found {len(usernames)} users to evaluate.")
        
        processed_in_session_count = 0

        for i, username in enumerate(usernames):
            logger.info(f"--- Evaluating user {i+1}/{len(usernames)}: {username} ---")

            # Check if user is already processed sufficiently
            user_pk = data_manager.get_user_insta_id(username)
            if user_pk:
                existing_reels_count = data_manager.count_reels_for_user(user_pk)
                if existing_reels_count >= reels_to_fetch:
                    logger.info(f"User {username} already has {existing_reels_count} reels. Skipping.")
                    continue

            # If we are about to process a user, and it's not the first one in this session, wait.
            if processed_in_session_count > 0:
                sleep_time = random.randint(60, 120)
                logger.info(f"Waiting for {sleep_time} seconds before processing {username}...")
                time.sleep(sleep_time)

            process_user_reels(
                username=username, 
                cl=cl,
                data_manager=data_manager,
                reels_to_fetch=reels_to_fetch
            )
            processed_in_session_count += 1
            
        logger.info("✅ All users evaluated successfully.")

    except Exception as e:
        logger.error(f"❌ An unexpected error occurred in main: {e}")

if __name__ == '__main__':
    main()