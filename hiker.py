import os
import json
import time
import random
from dotenv import load_dotenv
from hikerapi import Client
from db_manager import InstagramDataManager
import logging
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_user_with_retry(username, cl, data_manager, max_retries=5):
    attempts = 0
    while attempts < max_retries:
        try:
            response = cl.user_by_username_v2(username)
            status_code = getattr(response, 'status_code', None)
            # Improved user not found check
            user_not_found = (
                not response or
                "user" not in response or
                (
                    isinstance(response, dict) and
                    response.get("detail", "").startswith("Target user not found") and
                    response.get("exc_type") == "UserNotFound"
                )
            )
            if user_not_found:
                logger.warning(f"User {username} not found (UserNotFound). Marking as processed.")
                data_manager.update_account_fields(
                    username=username,
                    all_reels_fetched_hiker=True,
                    all_following_fetched_hiker=True
                )
                return None  # User not found, skip further processing
            return response["user"]
        except httpx.RequestError as e:
            attempts += 1
            logger.error(f"Network error for {username}: {e}. Attempt {attempts}/{max_retries}. Retrying in 30s...")
            time.sleep(30)
        except Exception as e:
            attempts += 1
            logger.error(f"Unexpected error for {username}: {e}. Attempt {attempts}/{max_retries}. Retrying in 30s...")
            time.sleep(30)
    logger.critical(f"Failed to fetch user {username} after {max_retries} attempts. Exiting script.")
    exit(1)

def process_user_with_hiker(
    username: str,
    cl: Client,
    data_manager: InstagramDataManager,
    reels_to_fetch: int = 60,
    fetch_reels: bool = True,
    fetch_following: bool = True,
    max_following: int = 800,
    min_followers: int = 10000
):
    """
    Process a single user using the Hiker API.
    """
    logger.info(f"Processing user: {username}")
    try:
        # 1. Get user info with retry logic
        user = fetch_user_with_retry(username, cl, data_manager)
        if user is None:
            return

        # Access attributes directly from the user object
        pk = str(user.get("pk"))
        following_count = user.get("following_count", 0)
        followers_count = user.get("follower_count", 0)
        
        # Upsert user info right away
        data_manager.upsert_account(
            username=user.get("username", ""),
            insta_id=pk,
            follower_count=user.get("follower_count", 0),
            following_count=following_count,
            full_name=user.get("full_name", ""),
            url=f"https://www.instagram.com/{user.get('username', '')}/",
            profile_pic_url=str(user.get("profile_pic_url", "")),
            biography=user.get("biography", "")
        )
        logger.info(f"Upserted user info for {username}")

        # Check for high following count first
        if following_count > max_following or followers_count < min_followers:
            logger.info(f"User {username} has a high following count ({following_count}) or low followers count ({followers_count}). Skipping following and reel processing.")
            data_manager.update_account_fields(username=username, all_following_fetched_hiker=True, all_reels_fetched_hiker=True)
            return

        # 2. Get reels
        if fetch_reels:
            logger.info(f"Starting reel fetching for {username}")
            reels = []
            next_page_id = None
            try:
                # Initial request for reels
                max_retries = 5
                attempts = 0
                while attempts < max_retries:
                    try:
                        response = cl.user_clips_v2(pk)
                        break
                    except httpx.RequestError as e:
                        attempts += 1
                        logger.error(f"Network error fetching reels for {username}: {e}. Attempt {attempts}/{max_retries}. Retrying in 30s...")
                        time.sleep(30)
                else:
                    logger.error(f"Failed to fetch reels for {username} after {max_retries} attempts. Skipping user.")
                    return
                # Check for reels on the first page
                if not response or 'response' not in response or not response['response'].get('items'):
                    logger.info(f"User {username} has 0 reels. Skipping all further processing.")
                    data_manager.update_account_fields(username=username, all_reels_fetched_hiker=True, all_following_fetched_hiker=True)
                    return # Stop processing this user
                # If we are here, process the first page and continue
                reels.extend(response['response']['items'])
                next_page_id = response.get('next_page_id')
                while len(reels) < reels_to_fetch and next_page_id:
                    attempts = 0
                    while attempts < max_retries:
                        try:
                            response = cl.user_clips_v2(pk, page_id=next_page_id)
                            break
                        except httpx.RequestError as e:
                            attempts += 1
                            logger.error(f"Network error fetching reels page for {username}: {e}. Attempt {attempts}/{max_retries}. Retrying in 30s...")
                            time.sleep(30)
                    else:
                        logger.error(f"Failed to fetch reels page for {username} after {max_retries} attempts. Marking as fully fetched.")
                        data_manager.update_account_fields(username=username, all_reels_fetched_hiker=True)
                        break
                    if not response or 'response' not in response or 'items' not in response['response']:
                        data_manager.update_account_fields(username=username, all_reels_fetched_hiker=True)
                        logger.info(f"No more reels found for {username} on this page. Marking as fully fetched.")
                        break
                    reels.extend(response['response']['items'])
                    next_page_id = response.get('next_page_id')
                    if not next_page_id:
                        data_manager.update_account_fields(username=username, all_reels_fetched_hiker=True)
                        logger.info(f"Reached the last page of reels for {username}.")
                        break
            except Exception as e:
                logger.error(f"An error occurred while fetching reels for {username}: {e}", exc_info=True)
            reels = reels[:reels_to_fetch]
            if reels:
                reels_data = []
                for item in reels:
                    if 'media' in item:
                        media = item['media']
                        # Ensure 'caption' is present as a dict or None
                        if 'caption' not in media:
                            media['caption'] = None
                        reels_data.append(media)
                data_manager.save_reels(reels_data, user_pk=pk)
                logger.info(f"Saved {len(reels_data)} reels for {username}")
                top_reels_pks = data_manager.get_top_reels(user_pk=pk, limit=5)
                if top_reels_pks:
                    reels_selected_list_json = json.dumps(top_reels_pks)
                    data_manager.update_account_fields(
                        username=username,
                        reels_selected_list=reels_selected_list_json
                    )
                    logger.info(f"Updated account {username} with top {len(top_reels_pks)} reels.")
            # Always mark as fully fetched after processing all available reels
            data_manager.update_account_fields(username=username, all_reels_fetched_hiker=True)
        else:
            logger.info(f"Skipping reel fetching for {username} as already complete or sufficient.")

        # 3. Get following
        if fetch_following:
            logger.info(f"Starting following fetching for {username}")
            following_next_page_id = None
            while True:
                max_retries = 5
                attempts = 0
                while attempts < max_retries:
                    try:
                        response = cl.user_following_v2(pk, page_id=following_next_page_id) if following_next_page_id else cl.user_following_v2(pk)
                        break
                    except httpx.RequestError as e:
                        attempts += 1
                        logger.error(f"Network error fetching following for {username}: {e}. Attempt {attempts}/{max_retries}. Retrying in 30s...")
                        time.sleep(30)
                else:
                    logger.error(f"Failed to fetch following for {username} after {max_retries} attempts. Marking as fully fetched.")
                    data_manager.update_account_fields(username=username, all_following_fetched_hiker=True)
                    break
                if not response or 'response' not in response or 'users' not in response['response']:
                    logger.info(f"No more following found for {username}. Marking as fully fetched.")
                    data_manager.update_account_fields(username=username, all_following_fetched_hiker=True)
                    break
                following = response['response']['users']
                if following:
                    data_manager.save_following(following, user_pk=pk)
                following_next_page_id = response.get('next_page_id')
                if not following_next_page_id:
                    logger.info(f"Reached the last page of following for {username}. Marking as fully fetched.")
                    data_manager.update_account_fields(username=username, all_following_fetched_hiker=True)
                    break
                else:
                    logger.info(f"Fetched a page of following for {username}, proceeding to next page.")
                    time.sleep(random.uniform(0.3, 1.2))
        else:
            logger.info(f"Skipping following fetching for {username} as already complete.")

    except Exception as e:
        logger.error(f"An error occurred while processing {username}: {e}", exc_info=True)

def main():
    """
    Main function to run the Hiker API-based scraping.
    """
    load_dotenv()
    HIKER_API_TOKEN = os.getenv("HIKER_API_TOKEN")

    if not HIKER_API_TOKEN:
        logger.critical("HIKER_API_TOKEN not found in .env file. Please add it.")
        return

    hiker_client = Client(token=HIKER_API_TOKEN)
    data_manager = InstagramDataManager()

    # Add only new usernames from data/new.csv to the database
    data_manager.add_new_usernames_from_csv_path('data/new.csv')

    # Fill missing reels_selected_list for users with reels but no selected reels
    data_manager.fill_missing_reels_selected_list(top_n=5)

    usernames = data_manager.get_database_usernames()
    if not usernames:
        logger.warning("No usernames found in the database.")
        return
        
    logger.info(f"Found {len(usernames)} users to process with Hiker API.")
    reels_to_fetch = 60
    max_following = 1001
    min_followers = 10000

    for username in usernames:
        # Check statuses
        existing_reels_count, all_reels_fetched = data_manager.get_user_hiker_status(username)
        all_following_fetched = data_manager.get_user_following_hiker_status(username)

        reels_done = all_reels_fetched or (existing_reels_count >= reels_to_fetch)
        following_done = all_following_fetched

        if reels_done and following_done:
            logger.info(f"User {username} is fully processed for both reels and following. Skipping.")
            continue

        process_user_with_hiker(
            username,
            hiker_client,
            data_manager,
            reels_to_fetch=reels_to_fetch,
            fetch_reels=(not reels_done),
            fetch_following=(not following_done),
            max_following=max_following,
            min_followers=min_followers
        )
        # Add a small delay to be respectful to the API
        sleep_time = random.uniform(0.1, 0.2)  # Using uniform for floating point numbers
        logger.info(f"Waiting for {sleep_time} seconds...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()