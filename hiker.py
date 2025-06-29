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

def download_avatar(username: str, insta_id: str, profile_pic_url: str, max_retries: int = 3, delay: int = 3):
    """
    Downloads a user's avatar with retries.
    """
    if not profile_pic_url:
        logger.warning(f"No profile_pic_url for {username}, skipping avatar download.")
        return
    if not insta_id:
        logger.warning(f"No insta_id for {username}, cannot save avatar.")
        return

    avatar_dir = "data/avatars"
    os.makedirs(avatar_dir, exist_ok=True)
    avatar_path = os.path.join(avatar_dir, f"{insta_id}.jpg")

    if os.path.exists(avatar_path):
        return

    for attempt in range(max_retries):
        try:
            with httpx.stream("GET", profile_pic_url, timeout=20) as response:
                response.raise_for_status()
                with open(avatar_path, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
                logger.info(f"Successfully downloaded avatar for {username} (ID: {insta_id}) to {avatar_path}")
                return
        except httpx.HTTPStatusError as e:
            logger.warning(f"Failed to download avatar for {username} (ID: {insta_id}). Status: {e.response.status_code}. Attempt {attempt + 1}/{max_retries}.")
        except httpx.RequestError as e:
            logger.error(f"Network error downloading avatar for {username} (ID: {insta_id}): {e}. Attempt {attempt + 1}/{max_retries}.")
        except Exception as e:
            logger.error(f"An unexpected error occurred during avatar download for {username} (ID: {insta_id}): {e}. Attempt {attempt + 1}/{max_retries}.")

        if attempt < max_retries - 1:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    
    logger.error(f"Failed to download avatar for {username} (ID: {insta_id}) after {max_retries} attempts.")

def fetch_user_with_retry(username, cl, data_manager, max_retries=5):
    attempts = 0
    while attempts < max_retries:
        try:
            response = cl.user_by_username_v2(username)
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
        profile_pic_url = str(user.get("profile_pic_url", ""))
        
        # Upsert user info right away
        data_manager.upsert_account(
            username=user.get("username", ""),
            insta_id=pk,
            follower_count=user.get("follower_count", 0),
            following_count=following_count,
            full_name=user.get("full_name", ""),
            url=f"https://www.instagram.com/{user.get('username', '')}/",
            profile_pic_url=profile_pic_url,
            biography=user.get("biography", "")
        )
        logger.info(f"Upserted user info for {username}")

        # Download avatar if it doesn't exist
        download_avatar(username, pk, profile_pic_url)

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
                        logger.error(f"Network error fetching reels for {username}: {e}. Attempt {attempts}/{max_retries}. Retrying in 5s...")
                        time.sleep(5)
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

    users_with_status = data_manager.get_hiker_processing_status_for_all_users()
    if not users_with_status:
        logger.warning("No users found in the database.")
        return
        
    logger.info(f"Found {len(users_with_status)} users to evaluate with Hiker API.")

    # Load policy variables from .env or use defaults
    reels_to_fetch = int(os.getenv("POLICY_REELS_TO_FETCH", 60))
    max_following = int(os.getenv("POLICY_MAX_FOLLOWING", 1001))
    min_followers = int(os.getenv("POLICY_MIN_FOLLOWERS", 10000))

    users_to_process = []
    for username, _insta_id, all_reels_fetched, all_following_fetched, _ in users_with_status:
        reels_done = bool(all_reels_fetched)
        following_done = bool(all_following_fetched)

        if not (reels_done and following_done):
            users_to_process.append({
                "username": username, 
                "reels_done": reels_done, 
                "following_done": following_done
            })
    
    total_users = len(users_with_status)
    logger.info(f"Filtered down to {len(users_to_process)} users needing processing out of {total_users} total.")

    for user_data in users_to_process:
        username = user_data["username"]
        reels_done = user_data["reels_done"]
        following_done = user_data["following_done"]
        
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
        sleep_time = random.uniform(0.1, 0.2)
        logger.info(f"Waiting for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    # Fill missing reels_selected_list for any users who might have been missed.
    logger.info("Running a final check to fill any missing selected reels lists...")
    data_manager.fill_missing_reels_selected_list(top_n=5)

if __name__ == "__main__":
    main()