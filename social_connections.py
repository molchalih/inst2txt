from db_manager import InstagramDataManager
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting to populate followed_creators_with_reels_selected_list...")
    data_manager = InstagramDataManager()
    # Ensure the new column exists
    data_manager.add_followed_creators_with_reels_selected_list_column()

    # Get the mapping: (follower_username, follower_insta_id, followed_creators_insta_ids)
    results = data_manager.get_followed_creators_with_reels_selected_list()
    logger.info(f"Found {len(results)} users with followed creators to update.")

    for follower_username, follower_insta_id, followed_creators_json in results:
        # followed_creators_json is a JSON array string (may be '[null]' if none)
        try:
            followed_list = json.loads(followed_creators_json) if followed_creators_json else []
            # Remove nulls and self-follow if present
            followed_list = [fid for fid in followed_list if fid and fid != follower_insta_id]
            followed_list_json = json.dumps(followed_list)
            data_manager.update_followed_creators_with_reels_selected_list(follower_insta_id, followed_list_json)
            logger.info(f"Updated {follower_username} ({follower_insta_id}) with {len(followed_list)} followed creators.")
        except Exception as e:
            logger.error(f"Error processing {follower_username} ({follower_insta_id}): {e}")

if __name__ == "__main__":
    main()
