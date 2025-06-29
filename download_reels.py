import asyncio
import aiohttp
import os
import random
import logging
import json
from db_manager import InstagramDataManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = 'data/reels'
os.makedirs(DATA_DIR, exist_ok=True)
THUMBNAIL_DIR = 'data/thumbnails'
os.makedirs(THUMBNAIL_DIR, exist_ok=True)

RETRY_LIMIT = 3
RETRY_DELAY = 60  # seconds

async def download_file(session, url, dest_path, pk, file_type):
    """Generic file downloader for videos and thumbnails."""
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(dest_path, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                logger.info(f"Downloaded {file_type} for {pk} to {dest_path}")
                return {'pk': pk, 'success': True, 'type': file_type}
            else:
                logger.error(f"Failed to download {file_type} for {pk}: HTTP {resp.status}")
                return {'pk': pk, 'success': False, 'type': file_type}
    except Exception as e:
        logger.error(f"Error downloading {file_type} for {pk}: {e}")
        return {'pk': pk, 'success': False, 'type': file_type}

async def process_user(session, username, reel_ids_to_download, data_manager, max_concurrent=5):
    any_downloaded = False
    failed_video_pks = []
    consecutive_video_failures = 0
    i = 0
    while i < len(reel_ids_to_download):
        tasks = []
        reels_in_batch = reel_ids_to_download[i:i+max_concurrent]
        i += max_concurrent

        for pk in reels_in_batch:
            # Video download task
            video_dest_path = os.path.join(DATA_DIR, f"{pk}.mp4")
            if os.path.exists(video_dest_path):
                data_manager.mark_reel_as_downloaded(pk)
            else:
                video_url = data_manager.get_reel_video_url(pk)
                if video_url:
                    tasks.append(download_file(session, video_url, video_dest_path, pk, 'video'))
                else:
                    logger.error(f"No video_url for reel {pk}, skipping video.")
            
            # Thumbnail download task
            thumb_dest_path = os.path.join(THUMBNAIL_DIR, f"{pk}.jpg")
            if not os.path.exists(thumb_dest_path):
                thumb_url = data_manager.get_reel_thumbnail_url(pk)
                if thumb_url:
                    tasks.append(download_file(session, thumb_url, thumb_dest_path, pk, 'thumbnail'))
                else:
                    logger.warning(f"No thumbnail_url for reel {pk}, skipping thumbnail.")
            
        if not tasks:
            continue

        results = await asyncio.gather(*tasks)
        
        video_results = [r for r in results if r['type'] == 'video']
        
        for res in video_results:
            pk = res['pk']
            if not res['success']:
                failed_video_pks.append(pk)
                consecutive_video_failures += 1
                logger.error(f"Failed to download video {pk}. Consecutive failures: {consecutive_video_failures}")
                if consecutive_video_failures >= 3:
                    logger.error("3 consecutive download failures. Waiting 3 seconds...")
                    await asyncio.sleep(3)
                    consecutive_video_failures = 0
                continue
            
            # On successful video download
            if failed_video_pks:
                for failed_pk in failed_video_pks:
                    data_manager.mark_reel_as_unavailable(failed_pk)
                failed_video_pks = []
            data_manager.mark_reel_as_downloaded(pk)
            any_downloaded = True
            consecutive_video_failures = 0

    # If there are any failed_pks left at the end, mark them as unavailable
    if failed_video_pks:
        for pk in failed_video_pks:
            data_manager.mark_reel_as_unavailable(pk)
            
    return True, any_downloaded

async def main():
    data_manager = InstagramDataManager()
    users = data_manager.get_all_selected_reels()
    if not users:
        logger.info("No users with selected reels found to process.")
        return

    # Flatten all reel PKs to check their status in one query
    all_pks_to_check = set()
    user_reel_map = {}
    for username, reels_json in users:
        try:
            pks = json.loads(reels_json)
            if pks:
                user_reel_map[username] = pks
                all_pks_to_check.update(pks)
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Could not parse reels_selected_list for {username}")
            continue

    if not all_pks_to_check:
        logger.info("No valid reel IDs found in selected lists.")
        return

    # Get a set of PKs that are already downloaded or unavailable
    reels_to_skip = data_manager.filter_reels_by_status(list(all_pks_to_check))
    logger.info(f"Found {len(reels_to_skip)} reels (out of {len(all_pks_to_check)}) that are already processed.")

    async with aiohttp.ClientSession() as session:
        for username, all_user_pks in user_reel_map.items():
            reels_to_download = [pk for pk in all_user_pks if pk not in reels_to_skip]

            if not reels_to_download:
                logger.info(f"All selected reels for user {username} are already processed. Skipping.")
                continue

            logger.info(f"Processing user: {username}, found {len(reels_to_download)} reel(s) to download.")
            ok, any_downloaded = await process_user(session, username, reels_to_download, data_manager)
            if not ok:
                logger.critical("Stopping script due to repeated download failure.")
                return
            if any_downloaded:
                block_delay = random.uniform(2, 3)
                logger.info(f"Waiting {block_delay:.2f}s before next user block...")
                await asyncio.sleep(block_delay)

if __name__ == "__main__":
    asyncio.run(main())
    # After downloads, extract audio from all downloaded videos
    from utility.extract_audio import extract_audio_for_all_downloaded_reels
    extract_audio_for_all_downloaded_reels() 