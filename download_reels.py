import asyncio
import aiohttp
import os
import random
import time
import logging
import json
from db_manager import InstagramDataManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = 'data/reels'
os.makedirs(DATA_DIR, exist_ok=True)

RETRY_LIMIT = 3
RETRY_DELAY = 60  # seconds

async def download_video(session, url, dest_path, pk):
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(dest_path, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                logger.info(f"Downloaded {pk} to {dest_path}")
                return pk, True
            else:
                logger.error(f"Failed to download {pk}: HTTP {resp.status}")
                return pk, False
    except Exception as e:
        logger.error(f"Error downloading {pk}: {e}")
        return pk, False

async def process_user(session, username, reels_selected_list, data_manager, max_concurrent=5):
    try:
        reel_ids = json.loads(reels_selected_list)
    except Exception as e:
        logger.error(f"Could not parse reels_selected_list for {username}: {e}")
        return True, False  # skip user, no downloads
    any_downloaded = False
    failed_pks = []
    consecutive_failures = 0
    i = 0
    while i < len(reel_ids):
        batch = []
        for _ in range(max_concurrent):
            if i >= len(reel_ids):
                break
            pk = reel_ids[i]
            dest_path = os.path.join(DATA_DIR, f"{pk}.mp4")
            file_exists = os.path.exists(dest_path)
            db_downloaded = data_manager.is_reel_downloaded(pk)
            db_unavailable = data_manager.is_reel_unavailable(pk)
            # If file exists but DB says not downloaded, update DB and skip
            if file_exists and not db_downloaded:
                data_manager.mark_reel_as_downloaded(pk)
                logger.info(f"File {dest_path} exists but DB not marked. Marked as downloaded in DB. Skipping download.")
                i += 1
                continue
            # If DB says downloaded or unavailable, skip
            if db_downloaded or db_unavailable:
                logger.info(f"Reel {pk} already downloaded or marked unavailable. Skipping.")
                i += 1
                continue
            video_url = data_manager.get_reel_video_url(pk)
            if not video_url:
                logger.error(f"No video_url found for reel {pk} (user {username}). Skipping.")
                i += 1
                continue
            batch.append((pk, video_url, dest_path))
            i += 1
        if not batch:
            continue
        tasks = [download_video(session, url, dest_path, pk) for pk, url, dest_path in batch]
        results = await asyncio.gather(*tasks)
        for pk, success in results:
            if not success:
                failed_pks.append(pk)
                consecutive_failures += 1
                logger.error(f"Failed to download {pk}. Consecutive failures: {consecutive_failures}")
                if consecutive_failures >= 3:
                    logger.error(f"3 consecutive download failures. Waiting 3 seconds before continuing...")
                    await asyncio.sleep(3)
                    consecutive_failures = 0  # reset after wait
                continue  # No delay after failed download
            # If we get here, the video was downloaded successfully
            if failed_pks:
                for failed_pk in failed_pks:
                    data_manager.mark_reel_as_unavailable(failed_pk)
                    logger.info(f"Marked {failed_pk} as video_unavailable after subsequent success.")
                failed_pks = []
            data_manager.mark_reel_as_downloaded(pk)
            any_downloaded = True
            consecutive_failures = 0
    # If there are any failed_pks left at the end, mark them as unavailable
    if failed_pks:
        for failed_pk in failed_pks:
            data_manager.mark_reel_as_unavailable(failed_pk)
            logger.info(f"Marked {failed_pk} as video_unavailable at end of user block.")
    return True, any_downloaded

async def main():
    data_manager = InstagramDataManager()
    users = data_manager.get_users_with_reels_selected_list()
    async with aiohttp.ClientSession() as session:
        for username, reels_selected_list in users:
            logger.info(f"Processing user: {username}")
            ok, any_downloaded = await process_user(session, username, reels_selected_list, data_manager)
            if not ok:
                logger.critical("Stopping script due to repeated download failure.")
                return
            if any_downloaded:
                block_delay = random.randint(3, 6)
                logger.info(f"Waiting {block_delay}s before next user block...")
                await asyncio.sleep(block_delay)

if __name__ == "__main__":
    asyncio.run(main())
    # After downloads, extract audio from all downloaded videos
    from utility.extract_audio import extract_audio_for_all_downloaded_reels
    extract_audio_for_all_downloaded_reels() 