import os
import subprocess
import logging
from db_manager import InstagramDataManager

# Configure logging, but we'll use print for the final summary for clarity
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_audio_from_video(video_path, audio_dir='data/audio', db_path='data/instagram_data.db'):
    """
    Extracts audio from a video file using ffmpeg and saves it as an mp3.
    Returns a status tuple: (success: bool, reason: str).
    Reasons: 'exists', 'extracted', 'no_audio', 'failed'.
    """
    os.makedirs(audio_dir, exist_ok=True)
    video_id = os.path.splitext(os.path.basename(video_path))[0]
    audio_path = os.path.join(audio_dir, f"{video_id}.mp3")

    if os.path.exists(audio_path):
        return True, 'exists'

    try:
        command = [
            'ffmpeg',
            '-i', video_path,
            '-hide_banner', '-loglevel', 'error', # Suppress verbose output
            '-vn',          # No video output
            '-c:a', 'mp3',  # Set audio codec to mp3
            '-y',           # Overwrite output without asking
            audio_path
        ]
        
        result = subprocess.run(command, capture_output=True, text=True, check=False)

        if result.returncode != 0:
            # Check if the error is due to a missing audio stream
            if "does not contain any stream" in result.stderr or "Stream map 'a' matches no streams" in result.stderr:
                logger.warning(f"No audio track found in video: {video_path}")
                manager = InstagramDataManager(db_path=db_path)
                manager.set_no_audio_flag(video_id)
                return False, 'no_audio'
            else:
                # Log other ffmpeg errors
                logger.error(f"Failed to extract audio from {video_path}. FFMPEG stderr:\n{result.stderr}")
                return False, 'failed'
        
        # Success
        logger.info(f"Extracted audio for {video_id}")
        return True, 'extracted'

    except FileNotFoundError:
        logger.error("ffmpeg is not installed or not in the system's PATH. Please install ffmpeg.")
        # Stop the whole process if ffmpeg is not found
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {video_path}: {e}")
        return False, 'failed'


def extract_audio_for_all_downloaded_reels(reels_dir='data/reels', audio_dir='data/audio', db_path='data/instagram_data.db'):
    """
    Go through all .mp4 files in the reels_dir, extract audio using ffmpeg,
    and print a summary of the operations at the end.
    """
    if not os.path.isdir(reels_dir):
        logger.warning(f"Reels directory not found: {reels_dir}")
        return

    mp4_files = [f for f in os.listdir(reels_dir) if f.endswith('.mp4')]
    
    total_videos = len(mp4_files)
    already_exists = 0
    newly_extracted = 0
    no_audio = 0
    failed = 0
    
    logger.info(f"Found {total_videos} video files in {reels_dir}. Starting audio extraction...")

    for i, mp4_file in enumerate(mp4_files):
        video_path = os.path.join(reels_dir, mp4_file)
        
        # Simple progress indicator
        print(f"\rProcessing video {i+1}/{total_videos}...", end="")

        try:
            success, reason = extract_audio_from_video(video_path, audio_dir=audio_dir, db_path=db_path)
            if success:
                if reason == 'exists':
                    already_exists += 1
                elif reason == 'extracted':
                    newly_extracted += 1
            else:
                if reason == 'no_audio':
                    no_audio += 1
                else:
                    failed += 1
        except FileNotFoundError:
            # The exception from the child function will bubble up. We stop here.
            return
    
    # Print final summary
    print("\n\n--- Audio Extraction Summary ---")
    print(f"Total videos scanned: {total_videos}")
    print(f"Audio files already existing: {already_exists}")
    print(f"Newly extracted audio files: {newly_extracted}")
    print(f"Videos with no audio track: {no_audio}")
    print(f"Failed extractions: {failed}")
    print("--------------------------------")


if __name__ == "__main__":
    extract_audio_for_all_downloaded_reels() 