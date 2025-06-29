import os
from moviepy import VideoFileClip
from db_manager import InstagramDataManager

def extract_audio_from_video(video_path, audio_dir='data/audio', db_path='data/instagram_data.db'):
    """
    Extracts audio from a video file and saves it as an mp3 in the specified directory.
    The output file is named <video_id>.mp3, where <video_id> is the stem of the video filename.
    If the audio file already exists, the function does nothing.
    Returns the path to the audio file.
    If no audio track is found, sets a flag in the database.
    """
    os.makedirs(audio_dir, exist_ok=True)
    video_id = os.path.splitext(os.path.basename(video_path))[0]
    audio_path = os.path.join(audio_dir, f"{video_id}.mp3")
    if os.path.exists(audio_path):
        print(f"Audio already exists: {audio_path}")
        return audio_path
    try:
        with VideoFileClip(video_path) as video:
            audio = video.audio
            if audio is None:
                print(f"No audio track found in video: {video_path}")
                manager = InstagramDataManager(db_path=db_path)
                manager.set_no_audio_flag(video_id)
                return None
            audio.write_audiofile(audio_path, codec='mp3')
        print(f"Extracted audio to: {audio_path}")
        return audio_path
    except Exception as e:
        print(f"Failed to extract audio: {e}")
        return None


def extract_audio_for_all_downloaded_reels(reels_dir='data/reels', audio_dir='data/audio', db_path='data/instagram_data.db'):
    """
    Go through all .mp4 files in the reels_dir and extract audio if not already present in audio_dir.
    If no audio track is found, set a flag in the database.
    """
    mp4_files = [f for f in os.listdir(reels_dir) if f.endswith('.mp4')]
    print(f"Found {len(mp4_files)} video files in {reels_dir}.")
    for mp4_file in mp4_files:
        pk = os.path.splitext(mp4_file)[0]
        video_path = os.path.join(reels_dir, mp4_file)
        audio_path = os.path.join(audio_dir, f"{pk}.mp3")
        if os.path.exists(audio_path):
            print(f"Audio already exists: {audio_path}")
            continue
        extract_audio_from_video(video_path, audio_dir=audio_dir, db_path=db_path)

if __name__ == "__main__":
    # Example usage
    # video_file = "data/reels/2926561024337436970.mp4"
    # extract_audio_from_video(video_file)
    # Uncomment to process all downloaded reels:
    extract_audio_for_all_downloaded_reels() 