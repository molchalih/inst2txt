import json
import os
from acrcloud.recognizer import ACRCloudRecognizer
from dotenv import load_dotenv
from db_manager import InstagramDataManager
from pydub import AudioSegment

# --- ACRCloud Credentials ---
load_dotenv()
config = {
    'host': os.getenv('ACR_HOST'),
    'access_key': os.getenv('ACR_ACCESS_KEY'),
    'access_secret': os.getenv('ACR_ACCESS_SECRET'),
    'timeout': int(os.getenv('ACR_TIMEOUT', '10'))
}

recognizer = ACRCloudRecognizer(config)

def recognize_track(filepath):
    """
    Returns a dict with these keys:
      - track:  title of the song (string)
      - artist: artist name (string)
      - genres: list of genre names (possibly empty)
      - score:  recognition confidence as a float [0.0–100.0]
      - error:  (optional) error message if something went wrong

    If no music match is found, 'track' and 'artist' are empty and 'genres' is [].
    On exception, 'error' is set and other fields are empty/zero.
    """
    try:
        # Only analyze the first 30 seconds
        audio = AudioSegment.from_file(filepath)
        if len(audio) > 30_000:
            audio = audio[:30_000]
            temp_path = filepath + ".tmp30s.mp3"
            audio.export(temp_path, format="mp3")
            use_path = temp_path
        else:
            use_path = filepath
        with open(use_path, 'rb') as f:
            buf = f.read()
        result = recognizer.recognize_by_filebuffer(buf, 0)
        if use_path != filepath:
            os.remove(use_path)
        
        if not result:
            return {
                "track": "",
                "artist": "",
                "genres": [],
                "score": 0.0,
                "error": "ACRCloud returned empty result."
            }
            
        data = json.loads(result)

        music_list = data.get("metadata", {}).get("music", [])
        if not music_list:
            return {
                "track": "",
                "artist": "",
                "genres": [],
                "score": 0.0
            }

        top = max(music_list, key=lambda t: t.get("score", 0))
        title   = top.get("title", "")
        artist  = top.get("artists", [{}])[0].get("name", "")
        genres  = [g.get("name", "") for g in top.get("genres", [])]
        score   = float(top.get("score", 0.0))

        return {
            "track": title,
            "artist": artist,
            "genres": genres,
            "score": score
        }

    except Exception as e:
        return {
            "track": "",
            "artist": "",
            "genres": [],
            "score": 0.0,
            "error": str(e)
        }

def analyze_selected_reels():
    data_manager = InstagramDataManager()
    load_dotenv()
    
    recognition_score_threshold = int(os.getenv("POLICY_MUSIC_RECOGNITION_SCORE", 70))

    all_selected_pks = data_manager.get_all_selected_reel_pks()
    if not all_selected_pks:
        print("No selected reels were found to analyze.")
        return

    reels_to_analyze = data_manager.get_reels_for_music_analysis(all_selected_pks)

    if not reels_to_analyze:
        print("All selected reels have already been analyzed for audio type or have no audio track.")
        return
        
    print(f"Found {len(reels_to_analyze)} reels to analyze for music content.")

    for pk in reels_to_analyze:
        audio_path = f"data/audio/{pk}.mp3"
        if not os.path.exists(audio_path):
            print(f"Reel {pk}: audio file not found at {audio_path}")
            continue
        result = recognize_track(audio_path)
        score = result.get("score", 0.0)

        if "error" in result:
            print(f"Reel {pk}: recognition error: {result['error']}")

        if score > recognition_score_threshold:
            genres = result.get("genres", [])
            genre_str = ", ".join(genres) if genres else "unknown"
            content = f"{result.get('track', '')} - {result.get('artist', '')} (genre: {genre_str})"
            data_manager.set_audio_info(pk, audio_type="music", audio_content=content)
            print(f"Reel {pk}: music detected, score={score}, content={content}")
        else:
            data_manager.set_audio_info(pk, audio_type="speech", audio_content="")
            print(f"Reel {pk}: speech or low score ({score})")


if __name__ == "__main__":
    analyze_selected_reels()
