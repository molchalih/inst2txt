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
    users = data_manager.get_all_selected_reels()
    for username, reels_selected_list in users:
        try:
            reel_ids = json.loads(reels_selected_list)
        except Exception as e:
            print(f"Could not parse reels_selected_list for {username}: {e}")
            continue
        for pk in reel_ids:
            # Check if audio_type is already set
            import sqlite3
            conn = sqlite3.connect('data/instagram_data.db')
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(reels)")
            columns = [row[1] for row in cursor.fetchall()]
            audio_type = None
            if 'audio_type' in columns:
                cursor.execute("SELECT audio_type FROM reels WHERE pk = ?", (pk,))
                row = cursor.fetchone()
                if row and row[0]:
                    audio_type = row[0]
            if audio_type:
                print(f"Reel {pk}: already has audio_type '{audio_type}', skipping.")
                conn.close()
                continue
            # Check for no_audio flag
            no_audio = False
            if 'no_audio' in columns:
                cursor.execute("SELECT no_audio FROM reels WHERE pk = ?", (pk,))
                row = cursor.fetchone()
                if row and row[0]:
                    no_audio = True
            conn.close()
            if no_audio:
                data_manager.set_audio_info(pk, audio_type="none", audio_content="")
                print(f"Reel {pk}: no audio (flagged)")
                continue
            audio_path = f"data/audio/{pk}.mp3"
            if not os.path.exists(audio_path):
                print(f"Reel {pk}: audio file not found at {audio_path}")
                continue
            result = recognize_track(audio_path)
            score = result.get("score", 0.0)
            if score > 70:
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
