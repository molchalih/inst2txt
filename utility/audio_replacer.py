import whisper
import os
import logging
import sqlite3
import torch
import torchaudio
from typing import List, Tuple, Optional

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "data/instagram_data.db"
AUDIO_SOURCE_DIR = "data/audio"

class AudioReprocessor:
    """
    A self-contained class to re-process speech audio files for a specific database.
    """
    def __init__(self, model_name: str = "medium"):
        self.db_path = DB_PATH
        self.audio_dir = AUDIO_SOURCE_DIR
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        logger.info(f"Initializing AudioReprocessor with device: {self.device}")
        
        # Load Whisper model
        try:
            self.model = whisper.load_model(model_name, device=self.device)
            logger.info(f"Whisper '{model_name}' model loaded successfully on '{self.device}'.")
        except Exception as e:
            logger.critical(f"Failed to load Whisper model: {e}", exc_info=True)
            raise
        
        # Transcription options for translation
        self.transcribe_options = {
            "task": "translate",  # Translate to English
            "beam_size": 5,
            "patience": 2,
            "suppress_tokens": "-1",
            "temperature": (0.0, 0.2, 0.4, 0.6, 0.8, 1.0),
        }

    def _get_db_connection(self):
        """Establishes a connection to the SQLite database."""
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return None

    def get_reels_to_reprocess(self) -> List[Tuple[str, str]]:
        """
        Fetches all reels from the primary database where audio_type is 'speech'.
        Returns a list of (pk, video_url) tuples.
        """
        reels_to_process = []
        conn = self._get_db_connection()
        if not conn:
            return []
            
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT pk, video_url FROM reels WHERE audio_type = 'speech'")
            reels_to_process = cursor.fetchall()
            logger.info(f"Found {len(reels_to_process)} reels marked as 'speech' to re-process.")
        except sqlite3.Error as e:
            logger.error(f"Failed to fetch reels for reprocessing: {e}")
        finally:
            conn.close()
            
        return reels_to_process

    def update_reel_transcription(self, pk: str, transcription: str):
        """Updates the audio_content for a specific reel in the database."""
        conn = self._get_db_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()
            # We are confident the audio_type is 'speech', so we just update the content.
            cursor.execute("UPDATE reels SET audio_content = ? WHERE pk = ?", (transcription, pk))
            conn.commit()
            logger.info(f"Successfully updated transcription for reel {pk}.")
        except sqlite3.Error as e:
            logger.error(f"Failed to update transcription for reel {pk}: {e}")
        finally:
            conn.close()

    def flag_reel_as_no_audio(self, pk: str):
        """Sets no_audio = 1 and audio_type = NULL for a reel, e.g., for long audio."""
        conn = self._get_db_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE reels SET audio_content = ?, no_audio = 1, audio_type = NULL WHERE pk = ?", ("", pk))
            conn.commit()
            logger.info(f"Flagged reel {pk} as having no audio (e.g., too long).")
        except sqlite3.Error as e:
            logger.error(f"Failed to flag reel {pk} as no audio: {e}")
        finally:
            conn.close()

    def detect_language(self, audio_path: str) -> Optional[str]:
        """Detects the language of an audio file using the first 30 seconds."""
        try:
            audio = whisper.load_audio(audio_path)
            audio = whisper.pad_or_trim(audio)
            mel = whisper.log_mel_spectrogram(audio, n_mels=self.model.dims.n_mels).to(self.model.device)
            _, probs = self.model.detect_language(mel)

            if not isinstance(probs, dict):
                logger.warning(f"Language detection returned unexpected type: {type(probs)}")
                return None
            
            detected_language = max(probs, key=lambda k: probs.get(k, 0))
            assert isinstance(detected_language, str)
            confidence = probs.get(detected_language, 0.0)
            logger.info(f"Detected language: {detected_language} (Confidence: {confidence:.2f}) for {os.path.basename(audio_path)}")
            return detected_language
        except Exception as e:
            logger.error(f"Error during language detection for {audio_path}: {e}", exc_info=True)
            return None

    def transcribe_and_translate(self, pk: str) -> Optional[str]:
        """
        Performs language detection and transcription with translation for a single audio file.
        """
        audio_path = os.path.join(self.audio_dir, f"{pk}.mp3")
        if not os.path.exists(audio_path):
            logger.warning(f"Audio file not found for reel {pk} at {audio_path}. Skipping.")
            return None
        
        # Check audio duration
        try:
            info = torchaudio.info(audio_path)
            duration = info.num_frames / info.sample_rate
            if duration > 600:  # 10 minutes
                logger.warning(f"Audio for reel {pk} is longer than 10 minutes ({duration:.2f}s). Flagging and skipping.")
                self.flag_reel_as_no_audio(pk)
                return None
        except Exception as e:
            logger.error(f"Could not determine audio duration for {pk}: {e}. Skipping.")
            return None
        
        # 1. Detect language
        detected_language = self.detect_language(audio_path)
        if not detected_language:
            logger.error(f"Could not detect language for {pk}, aborting transcription.")
            return None

        # 2. Transcribe and translate
        try:
            logger.info(f"Transcribing and translating {audio_path} (lang: {detected_language})...")
            full_audio = whisper.load_audio(audio_path)
            
            options = self.transcribe_options.copy()
            options["language"] = detected_language
            
            result = self.model.transcribe(full_audio, **options)
            transcription = str(result.get("text", "")).strip()

            if transcription:
                logger.info(f"Successfully transcribed reel {pk}. Length: {len(transcription)} chars.")
                return transcription
            else:
                logger.warning(f"Transcription for reel {pk} resulted in empty text.")
                return None
        except Exception as e:
            logger.error(f"Error during transcription for reel {pk}: {e}", exc_info=True)
            return None

    def run_reprocessing(self):
        """Orchestrates the entire reprocessing workflow."""
        logger.info("--- Starting Audio Reprocessing Workflow ---")
        reels = self.get_reels_to_reprocess()
        
        if not reels:
            logger.info("No reels to reprocess. Exiting.")
            return
            
        total_reels = len(reels)
        processed_count = 0
        updated_count = 0

        for i, (pk, _) in enumerate(reels):
            logger.info(f"--- Processing reel {i+1}/{total_reels} (PK: {pk}) ---")
            processed_count += 1
            
            new_transcription = self.transcribe_and_translate(pk)
            
            if new_transcription:
                self.update_reel_transcription(pk, new_transcription)
                updated_count += 1
                logger.info(f"--- Finished processing reel {pk} (SUCCESS) ---")
            else:
                logger.warning(f"--- Finished processing reel {pk} (FAILED) ---")

        logger.info("--- Audio Reprocessing Workflow Finished ---")
        logger.info(f"Summary: Scanned {total_reels} reels.")
        logger.info(f"Attempted to process: {processed_count} reels.")
        logger.info(f"Successfully updated: {updated_count} reels in the database.")

if __name__ == "__main__":
    reprocessor = AudioReprocessor()
    reprocessor.run_reprocessing()
