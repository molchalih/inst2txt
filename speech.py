import whisper
import os
import logging
from typing import Optional
import torch
import torchaudio
from db_manager import InstagramDataManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SpeechProcessor:
    def __init__(self, db_path: str = "data/instagram_data.db"):
        """Initialize the speech processor with database connection and Whisper model."""
        self.db_manager = InstagramDataManager(db_path)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Speech processor using device: {self.device}")

        self.model = whisper.load_model("medium", device=self.device)
        self.transcribe_options = {
            "task": "translate",
            "beam_size": 5,
            "patience": 2,
            "suppress_tokens": "-1",
            "temperature": (0.0, 0.2, 0.4, 0.6, 0.8, 1.0),
        }
        self.audio_dir = "data/audio"
        logger.info(f"Speech processor initialized with Whisper 'medium' model on {self.device}")
        logger.info(f"Audio directory: {self.audio_dir}")
    
    def get_audio_file_path(self, reel_id: str) -> Optional[str]:
        """Get the local audio file path for a reel ID."""
        audio_path = os.path.join(self.audio_dir, f"{reel_id}.mp3")
        if os.path.exists(audio_path):
            logger.info(f"Found audio file: {audio_path}")
            return audio_path
        else:
            logger.warning(f"Audio file not found: {audio_path}")
            return None
    
    def detect_language(self, audio_path: str) -> Optional[str]:
        """Detects the language of an audio file using the first 30 seconds."""
        try:
            logger.info(f"Detecting language for: {audio_path}")
            audio = whisper.load_audio(audio_path)
            # Process only the first 30 seconds for language detection
            audio = whisper.pad_or_trim(audio)
            mel = whisper.log_mel_spectrogram(audio, n_mels=self.model.dims.n_mels).to(self.model.device)
            
            # For a single audio segment, `probs` is a dictionary mapping language codes to probabilities.
            _, probs = self.model.detect_language(mel)
            if not isinstance(probs, dict):
                 logger.error(f"Unexpected output from language detection: {type(probs)}. Expected a dictionary.")
                 return None

            detected_language = max(probs, key=lambda k: probs.get(k, 0))
            assert isinstance(detected_language, str)
            confidence = probs.get(detected_language, 0.0)

            logger.info(f"Detected language: {detected_language} with confidence {confidence:.2f}")
            return detected_language
        except Exception as e:
            logger.error(f"Error detecting language for {audio_path}: {e}", exc_info=True)
            return None

    def transcribe_audio(self, audio_path: str) -> Optional[str]:
        """Transcribe audio using Whisper, with auto-detected language."""
        try:
            detected_language = self.detect_language(audio_path)
            if not detected_language:
                logger.error("Could not detect language, aborting transcription.")
                return None
            
            logger.info(f"Transcribing audio from {audio_path} (detected language: {detected_language})...")

            # Load the full audio for transcription
            audio = whisper.load_audio(audio_path)

            # Update transcribe options with detected language
            transcribe_options = self.transcribe_options.copy()
            transcribe_options["language"] = detected_language
            
            # Transcribe with Whisper
            result = self.model.transcribe(audio, **transcribe_options)
            
            transcription_text = str(result["text"]).strip()
            
            logger.info(f"Transcription completed. Length: {len(transcription_text)} chars")
            
            return transcription_text if transcription_text else None
            
        except Exception as e:
            logger.error(f"Error transcribing audio: {e}", exc_info=True)
            return None
    
    def process_speech_reels(self, batch_size: int = 10):
        """Process all reels with audio_type 'speech' in batches."""
        try:
            # Get reels to process from database
            reels_to_process = self.db_manager.get_speech_reels_to_process(batch_size)
            
            if not reels_to_process:
                logger.info("No reels with audio_type 'speech' found to process")
                return
            
            logger.info(f"Processing {len(reels_to_process)} reels")
            
            for pk, video_url in reels_to_process:
                logger.info(f"Processing reel {pk}")
                
                # Get local audio file path
                audio_path = self.get_audio_file_path(pk)
                if not audio_path:
                    logger.warning(f"Audio file not found for reel {pk}, marking as no_audio")
                    self.db_manager.mark_reel_as_no_audio_and_clear_type(pk)
                    continue

                # Check for long audio files
                try:
                    info = torchaudio.info(audio_path)
                    duration = info.num_frames / info.sample_rate
                    if duration > 600:  # 10 minutes
                        logger.warning(f"Audio for reel {pk} is longer than 10 minutes ({duration:.2f}s). Flagging and skipping.")
                        self.db_manager.mark_reel_as_no_audio_and_clear_type(pk)
                        continue
                except Exception as e:
                    logger.error(f"Could not determine audio duration for reel {pk}: {e}. Flagging and skipping.")
                    self.db_manager.mark_reel_as_no_audio_and_clear_type(pk)
                    continue
                
                # Transcribe audio
                transcription = self.transcribe_audio(audio_path)
                
                if transcription:
                    # Save transcription to database
                    self.db_manager.set_audio_info(pk, "speech", transcription)
                    logger.info(f"Successfully transcribed reel {pk}: {len(transcription)} chars")
                else:
                    # No speech detected, mark as no audio
                    logger.info(f"No speech detected in reel {pk}, marking as no_audio")
                    self.db_manager.mark_reel_as_no_audio_and_clear_type(pk)
            
            logger.info("Batch processing completed")
            
        except Exception as e:
            logger.error(f"Error processing speech reels: {e}")
            raise
    
    def get_processing_stats(self):
        """Get statistics about speech processing."""
        try:
            # Get database stats
            stats = self.db_manager.get_speech_processing_stats()
            
            # Add audio file count
            if os.path.exists(self.audio_dir):
                audio_files = [f for f in os.listdir(self.audio_dir) if f.endswith('.mp3')]
                audio_file_count = len(audio_files)
            else:
                audio_file_count = 0
            
            stats['audio_file_count'] = audio_file_count
            logger.info(f"Audio files available: {audio_file_count}")
            
            return stats
                
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {}

def main():
    """Main function to run the speech processor for the pipeline."""
    try:
        processor = SpeechProcessor()
        
        logger.info("=== Initial Speech Analysis Statistics ===")
        stats = processor.get_processing_stats()
        
        reels_to_process_count = stats.get('speech_pending', 0)
        if reels_to_process_count == 0:
            logger.info("No reels with speech pending processing. All done!")
            return
        
        logger.info(f"=== Starting Speech Processing for {reels_to_process_count} reels... ===")
        processor.process_speech_reels(batch_size=reels_to_process_count) # Process all in one go
        
        logger.info("=== Final Speech Analysis Statistics ===")
        processor.get_processing_stats()
        
    except Exception as e:
        logger.error(f"An error occurred in the main speech processing loop: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
