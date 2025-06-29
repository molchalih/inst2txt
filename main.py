import asyncio
import logging
from hiker import main as hiker_main
from download_reels import main as download_reels_main
from utility.extract_audio import extract_audio_for_all_downloaded_reels
from music import analyze_selected_reels
from speech import main as speech_main
from translate import main as translate_main
from concise import main as concise_main
from video import main as video_main
from postvideo import clean_video_descriptions as postvideo_main
from vector import generate_embeddings_for_reels as vector_main
from clustering import main as clustering_main
from social_connections import main as social_connections_main

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main execution block"""
    logger.info("--- Starting Hiker Processing ---")
    hiker_main()
    logger.info("--- Hiker Processing Finished ---")

    logger.info("--- Starting Reels Download ---")
    asyncio.run(download_reels_main())
    logger.info("--- Reels Download Finished ---")

    logger.info("--- Starting Audio Extraction ---")
    extract_audio_for_all_downloaded_reels()
    logger.info("--- Audio Extraction Finished ---")

    logger.info("--- Starting Music Analysis ---")
    analyze_selected_reels()
    logger.info("--- Music Analysis Finished ---")

    logger.info("--- Starting Speech Analysis ---")
    speech_main()
    logger.info("--- Speech Analysis Finished ---")

    logger.info("--- Starting Caption Translation ---")
    translate_main()
    logger.info("--- Caption Translation Finished ---")

    logger.info("--- Starting Text Condensing ---")
    concise_main()
    logger.info("--- Text Condensing Finished ---")

    logger.info("--- Starting Video Analysis ---")
    video_main()
    logger.info("--- Video Analysis Finished ---")

    logger.info("--- Starting Post-Video Description Cleanup ---")
    postvideo_main()
    logger.info("--- Post-Video Description Cleanup Finished ---")

    logger.info("--- Starting Vector Embedding Generation ---")
    vector_main()
    logger.info("--- Vector Embedding Generation Finished ---")

    logger.info("--- Starting Clustering Analysis ---")
    clustering_main()
    logger.info("--- Clustering Analysis Finished ---")

    logger.info("--- Starting Social Connections Analysis ---")
    social_connections_main()
    logger.info("--- Social Connections Analysis Finished ---")


if __name__ == "__main__":
    main()