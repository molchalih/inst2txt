import re
from db_manager import InstagramDataManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_video_descriptions():
    """
    Clean up model_description_text by removing model-generated prefixes and mentions
    of "video", then saves the result to model_description_processed.
    """
    logger.info("=== Starting Description Cleanup ===")
    
    db_manager = InstagramDataManager()
    reels = db_manager.get_reels_for_processing()
    
    if not reels:
        logger.info("No reels found needing processing.")
        return
    
    logger.info(f"Found {len(reels)} reels with descriptions to process.")
    
    # A list of regex patterns for common model-generated prefixes.
    # They are case-insensitive and anchored to the start of the string.
    prefix_patterns = [
        re.compile(r"^\s*the aesthetic of this video is characterized by\s*", re.IGNORECASE),
        re.compile(r"^\s*the aesthetic of the video is characterized by\s*", re.IGNORECASE),
        re.compile(r"^\s*the aesthetic of this video is\s*", re.IGNORECASE),
        re.compile(r"^\s*this video'?s aesthetic is\s*", re.IGNORECASE),
        re.compile(r"^\s*the video'?s aesthetic is\s*", re.IGNORECASE),
        re.compile(r"^\s*this video is about\s*", re.IGNORECASE),
        re.compile(r"^\s*the video is about\s*", re.IGNORECASE),
        re.compile(r"^\s*this video showcases\s*", re.IGNORECASE),
        re.compile(r"^\s*the video showcases\s*", re.IGNORECASE),
        re.compile(r"^\s*this video features\s*", re.IGNORECASE),
        re.compile(r"^\s*the video features\s*", re.IGNORECASE),
        re.compile(r"^\s*this video portrays\s*", re.IGNORECASE),
        re.compile(r"^\s*the video portrays\s*", re.IGNORECASE),
    ]

    # A list of general words/phrases to remove globally from the entire text.
    general_patterns = [
        re.compile(r"\b(of|in|for|from)\s+(this|the)\s+video\b", re.IGNORECASE),
        re.compile(r"\b(this|the)\s+video'?s\b", re.IGNORECASE),
        re.compile(r"\b(this|the)\s+video\b", re.IGNORECASE),
        re.compile(r"\bvideo\b", re.IGNORECASE),
    ]
    
    processed_count = 0
    for pk, description in reels:
        if not description:
            continue
        
        original = description
        cleaned = description
        
        # Step 1: Remove one of the known prefixes from the start of the string.
        for pattern in prefix_patterns:
            # As soon as one prefix matches and is removed, we stop.
            new_cleaned, num_subs = pattern.subn('', cleaned, count=1)
            if num_subs > 0:
                cleaned = new_cleaned
                break
        
        # Step 2: Remove general "video" mentions from anywhere in the text.
        for pattern in general_patterns:
            cleaned = pattern.sub('', cleaned)
            
        # Step 3: Clean up whitespace and fix capitalization.
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        if cleaned:
            cleaned = cleaned[0].upper() + cleaned[1:]
        
        # Step 4: Save the result.
        db_manager.save_processed_description(pk, cleaned)
        processed_count += 1
        
        if original != cleaned:
            logger.info(f"Processed reel {pk}: '{original[:80]}...' -> '{cleaned[:80]}...'")
        else:
            # This can happen if no patterns matched. We still save the original
            # content to the 'processed' field to mark it as done.
            logger.info(f"Processed reel {pk} (no changes made): '{original[:80]}...'")

    logger.info("\n=== Processing Complete ===")
    logger.info(f"Processed {processed_count} descriptions.")

if __name__ == "__main__":
    clean_video_descriptions()
