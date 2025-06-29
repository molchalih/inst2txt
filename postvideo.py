import re
from db_manager import InstagramDataManager

def clean_video_descriptions():
    """Clean up model_description_text by removing 'the video' mentions and save to new field"""
    print("=== Starting Description Cleanup ===")
    
    # Initialize database manager
    db_manager = InstagramDataManager()
    
    # Ensure processed column exists
    db_manager.ensure_processed_column()
    
    # Get reels that need processing
    reels = db_manager.get_reels_for_processing()
    
    if not reels:
        print("No reels found needing processing")
        return
    
    print(f"Found {len(reels)} reels with descriptions to process")
    
    processed_count = 0
    
    for pk, description in reels:
        if not description:
            continue
        
        original = description
        cleaned = description
        
        # Remove "of this video" from anywhere in the text (case insensitive)
        cleaned = re.sub(r'\bof this video\b', '', cleaned, flags=re.IGNORECASE)
        
        # Remove "the video" from anywhere in the text (case insensitive)
        cleaned = re.sub(r'\bthe video\b', '', cleaned, flags=re.IGNORECASE)
        
        # Also remove "this video" from anywhere in the text
        cleaned = re.sub(r'\bthis video\b', '', cleaned, flags=re.IGNORECASE)
        
        # Remove "video" from anywhere in the text
        cleaned = re.sub(r'\bvideo\b', '', cleaned, flags=re.IGNORECASE)
        
        # Remove "the video's" from anywhere in the text
        cleaned = re.sub(r'\bthe video\'s\b', '', cleaned, flags=re.IGNORECASE)
        
        # Remove "this video's" from anywhere in the text
        cleaned = re.sub(r'\bthis video\'s\b', '', cleaned, flags=re.IGNORECASE)
        
        # Clean up extra whitespace that might be left
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Save the cleaned description to the new field
        db_manager.save_processed_description(pk, cleaned)
        processed_count += 1
        print(f"Processed reel {pk}: '{original[:50]}...' -> '{cleaned[:50]}...'")
    
    print("\n=== Processing Complete ===")
    print(f"Processed {processed_count} descriptions")
    print(f"Total processed: {len(reels)}")

if __name__ == "__main__":
    clean_video_descriptions()
