import sqlite3
import pandas as pd
import os
from typing import List, Tuple
import logging
from db_manager import InstagramDataManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main application entry point."""
    logger.info("🚀 Starting Instagram Data Manager...")
    
    try:
        # Initialize the data manager
        data_manager = InstagramDataManager()
        
        # Ensure CSV and database are in sync
        sync_success = data_manager.ensure_sync()
        
        if sync_success:
            logger.info("🎉 Application started successfully with synchronized data")
            print("Instagram Data Manager is ready!")
        else:
            logger.error("❌ Failed to start application due to sync issues")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)