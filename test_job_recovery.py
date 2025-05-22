"""Test script to verify job recovery functions"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Test job recovery functions"""
    logger.info("Starting test script")
    
    try:
        # Import the functions
        logger.info("Importing functions...")
        from src.core.routes import get_db_instance, check_and_restart_interrupted_jobs
        
        # Test get_db_instance
        logger.info("Testing get_db_instance function...")
        jobs_db = get_db_instance("jobs")
        logger.info(f"Jobs DB instance: {jobs_db}")
        
        # Test job recovery function
        logger.info("Testing check_and_restart_interrupted_jobs function...")
        check_and_restart_interrupted_jobs()
        
        logger.info("Test completed successfully")
        return 0
    
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 