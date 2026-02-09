
import threading
import time
import asyncio
from datetime import datetime
from src.utils.utils_core import get_logger
from src.environment.history_service import sync_recent_finished_matches

logger = get_logger("scheduler", "SYNC_LOGS.log")

_scheduler_running = False

def run_sync_loop():
    """
    Background loop that periodically syncs finished matches.
    """
    global _scheduler_running
    logger.info("Starting Background Sync Scheduler...")
    
    # Initial Sync (Catch up on last 7 days)
    try:
        logger.info("Performing Initial Sync (Last 7 Days)...")
        asyncio.run(sync_recent_finished_matches(days_back=7))
    except Exception as e:
        logger.error(f"Initial Sync Failed: {e}")

    while True:
        try:
            # Wait for 15 minutes before next check
            time.sleep(900) 
            
            logger.info("Running Periodic Sync (Last 24 Hours)...")
            # Sync only last 1 day to be efficient
            asyncio.run(sync_recent_finished_matches(days_back=1))
            
        except Exception as e:
            logger.error(f"Periodic Sync Failed: {e}")
            time.sleep(60) # Wait a bit before retrying on error

def start_background_sync():
    """
    Starts the background sync thread if not already running.
    Should be called once at application startup.
    """
    global _scheduler_running
    if _scheduler_running:
        return
    
    _scheduler_running = True
    thread = threading.Thread(target=run_sync_loop, daemon=True)
    thread.start()
    logger.info("Background Sync Thread Started.")
