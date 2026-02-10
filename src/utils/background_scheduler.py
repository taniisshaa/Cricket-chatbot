
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
            # --- HIGH FREQUENCY SYNC FOR TODAY'S MATCHES ---
            # Wait for 2 minutes before next check (Reduced from 15 mins for 'Automatic' feel)
            time.sleep(120) 
            
            logger.info("Running High-Frequency Sync (Last 24 Hours)...")
            # Sync last 1 day. This ensures finished matches from today land in DB ASAP.
            asyncio.run(sync_recent_finished_matches(days_back=1))
            
            # Every 6 cycles (approx 12 mins), do a deeper sync for last 3 days
            # to catch any delayed status updates.
            # (Logic can be added here if needed, but 1 day is sufficient for 'Today')
            
        except Exception as e:
            logger.error(f"Periodic Sync Failed: {e}")
            time.sleep(30) # Wait a bit before retrying on error

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
