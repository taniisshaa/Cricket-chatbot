
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
    Background loop that periodically syncs ONLY FINISHED matches.
    Scheduled/Upcoming matches are NOT stored in database.
    """
    global _scheduler_running
    logger.info("🚀 Starting Smart Background Sync Scheduler...")
    logger.info("📋 Strategy: ONLY FINISHED matches will be stored")
    logger.info("⏱️  Sync Frequency: Every 1 minute for real-time updates")
    
    # Initial Sync (Catch up on last 7 days of FINISHED matches)
    try:
        logger.info("🔄 Performing Initial Sync (Last 7 Days - FINISHED ONLY)...")
        result = asyncio.run(sync_recent_finished_matches(days_back=7))
        logger.info(f"✅ Initial Sync: {result.get('updated', 0)} stored, {result.get('skipped', 0)} skipped")
    except Exception as e:
        logger.error(f"❌ Initial Sync Failed: {e}")

    while True:
        try:
            # --- REAL-TIME SYNC FOR FINISHED MATCHES ---
            # Wait for 1 minute before next check (Real-time feel)
            time.sleep(60)
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"🔄 [{current_time}] Running Real-Time Sync (Last 24 Hours)...")
            
            # Sync last 1 day - Only FINISHED matches will be stored
            result = asyncio.run(sync_recent_finished_matches(days_back=1))
            
            stored = result.get('updated', 0)
            skipped = result.get('skipped', 0)
            
            if stored > 0:
                logger.info(f"✅ Sync Success: {stored} FINISHED matches stored, {skipped} scheduled/in-progress skipped")
            else:
                logger.info(f"ℹ️  No new finished matches. {skipped} scheduled matches skipped.")
            
        except Exception as e:
            logger.error(f"❌ Periodic Sync Failed: {e}")
            time.sleep(30) # Wait a bit before retrying on error

def start_background_sync():
    """
    Starts the background sync thread if not already running.
    Should be called once at application startup.
    
    BEHAVIOR:
    - Syncs every 1 minute
    - ONLY stores FINISHED matches
    - Skips all scheduled/upcoming matches
    - Automatically detects when match finishes and stores complete data
    """
    global _scheduler_running
    if _scheduler_running:
        logger.info("⚠️  Background Sync already running")
        return
    
    _scheduler_running = True
    thread = threading.Thread(target=run_sync_loop, daemon=True)
    thread.start()
    logger.info("✅ Background Sync Thread Started - Monitoring for FINISHED matches")
