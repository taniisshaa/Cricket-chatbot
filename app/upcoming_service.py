
import asyncio
from datetime import datetime, timedelta
from app.utils_core import get_logger
from app.backend_core import sportmonks_cric, _normalize_sportmonks_to_app_format

logger = get_logger("upcoming_svc", "UPCOMING_SCHEDULE.log")

async def get_upcoming_matches(days=14):
    """
    Fetches upcoming fixtures for the next X days.
    """
    today = datetime.now()
    start_date = today.strftime("%Y-%m-%d")
    end_date = (today + timedelta(days=days)).strftime("%Y-%m-%d")

    logger.info(f"Fetching Upcoming Matches: {start_date} to {end_date}")

    includes = "localteam,visitorteam,venue"

    res = await sportmonks_cric("/fixtures", {
        "filter[starts_between]": f"{start_date},{end_date}",
        "include": includes,
        "sort": "starting_at"
    }, use_cache=True, ttl=3600)

    matches = []
    if res.get("ok"):
        raw = res.get("data", [])
        for m in raw:
            norm = _normalize_sportmonks_to_app_format(m)

            if norm.get("status") not in ["Finished", "Completed", "Abandoned"]:
                 matches.append(norm)

    logger.info(f"Found {len(matches)} upcoming matches from {start_date} to {end_date}")
    return {
        "ok": True,
        "range": f"{start_date} to {end_date}",
        "data": matches
    }

async def get_schedule_by_tour(tour_name, limit=5):
    """
    Search for upcoming matches of a specific series/league.
    """


    pass
