from datetime import datetime, timedelta
from src.utils.utils_core import get_logger
from src.environment.backend_core import sportmonks_cric, _normalize_sportmonks_to_app_format
logger = get_logger("upcoming_svc", "UPCOMING_SCHEDULE.log")
async def get_upcoming_matches(days=14, check_date=None):
    """
    Fetches upcoming fixtures for the next X days or a specific date.
    """
    if check_date:
        start_date = check_date
        end_date = check_date
    else:
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
            if check_date or norm.get("status") not in ["Finished", "Completed", "Abandoned"]:
                 matches.append(norm)
    if not matches and check_date:
        logger.info(f"No matches found on {check_date}, expanding search +/- 2 days...")
        try:
            dt_obj = datetime.strptime(check_date, "%Y-%m-%d")
            start_exp = (dt_obj - timedelta(days=2)).strftime("%Y-%m-%d")
            end_exp = (dt_obj + timedelta(days=2)).strftime("%Y-%m-%d")
            res_exp = await sportmonks_cric("/fixtures", {
                "filter[starts_between]": f"{start_exp},{end_exp}",
                "include": includes,
                "sort": "starting_at"
            }, use_cache=True, ttl=300)
            if res_exp.get("ok"):
                raw_exp = res_exp.get("data", [])
                for m in raw_exp:
                    norm = _normalize_sportmonks_to_app_format(m)
                    norm["note"] = f"(Originally searched {check_date})"
                    matches.append(norm)
        except Exception as e:
            logger.error(f"Fallback search failed: {e}")
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