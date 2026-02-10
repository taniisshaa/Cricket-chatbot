from datetime import datetime, timedelta
from src.utils.utils_core import get_logger
from src.environment.backend_core import sportmonks_cric, _normalize_sportmonks_to_app_format
logger = get_logger("upcoming_svc", "UPCOMING_SCHEDULE.log")
async def get_upcoming_matches(days=14, check_date=None):
    """
    Fetches upcoming fixtures for the next X days or a specific date.
    """
    today_obj = datetime.now()
    today_str = today_obj.strftime("%Y-%m-%d")
    
    if check_date:
        # If the user specifically asked for 'Upcoming matches' and the date is TODAY,
        # we should show the full upcoming range starting from today.
        if check_date == today_str:
            start_date = today_str
            end_date = (today_obj + timedelta(days=days)).strftime("%Y-%m-%d")
        else:
            start_date = check_date
            end_date = check_date
    else:
        start_date = today_str
        end_date = (today_obj + timedelta(days=days)).strftime("%Y-%m-%d")
        
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
            # ONLY include matches that haven't started yet (Upcoming) 
            if norm.get("status") == "Upcoming":
                 matches.append(norm)
            elif check_date and check_date != today_str and check_date == norm.get("date"):
                 # Single day specific request - show regardless of status for that day
                 matches.append(norm)
                 
    if not matches and check_date and check_date != today_str:
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
                    # Even in expansion, we only want UPCOMING matches if we are in this service
                    if norm.get("status") == "Upcoming":
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