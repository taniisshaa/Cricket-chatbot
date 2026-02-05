import asyncio
from datetime import datetime, timedelta
from src.utils.utils_core import get_logger
from src.environment.backend_core import sportmonks_cric, _normalize_sportmonks_to_app_format
logger = get_logger("ipl_svc", "IPL.log")
async def get_todays_matches_full(use_cache=True):
    """
    Fetches the full schedule for TODAY (Active + Scheduled + Fininshed today).
    Includes matches started up to 5 days ago if they are still Active (e.g. Test Matches).
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    includes = "localteam,visitorteam,runs,venue"
    logger.info(f"Fetching Today's Full Schedule: {today_str}")
    res = await sportmonks_cric("/fixtures", {
        "filter[starts_between]": f"{today_str},{today_str}",
        "include": includes,
        "sort": "starting_at"
    }, use_cache=use_cache, ttl=120)
    matches = []
    seen_ids = set()
    if res.get("ok"):
        raw_data = res.get("data", [])
        for m in raw_data:
            matches.append(_normalize_sportmonks_to_app_format(m))
            seen_ids.add(m.get("id"))
    try:
        past_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        res_ongoing = await sportmonks_cric("/fixtures", {
            "filter[starts_between]": f"{past_date},{yesterday}",
            "include": includes,
        }, use_cache=use_cache, ttl=300)
        if res_ongoing.get("ok"):
            raw_ongoing = res_ongoing.get("data", [])
            for m in raw_ongoing:
                status = str(m.get("status", "")).lower()
                active_statuses = ["live", "stumps", "break", "tea", "lunch", "dinner", "innings", "delayed", "interrupted"]
                if any(s in status for s in active_statuses) and "finished" not in status and "completed" not in status:
                    if m.get("id") not in seen_ids:
                        matches.append(_normalize_sportmonks_to_app_format(m))
                        seen_ids.add(m.get("id"))
    except Exception as e:
        logger.error(f"Error fetching ongoing multi-day matches: {e}")
    logger.info(f"Today's Full Schedule (inc. ongoing): Found {len(matches)} matches")
    return {
        "ok": True,
        "date": today_str,
        "count": len(matches),
        "data": matches
    }
async def get_matches_by_date(target_date, team_name=None, use_cache=True):
    """
    Fetches matches for a specific date in the current year.
    Used for 'Yesterday match' or '28 Jan match'.
    """
    logger.info(f"Fetching Matches for Date: {target_date} (Team: {team_name})")
    includes = "localteam,visitorteam,runs,venue,manofmatch,batting.batsman,bowling.bowler"
    res = await sportmonks_cric("/fixtures", {
        "filter[starts_between]": f"{target_date},{target_date}",
        "include": includes
    }, use_cache=use_cache, ttl=600 if target_date != datetime.now().strftime("%Y-%m-%d") else 120)
    raw_matches = res.get("data", []) if res.get("ok") else []
    if not raw_matches:
        logger.info(f"No matches found on {target_date}, expanding search window +/- 1 day...")
        try:
            dt_obj = datetime.strptime(target_date, "%Y-%m-%d")
            start = (dt_obj - timedelta(days=1)).strftime("%Y-%m-%d")
            end = (dt_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            res_expanded = await sportmonks_cric("/fixtures", {
                "filter[starts_between]": f"{start},{end}",
                "include": includes
            })
            if res_expanded.get("ok"):
                raw_matches = res_expanded.get("data", [])
        except Exception as e:
            logger.error(f"Error in expanded date search: {e}")
    matches = []
    t_filter = (team_name or "").lower().strip()
    for m in raw_matches:
        normalized = _normalize_sportmonks_to_app_format(m)
        if t_filter:
            if t_filter in normalized["t1"].lower() or t_filter in normalized["t2"].lower():
                matches.append(normalized)
        else:
            matches.append(normalized)
    logger.info(f"Target Date {target_date}: Final Count {len(matches)} matches (Team Filter: {team_name})")
    return {
        "ok": True,
        "date": target_date,
        "data": matches
    }
async def get_recent_matches(days=3):
    """
    Get finished matches from the last few days (Recent Past).
    """
    today = datetime.now()
    start_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Fetching Recent Matches: {start_date} to {end_date}")
    res = await sportmonks_cric("/fixtures", {
        "filter[starts_between]": f"{start_date},{end_date}",
        "include": "localteam,visitorteam,runs,venue",
        "sort": "-starting_at"
    })
    matches = []
    if res.get("ok"):
        data = res.get("data", [])
        for m in data:
            matches.append(_normalize_sportmonks_to_app_format(m))
    logger.info(f"Recent Matches ({start_date} to {end_date}): Found {len(matches)} matches")
    return {
        "ok": True,
        "data": matches
    }