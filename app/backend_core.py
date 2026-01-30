import os
import json
import httpx
import asyncio
import hashlib
from datetime import datetime
from app.utils_core import get_logger

logger = get_logger("backend_core", "general_app.log")

_CACHE = {}
SPORTMONKS_BASE = "https://cricket.sportmonks.com/api/v2.0"
_CLIENT = None
_LOOP_REF = None

def _get_cache_key(endpoint, params):
    param_str = json.dumps(params or {}, sort_keys=True, default=str)
    return hashlib.md5(f"{endpoint}:{param_str}".encode()).hexdigest()

def _get_from_cache(cache_key, ttl=300):
    if cache_key in _CACHE:
        cached_data, timestamp = _CACHE[cache_key]
        if (datetime.now() - timestamp).total_seconds() < ttl:
            return cached_data
        del _CACHE[cache_key]
    return None

def _save_to_cache(cache_key, data):
    _CACHE[cache_key] = (data, datetime.now())

async def _get_client():
    global _CLIENT, _LOOP_REF
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        return httpx.AsyncClient(timeout=10.0)
    if _CLIENT is None or _CLIENT.is_closed or _LOOP_REF != current_loop:
        _CLIENT = httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=20))
        _LOOP_REF = current_loop
    return _CLIENT

async def sportmonks_cric(endpoint, params=None, use_cache=True, ttl=60, **kwargs):
    params = params or {}
    sm_key = os.getenv("SPORTMONKS_API_KEY")
    if not sm_key: return {"ok": False, "error": "API Key Missing"}
    
    if kwargs.get("force_api"):
        use_cache = False
    
    cache_key = _get_cache_key(f"sm:{endpoint}", params)
    if use_cache:
        cached = _get_from_cache(cache_key, ttl)
        if cached: return cached
        
    params["api_token"] = sm_key
    client = await _get_client()
    
    # Use a longer timeout for complex includes
    timeout = httpx.Timeout(45.0, connect=10.0)
    
    try:
        r = await client.get(f"{SPORTMONKS_BASE}{endpoint}", params=params, timeout=timeout)
        if r.status_code == 200:
            result = {"ok": True, "status": 200, "data": r.json().get("data", [])}
            if use_cache: _save_to_cache(cache_key, result)
            return result
        
        # Fallback: If it's a timeout or 500-range error, try without some includes if possible
        if r.status_code >= 500 and "include" in params:
             logger.warning(f"Complex API call failed ({r.status_code}), retrying with simplified query...")
             simple_params = params.copy()
             del simple_params["include"]
             r2 = await client.get(f"{SPORTMONKS_BASE}{endpoint}", params=simple_params, timeout=timeout)
             if r2.status_code == 200:
                  return {"ok": True, "status": 200, "data": r2.json().get("data", []), "warning": "Simplified data"}

        return {"ok": False, "status": r.status_code, "error": r.text[:200]}
    except (httpx.TimeoutException, httpx.NetworkError) as e:
        logger.error(f"API Connection Error on {endpoint}: {e}")
        # Try one last time with NO includes if it was a timeout
        if "include" in params:
             try:
                 simple_params = params.copy()
                 del simple_params["include"]
                 r2 = await client.get(f"{SPORTMONKS_BASE}{endpoint}", params=simple_params, timeout=timeout)
                 if r2.status_code == 200:
                      return {"ok": True, "status": 200, "data": r2.json().get("data", []), "warning": "Simplified data after timeout"}
             except: pass
        return {"ok": False, "status": 0, "error": str(e)}
    except Exception as e:
        return {"ok": False, "status": 0, "error": str(e)}

# --- Wrappers ---
async def getSeries(search=None, **kwargs):
    params = {"search": search} if search else {}
    return await sportmonks_cric("/leagues", params, **kwargs)

async def getSeriesInfo(series_id, includes=None, **kwargs):
    info = await sportmonks_cric(f"/seasons/{series_id}", {}, **kwargs)
    if not info.get("data"): info = await sportmonks_cric(f"/leagues/{series_id}", {}, **kwargs)
    
    params = {"include": f"localteam,visitorteam,venue,{includes}" if includes else "localteam,visitorteam,venue"}
    sid = info.get("data", {}).get("season_id") or info.get("data", {}).get("current_season_id")
    if sid: params["filter[season_id]"] = sid
    else: params["filter[season_id]"] = series_id # Fallback
    
    fixtures = await sportmonks_cric("/fixtures", params, **kwargs)
    return {
        "ok": True, 
        "data": {"info": info.get("data"), "matchList": [m for m in fixtures.get("data", [])]}
    }

async def getMatchScorecard(match_id, **kwargs):
    return await sportmonks_cric(f"/fixtures/{match_id}", {"include": "localteam,visitorteam,runs,batting.batsman,bowling.bowler,venue,balls,scoreboards"}, **kwargs)

async def getMatchInfo(match_id, **kwargs):
    return await sportmonks_cric(f"/fixtures/{match_id}", {"include": "localteam,visitorteam,venue"}, **kwargs)

async def getMatchSquad(match_id, **kwargs):
    return await sportmonks_cric(f"/fixtures/{match_id}", {"include": "lineup"}, **kwargs)

async def getMatchCommentary(match_id, **kwargs):
    return await sportmonks_cric(f"/fixtures/{match_id}", {"include": "balls"}, **kwargs)

async def getMatchPoints(match_id, **kwargs):
    return {"ok": False, "error": "Not implemented"}

async def getPlayers(search=None, **kwargs):
    params = {"filter[lastname]": search} if search else {}
    return await sportmonks_cric("/players", params, **kwargs)

async def getPlayersInfo(player_id, **kwargs):
    return await sportmonks_cric(f"/players/{player_id}", {"include": "country,career"}, **kwargs)

async def getStandings(series_id, **kwargs):
    return await sportmonks_cric(f"/standings/season/{series_id}", {}, **kwargs)

async def get_upcoming_matches(**kwargs):
    # Redirect to upcoming_service via import or simple wrapper
    from app.upcoming_service import get_upcoming_matches
    return await get_upcoming_matches()

async def getCurrentMatches(**kwargs):
    # Combined logic simplified
    from app.live_match_service import fetch_realtime_matches
    from app.current_season_service import get_todays_matches_full, get_matches_by_date
    
    date_query = kwargs.get("date")
    team_query = kwargs.get("team") or kwargs.get("team_a")
    if date_query:
        logger.info(f"Fetching matches for specific date: {date_query} (Team: {team_query})")
        return await get_matches_by_date(date_query, team_name=team_query)

    live = await fetch_realtime_matches()
    today = await get_todays_matches_full()
    
    combined = live + today.get("data", [])
    # Unique by ID
    seen = set()
    unique = []
    for m in combined:
        if m["id"] not in seen:
            unique.append(m)
            seen.add(m["id"])
    return {"ok": True, "data": unique}

async def getTodayMatches(**kwargs):
    from app.current_season_service import get_todays_matches_full
    return await get_todays_matches_full()

async def get_live_matches(**kwargs): return await getCurrentMatches(**kwargs)
async def get_series_matches_by_id(series_id, **kwargs):
    res = await getSeriesInfo(series_id, **kwargs)
    return {"data": res.get("data", {}).get("matchList", [])}

# --- Normalization ---
def _normalize_sportmonks_to_app_format(sm_match):
    # Simplified normalization to keep lines down
    m_id = sm_match.get("id")
    local = sm_match.get("localteam", {}).get("name", "Local")
    visitor = sm_match.get("visitorteam", {}).get("name", "Visitor")
    status = sm_match.get("status", "")
    return {
        "id": m_id,
        "name": f"{local} vs {visitor}",
        "status": status,
        "note": sm_match.get("note", ""),
        "date": sm_match.get("starting_at", "").split("T")[0],
        "venue": sm_match.get("venue", {}).get("name"),
        "t1": local, "t2": visitor,
        "matchEnded": status in ["Finished", "Completed", "Abandoned"],
        "scoreboards": sm_match.get("scoreboards", []),
        "scorecard": [] # Populated fully if needed
    }

# --- New Helper ---
async def fetch_last_finished_match(team_name=None, opponent_name=None, match_type=None):
    """
    Fetches the most recent finished match, optionally filtering by team name and opponent.
    """
    logger.info(f"Fetching last finished match. Team: {team_name} | Opponent: {opponent_name}")
    
    # We fetch a batch of recent fixtures to find the last finished one
    params = {
        "sort": "-starting_at",
        "include": "localteam,visitorteam,runs,venue",
    }
    
    res = await sportmonks_cric("/fixtures", params)
    if not res.get("ok"):
        return None
        
    matches = res.get("data", [])
    
    t_filter = (team_name or "").lower().strip()
    o_filter = (opponent_name or "").lower().strip()
    
    for m in matches:
        # Check if actually finished
        status = str(m.get("status", "")).lower()
        if status not in ["finished", "completed"] and "won" not in status:
             continue
             
        t1 = m.get("localteam", {}).get("name", "").lower()
        t2 = m.get("visitorteam", {}).get("name", "").lower()

        # Complex Match Logic
        match_hit = False
        if not t_filter:
            match_hit = True
        elif t_filter in t1 or t_filter in t2:
            if not o_filter:
                match_hit = True
            elif o_filter in t1 or o_filter in t2:
                match_hit = True
        
        if match_hit:
            return _normalize_sportmonks_to_app_format(m)
        
    return None

# --- API Dispatcher ---
async def cricket_api(intent, **kwargs):
    logger.info(f"API Dispatch: {intent}")
    try:
        # Import Services inside to avoid circular deps
        from app.live_match_service import get_live_match_details, fetch_match_context_bundle, fetch_realtime_matches
        from app.search_service import find_match_id, find_series_smart
        from app.analytics_service import get_series_analytics, get_head_to_head_statistics
        from app.upcoming_service import get_upcoming_matches as svc_upcoming
        
        if intent == "live_match":
            return await getCurrentMatches()
        elif intent in ["upcoming_match", "upcoming_matches"]:
            return await svc_upcoming()
        elif intent in ["fixture_details", "match_info"]:
            return await getMatchInfo(kwargs.get("id"))
        elif intent in ["match_context", "fantasy_picks", "deep_analysis"]:
            fid = kwargs.get("id") or kwargs.get("match_id")
            if not fid and kwargs.get("team_a"):
                 fid = await find_match_id(kwargs.get("team_a"), kwargs.get("team_b"), series_name=kwargs.get("series"))
            return await fetch_match_context_bundle(fid)
        elif intent == "head_to_head":
            return await get_head_to_head_statistics(kwargs.get("team_a"), kwargs.get("team_b"))
        elif intent == "series_analytics":
            sname = kwargs.get("series")
            sid = await find_series_smart(sname, kwargs.get("year"))
            if sid: return {"data": await get_series_analytics(sid)}
            return {"error": "Series not found"}
        return {"error": "Unknown Intent"}
    except Exception as e:
        logger.error(f"API Error: {e}")
        return {"error": str(e)}

# Aliases for compatibility
get_series_info = getSeriesInfo
get_match_scorecard = getMatchScorecard
get_series_standings = getStandings
get_all_series = getSeries
get_todays_matches = getTodayMatches
