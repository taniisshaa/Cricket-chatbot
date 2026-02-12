import asyncio
import re
from datetime import datetime
import dateparser
from src.utils.utils_core import get_logger
from src.environment.backend_core import getSeries, getSeriesInfo, getCurrentMatches, getTodayMatches, getMatchScorecard, get_live_matches, get_series_matches_by_id, getMatchInfo, getPlayers, get_upcoming_matches
from src.utils.match_utils import _match_series_name
logger = get_logger("search_svc")
def _normalize(s):
    return (s or "").strip().lower()
def _is_team_match(query, target_name):
    if not query or not target_name: return False
    q = _normalize(query)
    t = _normalize(target_name)
    if q in t: return True
    words = re.split(r'[\s\.\-]+', target_name)
    initials = "".join([w[0] for w in words if w]).lower()
    if q == initials: return True
    return False
async def find_player_id(player_name):
    if not player_name: return None
    res = await getPlayers(player_name)
    data = res.get("data", [])
    return data[0]["id"] if data else None
async def resolve_season_for_league(league_id, target_year):
    """Find Season ID for a specific year given a League ID."""
    from src.environment.backend_core import sportmonks_cric
    res = await sportmonks_cric(f"/leagues/{league_id}", {"include": "seasons"})
    if not res.get("data"): return None
    seasons = res["data"].get("seasons", [])
    target_str = str(target_year)
    # logger.info(f"Resolving Season for League {league_id} Year {target_year}. Seasons: {len(seasons)}")
    for s in seasons:
        if str(s.get("name")) == target_str: return s.get("id")
    for s in seasons:
        if target_str in str(s.get("name", "")): return s.get("id")
    return None
async def find_series_smart(series_name, year=None):
    if not series_name: return None
    logger.info(f"Smart Search: {series_name} | Year: {year}")
    
    # Extract year from name if not provided
    if not year:
        year_match = re.search(r'\b(20\d{2})\b', series_name)
        if year_match:
            year = year_match.group(1)
    
    clean_series = _normalize(series_name)
    # Remove year from search string to find the league
    league_search = re.sub(r'\b20\d{2}\b', '', clean_series).strip() or clean_series
    
    year_str = str(year) if year else ""
    
    # 1. Try finding by year directly (returns seasons)
    if year_str:
        y_res = await getSeries(year_str, rows=50)
        if y_res.get("data"):
            candidates = [s for s in y_res["data"] if _match_series_name(league_search, s.get("name"))]
            if candidates: 
                # If there are multiple, prefer the ones with exactly the year or the league name
                return candidates[0]["id"]
    
    # 2. Try finding league first
    res = await getSeries(league_search, rows=20)
    candidates = res.get("data", [])
    if not candidates:
        # Fallback to full name search
        res = await getSeries(clean_series, rows=20)
        candidates = res.get("data", [])
        
    if not candidates: return None
    
    best = candidates[0]
    # If we have a year, always resolve to season
    if year_str:
        season_id = await resolve_season_for_league(best["id"], year_str)
        if season_id: return season_id
        
    return best["id"]
async def find_match_id(team1=None, team2=None, series_id=None, target_date=None, series_name=None, year=None):
    """Logic-driven match finder."""
    if not any([team1, team2, series_id, series_name]): return None
    logger.info(f"Finding Match: {team1} vs {team2} | Date: {target_date}")
    pool = []
    l_res = await getCurrentMatches()
    pool.extend(l_res.get("data", []))
    if not pool:
        u_res = await get_upcoming_matches()
        pool.extend(u_res.get("data", []))
    if series_id:
        s_res = await get_series_matches_by_id(series_id)
        pool.extend(s_res.get("data", []))
    elif series_name:
        s_id = await find_series_smart(series_name, year)
        if s_id:
            s_res = await get_series_matches_by_id(s_id)
            pool.extend(s_res.get("data", []))
    matches = []
    for m in pool:
        name = _normalize(m.get("name", ""))
        t1_ok = _is_team_match(team1, name) if team1 else True
        t2_ok = _is_team_match(team2, name) if team2 else True
        date_ok = True
        if target_date:
            m_date = m.get("date", "")
            if m_date != target_date: date_ok = False
        if t1_ok and t2_ok and date_ok:
            matches.append(m)
    if not matches: return None
    matches.sort(key=lambda x: (x.get("status") == "Live", x.get("matchEnded")), reverse=True)
    return matches[0]["id"]
async def find_match_by_score(team, score_str, year=None, series_name=None, score_details=None):
    """Find a match where a specific score happened."""
    logger.info(f"Score Search: {team} {score_str}")
    s_id = await find_series_smart(series_name, year)
    if not s_id: return None
    res = await get_series_matches_by_id(s_id)
    matches = res.get("data", [])
    for m in matches:
        if not m.get("matchEnded"): continue
        sc = await getMatchScorecard(m["id"])
        if not sc.get("data"): continue
        if _is_team_match(team, m.get("name")):
             return sc.get("data")
    return None