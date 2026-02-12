import asyncio
from src.utils.utils_core import get_logger
from src.environment.backend_core import getSeriesInfo, getMatchScorecard, get_series_matches_by_id, getSeries, _normalize_sportmonks_to_app_format
from src.core.search_service import find_series_smart
logger = get_logger("analytics_svc")
async def get_series_final_info(series_id):
    res = await get_series_matches_by_id(series_id)
    matches = res.get("data", [])
    if not matches: return None
    completed = [m for m in matches if m.get("matchEnded")]
    if completed: return {"data": completed[-1]}
    return None
async def get_player_recent_performance(player_name, series_id=None):
    from src.environment.backend_core import getPlayers, getMatchScorecard
    p_res = await getPlayers(search=player_name)
    if not p_res.get("data"):
         return {"error": f"Player {player_name} not found in API"}
    player_id = p_res["data"][0]["id"]
    formal_name = p_res["data"][0].get("fullname")
    matches_to_scan = []
    if series_id:
        res = await get_series_matches_by_id(series_id)
        if res.get("data"):
            matches_to_scan = sorted(
                [m for m in res["data"] if m.get("matchEnded")],
                key=lambda x: x.get("date", "0000-00-00"),
                reverse=True
            )[:5] 
    recent_stats = []
    for m in matches_to_scan:
        sc = await getMatchScorecard(m["id"])
        if not sc.get("data"): continue
        data = sc["data"]
        match_name = data.get("name")
        date = data.get("date")
        found = False
        perf = {"match": match_name, "date": date, "batting": {}, "bowling": {}}
        for inn in data.get("scorecard", []):
            for b in inn.get("batting", []):
                if b.get("batsman_id") == player_id:
                    perf["batting"] = {
                        "runs": b.get("score") or b.get("runs"),
                        "balls": b.get("ball"),
                        "4s": b.get("four_x"),
                        "6s": b.get("six_x")
                    }
                    found = True
            for b in inn.get("bowling", []):
                if b.get("bowler_id") == player_id:
                     perf["bowling"] = {
                         "wickets": b.get("wickets"),
                         "runs": b.get("runs"),
                         "overs": b.get("overs")
                     }
                     found = True
        if found:
            recent_stats.append(perf)
    if not recent_stats:
        return {
             "player": formal_name,
             "status": "No recent match data found in API (Player may be inactive in selected series)",
             "data": []
        }
    return {
        "player": formal_name,
        "recent_matches": recent_stats
    }
async def handle_tournament_specialist_logic(analysis, user_query, series_name=None, year=None):
    if not series_name: return None
    
    # PRIORITY 1: Check DATABASE for season champion data
    if year:
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            DB_CONFIG = {
                "dbname": "cricket_db",
                "user": "postgres",
                "password": "1234",
                "host": "localhost",
                "port": "5432"
            }
            
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Query season_champions table
            cur.execute("""
                SELECT 
                    wt.name as winner_team,
                    rt.name as runner_up_team,
                    s.name as season_name,
                    s.year
                FROM season_champions sc
                JOIN teams wt ON sc.winner_team_id = wt.id
                LEFT JOIN teams rt ON sc.runner_up_team_id = rt.id
                JOIN seasons s ON sc.season_id = s.id
                JOIN leagues l ON s.league_id = l.id
                WHERE (l.name ILIKE %s OR l.code ILIKE %s)
                  AND s.year = %s
                LIMIT 1
            """, (f'%{series_name}%', f'%{series_name}%', str(year)))
            
            champion = cur.fetchone()
            cur.close()
            conn.close()
            
            if champion:
                logger.info(f"✅ Found champion in DATABASE: {champion['winner_team']} (IPL {year})")
                return {
                    "winner_info": {
                        "winner": champion['winner_team'],
                        "runner_up": champion['runner_up_team'],
                        "season": f"{series_name} {year}",
                        "source": "database"
                    }
                }
        except Exception as e:
            logger.warning(f"Database champion query failed: {e}")
    
    # PRIORITY 2: Fall back to API
    sid = await find_series_smart(series_name, year)
    if not sid: return None
    intent = analysis.get("intent", "").lower()
    uq = user_query.lower()
    if any(k in intent or k in uq for k in ["winner", "standings", "score", "total", "record", "best", "high", "low"]):
        s = await get_series_analytics(sid)
        if not s: return None
        return {
            "winner_info": s.get("winner_info"),
            "batting_leaders": s.get("batting_leaders"),
            "bowling_leaders": s.get("bowling_leaders"),
            "highest_team_score": s.get("highest_team_score"),
            "highest_match_aggregate": s.get("highest_match_aggregate")
        }
    return None


async def extract_series_winner(series_id, matches=None):
    if not matches:
        data = await get_series_matches_by_id(series_id)
        # We MUST normalize these so they have 'winner', 'matchEnded', and 'name'
        raw_matches = data.get("data", [])
        matches = [_normalize_sportmonks_to_app_format(m) for m in raw_matches]
    
    if not matches: return None
    
    # Filter completed matches
    completed_matches = [m for m in matches if m.get("matchEnded")]
    if not completed_matches: return None
    
    # The last match in a multi-stage tournament is usually the Final
    final_match = completed_matches[-1]
    
    # Try to find winner name
    winner_name = "Unknown"
    winner_id = final_match.get("winner_team_id")
    
    if winner_id:
        if winner_id == final_match.get("t1_id"):
             winner_name = final_match.get("t1")
        elif winner_id == final_match.get("t2_id"):
             winner_name = final_match.get("t2")
    
    # Extract Score Summary
    scores_list = []
    scoreboards = final_match.get("scoreboards", [])
    
    # Filter for total scoreboards
    totals = [sb for sb in scoreboards if sb.get("type") == "total"]
    
    for sb in totals:
        tid = sb.get("team_id")
        runs = sb.get("total")
        wickets = sb.get("wickets")
        overs = sb.get("overs")
        
        tname = "Unknown"
        if tid == final_match.get("t1_id"): tname = final_match.get("t1")
        elif tid == final_match.get("t2_id"): tname = final_match.get("t2")
        
        scores_list.append(f"{tname} {runs}/{wickets} ({overs} ov)")
        
    final_score_str = ", ".join(scores_list)

    return {
        "winner": winner_name,
        "final_match": final_match.get("name"),
        "final_scores": final_score_str,
        "total_matches": len(matches),
        "winner_id": winner_id
    }
async def get_series_analytics(series_id, deep_scan=True, segment=None, limit=None):
    """
    Detailed Series Analytics.
    """
    res = await get_series_matches_by_id(series_id)
    matches = res.get("data", [])
    if not matches: return None
    analytics = {
        "series_info": res.get("info", {}),
        "total_matches": len(matches),
        "matches_analyzed": 0,
        "highest_team_score": {"runs": 0, "team": "", "match": "", "date": ""},
        "highest_match_aggregate": {"runs": 0, "match": "", "date": "", "scores": ""},
        "most_runs": {},
        "most_wickets": {}
    }
    
    completed = [m for m in matches if m.get("matchEnded")]
    if limit: completed = completed[:limit]
    analytics["matches_analyzed"] = len(completed)
    tasks = []
    chunk_size = 10
    scorecards = []
    for i in range(0, len(completed), chunk_size):
        chunk = completed[i:i+chunk_size]
        t = [getMatchScorecard(m["id"]) for m in chunk]
        r = await asyncio.gather(*t, return_exceptions=True)
        scorecards.extend(r)
    for sc in scorecards:
        if isinstance(sc, Exception) or not sc.get("data"): continue
        data = sc["data"]
        match_total = 0
        match_scores = []
        for inn in data.get("scorecard", []):
            try:
                r = int(inn.get("totals", {}).get("R") or 0)
                w = int(inn.get("totals", {}).get("W") or 0)
                team = inn.get("inning", "").split(" Inning")[0]
                match_total += r
                match_scores.append(f"{team}: {r}/{w}")
                if r > analytics["highest_team_score"]["runs"]:
                    analytics["highest_team_score"] = {
                        "runs": r,
                        "team": team,
                        "match": data.get("name"),
                        "date": data.get("date")
                    }
                for b in inn.get("batting", []):
                    p_name = b.get("batsman", {}).get("name")
                    runs = int(b.get("r") or 0)
                    if p_name:
                        analytics["most_runs"][p_name] = analytics["most_runs"].get(p_name, 0) + runs
                for b in inn.get("bowling", []):
                    p_name = b.get("bowler", {}).get("name")
                    w = int(b.get("w") or 0)
                    if p_name:
                        analytics["most_wickets"][p_name] = analytics["most_wickets"].get(p_name, 0) + w
            except: pass
        if match_total > analytics["highest_match_aggregate"]["runs"]:
            analytics["highest_match_aggregate"] = {
                "runs": match_total,
                "match": data.get("name"),
                "date": data.get("date"),
                "scores": " & ".join(match_scores)
            }
    analytics["batting_leaders"] = sorted(analytics["most_runs"].items(), key=lambda x: x[1], reverse=True)[:10]
    analytics["bowling_leaders"] = sorted(analytics["most_wickets"].items(), key=lambda x: x[1], reverse=True)[:10]
    del analytics["most_runs"]
    del analytics["most_wickets"]
    analytics["winner_info"] = await extract_series_winner(series_id, matches)
    return analytics
async def get_series_top_performers(series_id):
    """Wrapper using get_series_analytics."""
    return await get_series_analytics(series_id)
async def get_head_to_head_statistics(team_a, team_b, limit=10):
    logger.info(f"H2H: {team_a} vs {team_b}")
    queries = [f"{team_a} vs {team_b}", f"{team_b} vs {team_a}"]
    tasks = [getSeries(q, rows=10) for q in queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    matches_found = []
    for r in results:
        if isinstance(r, dict) and r.get("data"):
            for s in r["data"]:
                s_info = await getSeriesInfo(s["id"])
                if s_info.get("data"):
                     m_list = s_info["data"].get("matchList", [])
                     for m in m_list:
                         if (team_a.lower() in m.get("name", "").lower() and team_b.lower() in m.get("name", "").lower()) and m.get("matchEnded"):
                             matches_found.append(m)
    unique = {m["id"]: m for m in matches_found}.values()
    final = sorted(unique, key=lambda x: x.get("date", ""), reverse=True)[:limit]
    return {
        "team_a": team_a,
        "team_b": team_b,
        "matches": final,
        "count": len(final)
    }