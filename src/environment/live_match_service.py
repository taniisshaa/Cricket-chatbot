import os
import asyncio
import httpx
from datetime import datetime
from src.utils.utils_core import get_logger
from src.environment.backend_core import (
    getMatchScorecard,
    getMatchCommentary,
    getMatchInfo,
    getMatchSquad,
    getMatchPoints
)
from src.core.search_service import find_match_id
from src.core.db_archiver import archive_match
logger = get_logger("live_svc", "LIVE_FEED.log")
SPORTMONKS_BASE = "https://cricket.sportmonks.com/api/v2.0"
_CLIENT = None
_LOOP_REF = None
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
async def sportmonks_live_request(endpoint, params=None):
    """
    Dedicated request function for live data with NO caching to ensure freshness.
    """
    params = params or {}
    sm_key = os.getenv("SPORTMONKS_API_KEY")
    if not sm_key:
        return {"ok": False, "error": "API Key Missing"}
    params["api_token"] = sm_key
    client = await _get_client()
    try:
        r = await client.get(f"{SPORTMONKS_BASE}{endpoint}", params=params)
        if r.status_code == 200:
            result = r.json()
            return {"ok": True, "status": 200, "data": result.get("data", [])}
        else:
             return {"ok": False, "status": r.status_code, "error": r.text[:200]}
    except Exception as e:
        return {"ok": False, "status": 0, "error": str(e)}
def _normalize_status_live(status):
    s = str(status or "").lower()
    if "live" in s: return "LIVE"
    if "inning" in s and "break" in s: return "Innings Break"
    if "inning" in s: return "LIVE"
    if "tea" in s: return "Tea Break"
    if "lunch" in s: return "Lunch"
    if "dinner" in s: return "Dinner"
    if "drinks" in s: return "Drinks"
    if "stump" in s: return "Stumps"
    if "delay" in s or "rain" in s or "interrupted" in s: return "Delayed"
    if "finished" in s or "completed" in s or "won by" in s or "ended" in s: return "Finished"
    return "Scheduled"
def _get_player_name(lineup_map, pid):
    """Helper to get player name from lineup map."""
    p = lineup_map.get(pid)
    if not p: return f"Player {pid}"
    return p.get("fullname") or p.get("lastname")
def _normalize_live_match_data(sm_match):
    """
    Cleaner normalization specifically for Live Matches.
    """
    try:
        m_id = sm_match.get("id")
        local_obj = sm_match.get("localteam") or {}
        visitor_obj = sm_match.get("visitorteam") or {}
        local = local_obj.get("name", "Team A")
        visitor = visitor_obj.get("name", "Team B")
        status_raw = sm_match.get("status", "")
        formatted_status = _normalize_status_live(status_raw)
        note = sm_match.get("note", "")
        if formatted_status == "Scheduled" and note:
             note_status = _normalize_status_live(note)
             if note_status in ["LIVE", "Innings Break", "Lunch", "Tea Break", "Dinner", "Drinks", "Delayed", "Stumps"]:
                 formatted_status = note_status
                 status_raw = note
        runs_data = sm_match.get("runs") or []
        scores_txt = []
        current_batting_team = None
        current_score_obj = None
        local_id = sm_match.get("localteam_id")
        for r in runs_data:
            t_id = r.get("team_id")
            is_local = str(t_id) == str(local_id)
            team_name = local if is_local else visitor
            score_str = f"{team_name}: {r.get('score')}/{r.get('wickets')} ({r.get('overs')})"
            scores_txt.append(score_str)
            current_score_obj = r
            current_batting_team = team_name
        lineup = sm_match.get("lineup") or []
        player_map = {p.get("id"): p for p in lineup}
        batting_details = []
        bowling_details = []
        batters_list = sm_match.get("batting") or []
        for b in batters_list:
             is_out = (b.get("catch_stump_player_id") or
                       b.get("runout_by_id") or
                       b.get("batsmanout_id") or
                       b.get("bowling_player_id"))
             if not is_out:
                  p_name = _get_player_name(player_map, b.get("player_id"))
                  active_marker = "*" if b.get("active") else ""
                  runs = b.get("score", 0)
                  balls = b.get("ball", 0)
                  sr = b.get("rate", 0)
                  batting_details.append(f"{p_name}{active_marker}: {runs}({balls}) SR:{sr}")
        bowlers_list = sm_match.get("bowling") or []
        active_bowler = next((bw for bw in bowlers_list if bw.get("active")), None)
        if active_bowler:
             p_name = _get_player_name(player_map, active_bowler.get("player_id"))
             o = active_bowler.get("overs", 0)
             r = active_bowler.get("runs", 0)
             w = active_bowler.get("wickets", 0)
             bowling_details.append(f"{p_name}: {w}/{r} ({o})")
        else:
             pass
        recent_balls_str = ""
        last_wicket_str = ""
        balls_data = sm_match.get("balls", [])
        if balls_data:
            recent_chunk = balls_data[-12:] if len(balls_data) > 12 else balls_data
            rb_tokens = []
            for ball in recent_chunk:
                is_wicket = ball.get("wicket") or ball.get("batsmanout_id")
                display_char = "."
                run_sc = ball.get("score", 0)
                if isinstance(run_sc, dict): run_sc = run_sc.get("runs", 0)
                if is_wicket:
                    display_char = "W"
                    out_p = _get_player_name(player_map, ball.get("batsmanout_id"))
                    how = "Out"
                    last_wicket_str = f"Last Wicket: {out_p} ({how})"
                elif run_sc == 4: display_char = "4"
                elif run_sc == 6: display_char = "6"
                else: display_char = str(run_sc)
                rb_tokens.append(display_char)
            recent_balls_str = " | Recent: " + " ".join(rb_tokens)
        overs_rem_str = ""
        try:
             m_type = str(sm_match.get("type") or "").upper()
             max_overs = 0
             if "T20" in m_type: max_overs = 20
             elif "ODI" in m_type: max_overs = 50
             if max_overs > 0 and current_score_obj:
                 cur_overs = float(current_score_obj.get("overs", 0))
                 total_balls = max_overs * 6
                 cur_balls_int = int(cur_overs) * 6 + int((cur_overs - int(cur_overs)) * 10)
                 rem_balls = total_balls - cur_balls_int
                 rem_overs_full = rem_balls // 6
                 rem_balls_part = rem_balls % 6
                 if rem_balls >= 0:
                     overs_rem_str = f" | Remaining: {rem_overs_full}.{rem_balls_part} Overs"
        except Exception:
             pass
        detailed_str = ""
        if batting_details:
             detailed_str += " | Batting: " + ", ".join(batting_details)
        if bowling_details:
             detailed_str += " | Bowling: " + ", ".join(bowling_details)
        inn_break_msg = " [INNINGS BREAK - 1st Inning Over, 2nd Yet to Start]" if formatted_status == "Innings Break" else ""
        starting_time = sm_match.get("starting_at", "N/A")
        final_score_string = f"Match Date: {starting_time} | " + " | ".join(scores_txt) + detailed_str + recent_balls_str + (" | " + last_wicket_str if last_wicket_str else "") + overs_rem_str + inn_break_msg
        is_actually_finished = (
            formatted_status == "Finished" or
            sm_match.get("winner_team_id") is not None or
            "won by" in note.lower() or
            "match drawn" in note.lower() or
            "match tied" in note.lower()
        )
        is_live_final = (not is_actually_finished) and (formatted_status in ["LIVE", "Innings Break", "Lunch", "Tea Break", "Dinner", "Drinks", "Delayed", "Stumps"])
        return {
            "id": m_id,
            "name": f"{local} vs {visitor}",
            "status": "Finished" if is_actually_finished else formatted_status,
            "original_status": status_raw,
            "score_string": final_score_string,
            "venue": sm_match.get("venue", {}).get("name"),
            "current_batting": current_batting_team,
            "note": note,
            "is_live": is_live_final,
            "raw_data": sm_match
        }
    except Exception as e:
        logger.error(f"Live Normalization Error: {e}")
        return None
async def fetch_realtime_matches(filter_team=None):
    """
    Fetches strictly LIVE matches from /livescores endpoint.
    If filter_team is provided, filters by that team name.
    """
    includes = "localteam,visitorteam,runs,venue,batting,bowling,lineup,balls"
    res_live = await sportmonks_live_request("/livescores", {"include": includes})
    live_matches = []
    logger.info(f"Fetch Realtime Matches called. Filter: {filter_team}")
    if res_live.get("ok"):
         data = res_live.get("data", [])
         logger.info(f"Livescores API returned {len(data)} items")
         for m in data:
             try:
                 if str(m.get("status", "")).lower() in ["finished", "completed"]:
                     asyncio.create_task(archive_match(m.get("id")))
                 norm = _normalize_live_match_data(m)
                 if norm:
                     logger.info(f" -> Found: {norm.get('name')} | Status: {norm.get('status')} | Live: {norm.get('is_live')}")
                     if filter_team:
                         t_filter = filter_team.lower().strip()
                         if t_filter in norm["name"].lower():
                              live_matches.append(norm)
                     else:
                         live_matches.append(norm)
                 else:
                     logger.warning(f" -> Failed normalization for match ID {m.get('id')}")
             except Exception as e:
                 logger.error(f"Error checking live match: {e}")
    else:
         logger.error(f"Livescores API Failed: {res_live.get('error')}")
    if not live_matches:
        logger.info("No live matches found in livescores, checking today's fixtures fallback...")
        today_str = datetime.now().strftime("%Y-%m-%d")
        includes = "localteam,visitorteam,runs,venue,batting,bowling,lineup,balls"
        res_today = await sportmonks_live_request(f"/fixtures", {
             "filter[starts_between]": f"{today_str},{today_str}",
             "include": includes
        })
        if res_today.get("ok"):
             data = res_today.get("data", [])
             logger.info(f"Fallback Fixtures found: {len(data)}")
             for m in data:
                 try:
                     norm = _normalize_live_match_data(m)
                     if norm and norm.get("is_live"):
                          if not any(lm["id"] == norm["id"] for lm in live_matches):
                               logger.info(f" -> FALLBACK FOUND: {norm['name']} ({norm['status']})")
                               if filter_team:
                                    t_filter = filter_team.lower().strip()
                                    if t_filter in norm["name"].lower():
                                         live_matches.append(norm)
                                    else:
                                         live_matches.append(norm)
                               else:
                                    live_matches.append(norm)
                 except Exception as e:
                      logger.error(f"Error processing fallback match: {e}")
        else:
             logger.error(f"Fallback Fixtures API Failed: {res_today.get('error')}")
    logger.info(f"Total Live Matches Returned: {len(live_matches)}")
    return live_matches
def _safe_int(val):
    try:
        if val is None: return 0
        return int(float(str(val)))
    except: return 0
def _get_top_performers_by_inning(scorecard):
    innings_data = []
    for inng in scorecard:
        i_name = inng.get("inning", "Unknown")
        scorers = []
        for b in inng.get("batting", []):
            try:
                bat_obj = b.get("batsman") or b.get("batter")
                p_name = bat_obj.get("name") if isinstance(bat_obj, dict) else str(bat_obj)
                r = _safe_int(b.get("runs") or b.get("r") or 0)
                scorers.append({"player": p_name, "runs": r})
            except: continue
        scorers.sort(key=lambda x: x["runs"], reverse=True)
        bowlers = []
        for b in inng.get("bowling", []):
            try:
                bowl_obj = b.get("bowler")
                p_name = bowl_obj.get("name") if isinstance(bowl_obj, dict) else str(bowl_obj)
                w = _safe_int(b.get("wickets") or b.get("w") or 0)
                r = _safe_int(b.get("runs") or b.get("r") or 0)
                bowlers.append({"player": p_name, "wickets": w, "runs": r})
            except: continue
        bowlers.sort(key=lambda x: (x["wickets"], -x["runs"]), reverse=True)
        innings_data.append({
            "inning": i_name,
            "top_scorers": scorers[:3],
            "top_bowlers": bowlers[:2]
        })
    return innings_data
async def get_live_match_details(match_id, use_cache=True):
    logger.info(f"Fetching Live Details: {match_id}")
    t1 = getMatchScorecard(match_id, ttl=15 if use_cache else 0)
    t2 = getMatchCommentary(match_id, ttl=15 if use_cache else 0)
    t3 = getMatchInfo(match_id, ttl=15 if use_cache else 0)
    t4 = getMatchSquad(match_id, ttl=60 if use_cache else 0)
    results = await asyncio.gather(t1, t2, t3, t4, return_exceptions=True)
    sc = results[0] if not isinstance(results[0], Exception) else {}
    comm = results[1] if not isinstance(results[1], Exception) else {}
    info = results[2] if not isinstance(results[2], Exception) else {}
    squad = results[3] if not isinstance(results[3], Exception) else {}
    if not sc or not sc.get("data"):
        return None
    data = sc["data"]
    info_data = info.get("data") or {}
    match_name = info_data.get("name") or data.get("name")
    status = str(info_data.get("status") or data.get("status", "")).lower()
    summary = {
        "match_name": match_name,
        "status": status.title(),
        "venue": info_data.get("venue"),
        "score_summary": [],
        "batting_team": "",
        "bowling_team": "",
        "commentary": [],
        "scorecard_stats": _get_top_performers_by_inning(data.get("scorecard", [])),
        "score": data.get("score"),
        "scores_full": data.get("score", []),
        "match_ended": info_data.get("matchEnded") or False
    }
    curr_inn = data.get("scorecard", [])[-1] if data.get("scorecard") else {}
    if curr_inn:
        inn_name = curr_inn.get("inning", "")
        summary["batting_team"] = inn_name.split(" Inning")[0]
    if comm.get("data"):
        summary["commentary"] = [c.get("comm", "") for c in comm["data"][:15]]
    return summary
def calculate_match_odds(match_details):
    if not match_details or not match_details.get("score"):
        return {"error": "Odds unavailable"}
    score = match_details["score"] or {}
    runs = _safe_int(score.get("runs") or score.get("r"))
    wickets = _safe_int(score.get("wickets") or score.get("w"))
    overs = float(score.get("overs") or score.get("o") or 0.0)
    odds = {"narrative": "Match Balanced"}
    if wickets < 3 and overs > 10:
        odds["narrative"] = "Batting team dominance."
    elif wickets > 7:
        odds["narrative"] = "Bowling team dominance."
    return odds
async def fetch_match_context_bundle(match_id):
    if not match_id: return None
    logger.info(f"Fetching Bundle: {match_id}")
    t1 = get_live_match_details(match_id)
    t2 = getMatchSquad(match_id)
    t3 = getMatchPoints(match_id)
    results = await asyncio.gather(t1, t2, t3, return_exceptions=True)
    details = results[0] if not isinstance(results[0], Exception) else {}
    squad = results[1] if not isinstance(results[1], Exception) else {}
    points = results[2] if not isinstance(results[2], Exception) else {}
    return {
        "match_id": match_id,
        "details": details,
        "squad": squad.get("data"),
        "fantasy_points": points.get("data"),
        "timestamp": datetime.now().isoformat()
    }
def extract_live_state(scorecard_data):
    if not scorecard_data or "scorecard" not in scorecard_data: return {}
    curr = scorecard_data["scorecard"][-1]
    batsmen = []
    for b in curr.get("batting", []):
        dism = (b.get("dismissal") or "").lower()
        if not dism or dism in ["batting", "not out"]:
            batsmen.append({
                "name": b.get("batsman", {}).get("name"),
                "runs": b.get("r"),
                "balls": b.get("b")
            })
    bowler = {}
    if curr.get("bowling"):
        active = curr["bowling"][-1] # Simplification
        bowler = {
            "name": active.get("bowler", {}).get("name"),
            "w": active.get("w"),
            "r": active.get("r")
        }
    return {
        "batsmen": batsmen,
        "bowler": bowler,
        "score": f"{curr.get('totals',{}).get('R')}/{curr.get('totals',{}).get('W')}"
    }