
import os
import json
import sqlite3
import asyncio
from datetime import datetime
from app.utils_core import get_logger
from app.backend_core import sportmonks_cric, _normalize_sportmonks_to_app_format
from app.search_service import find_series_smart, find_player_id, _normalize

logger = get_logger("history_svc", "PAST_HISTORY.log")

DB_PATH = os.path.join("data", "legacy_ipl_wpl.db")

def get_history_conn():
    """Returns a connection to the historical database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

async def get_player_past_performance(player_name, series_name=None, year=None):
    """
    Fetches detailed historical stats for a player from the local DB (player_stats table).
    """
    logger.info(f"Historical Query: Player={player_name}, Series={series_name}, Year={year}")
    conn = get_history_conn()
    cursor = conn.cursor()

    # Base Query on player_stats
    query = """
        SELECT ps.*, m.name as match_name, m.date, m.venue, s.name as series_name, s.year
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.id
        JOIN series s ON m.series_id = s.id
        WHERE ps.name LIKE ?
    """
    params = [f"%{player_name}%"]

    if year:
        query += " AND CAST(s.year AS TEXT) = ?"
        params.append(str(year))
    if series_name:
        query += " AND s.name LIKE ?"
        params.append(f"%{series_name}%")

    query += " ORDER BY m.date DESC"

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]
        conn.close()
        logger.info(f"Found {len(results)} detailed records for {player_name}")
        return results
    except Exception as e:
        logger.error(f"Error fetching player history: {e}")
        conn.close()
        return []

async def get_head_to_head_history(team_a, team_b):
    """
    Fetches all past encounters between two teams from the local DB.
    """
    logger.info(f"Historical H2H: {team_a} vs {team_b}")
    conn = get_history_conn()
    cursor = conn.cursor()


    query = """
        SELECT * FROM matches
        WHERE (name LIKE ? AND name LIKE ?)
        OR (name LIKE ? AND name LIKE ?)
    """
    params = [f"%{team_a}%", f"%{team_b}%", f"%{team_b}%", f"%{team_a}%"]

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]
        conn.close()
        logger.info(f"Found {len(results)} H2H matches between {team_a} and {team_b}")
        return results
    except Exception as e:
        logger.error(f"Error fetching H2H history: {e}")
        conn.close()
        return []

async def get_series_history_summary(series_name, year=None):
    """
    Fetches a summary of a past series (Winner, Standings, etc.)
    """
    logger.info(f"Historical Series Summary: {series_name} ({year})")
    conn = get_history_conn()
    cursor = conn.cursor()

    # Handle Aliases
    search_name = series_name
    if series_name:
        s_lower = series_name.lower()
        if "ipl" in s_lower: search_name = s_lower.replace("ipl", "indian premier league")
        elif "wpl" in s_lower: search_name = s_lower.replace("wpl", "women's premier league")
    
    query = "SELECT * FROM series WHERE name LIKE ?"
    params = [f"%{search_name}%"]
    if year:
        query += " AND year = ?"
        params.append(str(year))

    try:

        cursor.execute(query, params)
        series_row = cursor.fetchone()
        if not series_row:
            # Fallback: Try exact year match if name search failed but year was provided
            if year: 
                 # Try finding any major league in that year if specific name failed
                 # This is a bit risky but better than nothing for "Winner of 2025"
                 pass
            
            conn.close()
            logger.warning(f"Series summary not found for {series_name} (Search: {search_name})")
            return None

        series_data = dict(series_row)
        series_id = series_data['id']


        cursor.execute("SELECT * FROM matches WHERE series_id=? ORDER BY date DESC LIMIT 1", [series_id])
        final_row = cursor.fetchone()

        if final_row:
            final_data = dict(final_row)
            if final_data.get('data_json'):
                try:
                    full_details = json.loads(final_data['data_json'])
                    final_data['details'] = full_details
                except:
                    pass
            series_data['final_match'] = final_data
        
        # Calculate Standings / Points Table
        cursor.execute("SELECT name, status, match_winner FROM matches WHERE series_id=?", [series_id])
        all_matches = cursor.fetchall()
        
        points = {}
        total_matches = len(all_matches)
        series_data['total_matches'] = total_matches
        
        for m in all_matches:
            # Extract team names from match name "A vs B"
            parts = m['name'].split(" vs ")
            if len(parts) == 2:
                t1, t2 = parts[0].strip(), parts[1].strip()
                if t1 not in points: points[t1] = {"p": 0, "w": 0, "l": 0, "nr": 0}
                if t2 not in points: points[t2] = {"p": 0, "w": 0, "l": 0, "nr": 0}
                
                status = str(m['status']).lower()
                winner = str(m['match_winner']) if m['match_winner'] else ""
                
                if "abandoned" in status or "no result" in status:
                    points[t1]["p"] += 1; points[t1]["nr"] += 1
                    points[t2]["p"] += 1; points[t2]["nr"] += 1
                elif winner:
                    # If match_winner stores name
                    if t1 in winner: 
                        points[t1]["p"] += 2; points[t1]["w"] += 1
                        points[t2]["l"] += 1
                    elif t2 in winner: 
                        points[t2]["p"] += 2; points[t2]["w"] += 1
                        points[t1]["l"] += 1
                    elif winner in points: # exact match
                         points[winner]["p"] += 2; points[winner]["w"] += 1
                         # lose for other?
                         other = t2 if winner == t1 else t1
                         if other in points: points[other]["l"] += 1
        
        sorted_standings = sorted([{"team": k, **v} for k, v in points.items()], key=lambda x: x['p'], reverse=True)
        series_data['standings'] = sorted_standings


        cursor.execute("""
            SELECT player_name, SUM(total_points) as total_pts
            FROM fantasy_points fp
            JOIN matches m ON fp.match_id = m.id
            WHERE m.series_id = ?
            GROUP BY player_name
            ORDER BY total_pts DESC
            LIMIT 5
        """, [series_id])
        top_players = [dict(row) for row in cursor.fetchall()]

        series_data['top_performers'] = top_players
        conn.close()
        return series_data
    except Exception as e:
        logger.error(f"Error fetching series summary: {e}")
        conn.close()
        return None

async def get_historical_match_details(match_query_or_teams, year=None):
    """
    Fetches detailed match info including JSON scorecard for specific match queries.
    """
    conn = get_history_conn()
    cursor = conn.cursor()


    query = "SELECT * FROM matches WHERE name LIKE ?"
    params = [f"%{match_query_or_teams}%"]

    if "final" in match_query_or_teams.lower():

        if year:
             # Smarter Series Lookup using the query string to differntiate IPL vs WPL
             q_lower = match_query_or_teams.lower()
             
             series_filter = ""
             if "ipl" in q_lower: series_filter = " AND name LIKE '%Indian Premier League%'"
             elif "wpl" in q_lower: series_filter = " AND name LIKE '%Women''s Premier League%'"
             
             s_query = f"SELECT id FROM series WHERE CAST(year AS TEXT)=?{series_filter}"
             cursor.execute(s_query, (str(year),))
             s_row = cursor.fetchone()
             
             if s_row:
                 # Get the last match of that series
                 cursor.execute("SELECT * FROM matches WHERE series_id=? ORDER BY date DESC LIMIT 1", (s_row['id'],))
                 match_row = cursor.fetchone()
                 if match_row:
                     m_data = dict(match_row)
                     if m_data.get('data_json'):
                         try: m_data['details'] = json.loads(m_data['data_json'])
                         except: pass
                     conn.close()
                     return m_data


    if year:
        query = "SELECT m.* FROM matches m JOIN series s ON m.series_id = s.id WHERE m.name LIKE ? AND CAST(s.year AS TEXT) = ?"
        params.append(str(year))
    else:
        query = "SELECT m.* FROM matches m WHERE m.name LIKE ? ORDER BY date DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        conn.close()
        return None


    # Prefer matches with data_json present
    match_data = None
    for row in rows:
        row_dict = dict(row)
        if row_dict.get('data_json'):
            match_data = row_dict
            break
    
    if not match_data:
        match_data = dict(rows[0])

    if match_data.get('data_json'):
        try:
            match_data['details'] = json.loads(match_data['data_json'])
        except:
            pass

    conn.close()

    if match_data:
        logger.info(f"Found match details for: {match_data.get('name')}")
    else:
        logger.warning(f"No match details found for query: {match_query_or_teams}")
    return match_data


async def search_historical_matches(query=None, team=None, year=None, date=None, series=None, limit=5):
    """
    Flexible search for multiple matches. Returns a list of matches.
    Used when the query might be ambiguous (e.g. "Mumbai vs Chennai" -> multiple matches).
    """
    conn = get_history_conn()
    cursor = conn.cursor()
    
    conditions = []
    params = []

    # 1. Text Query (e.g. "Final", "Qualifier", or team names embedded)
    if query:
        conditions.append("m.name LIKE ?")
        params.append(f"%{query}%")
    
    # 2. Team Filter (Check both sides)
    if team:
        conditions.append("(m.name LIKE ? OR m.name LIKE ?)")
        # Simple heuristic: team name appears in match string
        params.append(f"%{team}%")
        params.append(f"%{team}%")

    # 3. Year Filter
    if year:
        conditions.append("CAST(s.year AS TEXT) = ?")
        params.append(str(year))
        
    # 4. Date Filter
    if date:
        conditions.append("m.date = ?")
        params.append(str(date))
        
    # 5. Series Filter
    if series:
        s_lower = series.lower()
        if "ipl" in s_lower: s_lower = s_lower.replace("ipl", "indian premier league")
        elif "wpl" in s_lower: s_lower = s_lower.replace("wpl", "women's premier league")
        conditions.append("s.name LIKE ?")
        params.append(f"%{s_lower}%")

    base_sql = """
        SELECT m.id, m.name, m.date, m.status, m.venue, m.match_winner, s.name as series_name, s.year
        FROM matches m
        JOIN series s ON m.series_id = s.id
    """
    
    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)
    
    base_sql += " ORDER BY m.date DESC LIMIT ?"
    params.append(limit)
    
    try:
        cursor.execute(base_sql, params)
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"Search error: {e}")
        conn.close()
        return []

async def get_all_historical_matches(search_query=None, limit=50):
    """
    Fetches historical matches from the local DB, optionally filtering by a search query.
    If no query is provided, returns the most recent matches.
    """
    conn = get_history_conn()
    cursor = conn.cursor()

    if search_query:
        query = "SELECT * FROM matches WHERE name LIKE ? ORDER BY date DESC LIMIT ?"
        params = [f"%{search_query}%", limit]
        logger.info(f"Searching DB for generic match query: {search_query}")
    else:
        query = "SELECT * FROM matches ORDER BY date DESC LIMIT ?"
        params = [limit]
        logger.info(f"Fetching recent historical matches from DB (Limit: {limit})")

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        results = []
        for row in rows:
            d = dict(row)
            if d.get('data_json'):
                try: d['details'] = json.loads(d['data_json'])
                except: pass
            results.append(d)
        
        conn.close()
        return results
    except Exception as e:
        logger.error(f"Error fetching all historical matches: {e}")
        conn.close()
        return []

async def get_season_leaders(year, category="points", series_name=None):
    """
    Returns top players for a specific season (year) using `player_stats` (Real Stats) or `fantasy_points` (MVP).
    Optionally filters by series name.
    """
    conn = get_history_conn()
    cursor = conn.cursor()

    series_filter_sql = ""
    params = [str(year)]
    
    if series_name:
         s_lower = series_name.lower()
         target_s = s_lower
         if "ipl" in s_lower: target_s = s_lower.replace("ipl", "indian premier league")
         elif "wpl" in s_lower: target_s = s_lower.replace("wpl", "women's premier league")
         
         series_filter_sql = " AND s.name LIKE ?"
         params.append(f"%{target_s}%")


    if category == "points":
        query = f"""
            SELECT fp.player_name, SUM(fp.total_points) as score
            FROM fantasy_points fp
            JOIN matches m ON fp.match_id = m.id
            JOIN series s ON m.series_id = s.id
            WHERE CAST(s.year AS TEXT) = ?{series_filter_sql}
            GROUP BY fp.player_name ORDER BY score DESC LIMIT 5
        """
        try:
             cursor.execute(query, params)
             res = [dict(row) for row in cursor.fetchall()]
             conn.close()
             return res
        except:
             conn.close()
             return []


    col_map = {
        "batting": "runs",
        "runs": "runs",
        "bowling": "wickets",
        "wickets": "wickets",
        "sixes": "sixes",
        "fours": "fours"
    }
    
    if category not in col_map and category != "points":
        conn.close()
        return []

    target_col = col_map.get(category, "runs")

    query = f"""
        SELECT
            ps.name as player_name,
            ps.team as player_team,
            SUM(ps.{target_col}) as score,
            COUNT(DISTINCT ps.match_id) as matches_played
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.id
        JOIN series s ON m.series_id = s.id
        WHERE CAST(s.year AS TEXT) = ?{series_filter_sql}
        GROUP BY ps.name
        ORDER BY score DESC
        LIMIT 10
    """

    try:
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    except Exception as e:
        logger.error(f"Error fetching season leaders (stats): {e}")
        conn.close()
        return []

async def get_season_match_stats(year, type="highest", series_name=None):
    """
    Returns highest or lowest team totals for a season.
    Optionally filters by series name.
    """
    conn = get_history_conn()
    cursor = conn.cursor()
    order = "DESC" if type == "highest" else "ASC"
    
    series_filter_sql = ""
    params = [str(year)]
    
    if series_name:
         s_lower = series_name.lower()
         target_s = s_lower
         if "ipl" in s_lower: target_s = s_lower.replace("ipl", "indian premier league")
         elif "wpl" in s_lower: target_s = s_lower.replace("wpl", "women's premier league")
         
         series_filter_sql = " AND s.name LIKE ?"
         params.append(f"%{target_s}%")

    query = f"""
        SELECT sc.team, sc.runs, sc.wickets, sc.overs, sc.inning, m.name as match_name, m.date
        FROM scorecards sc
        JOIN matches m ON sc.match_id = m.id
        JOIN series s ON m.series_id = s.id
        WHERE CAST(s.year AS TEXT) = ?{series_filter_sql}
        ORDER BY sc.runs {order}
        LIMIT 5
    """
    try:
        cursor.execute(query, params)
        res = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return res
    except:
        conn.close()
        return []

async def get_team_season_summary(team_name, year):
    """
    Provides a deep dive into a team's performance for a specific year.
    """
    conn = get_history_conn()
    cursor = conn.cursor()
    
    # 1. Matches played and results
    query = """
        SELECT name, date, status, match_winner, venue
        FROM matches m
        JOIN series s ON m.series_id = s.id
        WHERE CAST(s.year AS TEXT) = ? AND (name LIKE ? OR name LIKE ?)
        ORDER BY date ASC
    """
    try:
        cursor.execute(query, (str(year), f"%{team_name}%", f"%{team_name}%"))
        matches = [dict(row) for row in cursor.fetchall()]
        
        # 2. Top performers for this team
        stats_query = """
            SELECT ps.name, SUM(ps.runs) as total_runs, SUM(ps.wickets) as total_wickets
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.id
            JOIN series s ON m.series_id = s.id
            WHERE CAST(s.year AS TEXT) = ? AND ps.team LIKE ?
            GROUP BY ps.name
            ORDER BY total_runs DESC LIMIT 3
        """
        cursor.execute(stats_query, (str(year), f"%{team_name}%"))
        top_players = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return {
            "team": team_name,
            "year": year,
            "matches": matches,
            "top_performers": top_players,
            "total_played": len(matches),
            "wins": len([m for m in matches if team_name.lower() in str(m['match_winner']).lower()])
        }
    except:
        conn.close()
        return None


async def get_season_records(year, user_query):
    """
    Fetches specific records (trivia) like Fastest 50, Most Sixes from the `historical_records` table.
    If year is None, searches for the all-time best record across all years.
    """
    conn = get_history_conn()
    cursor = conn.cursor()

    uq = user_query.lower()
    category_key = None

    if "fastest" in uq and ("50" in uq or "fifty" in uq): category_key = "fastest_fifty"
    elif "fastest" in uq and ("100" in uq or "century" in uq): category_key = "fastest_century"
    elif ("highest" in uq or "best" in uq) and "score" in uq: category_key = "highest_score"
    elif "most" in uq and "six" in uq: category_key = "most_sixes"
    elif "most" in uq and "four" in uq: category_key = "most_fours"
    elif ("highest" in uq or "best" in uq) and "wickets" in uq: category_key = "most_wickets"
    elif "catch" in uq: category_key = "most_catches"

    if not category_key:
        conn.close()
        return None

    try:
        if year:
            cursor.execute("SELECT * FROM historical_records WHERE year=? AND category=?", (str(year), category_key))
        else:
            # All-time search (logic-driven)
            # For records like 'fastest_fifty', we want the one with the lowest details numeric value if possible
            # But the table stores strings like '14 balls'. 
            # We'll just return the top entry or list them for the AI to decide.
            cursor.execute("SELECT * FROM historical_records WHERE category=? ORDER BY year DESC", (category_key,))
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows: return None
        
        results = [dict(r) for r in rows]
        # If searching all-time, we return the whole list for the Research Agent to pick the absolute best
        return results if not year else results[0]
    except Exception as e:
        logger.error(f"Error in get_season_records: {e}")
        conn.close()
        return None

async def sync_historical_series(series_id):
    """
    Populates the local DB with data for a specific series from the API.
    Captures FULL match details, player stats, and scorecards.
    """
    logger.info(f"Syncing Historical Series: {series_id}")

    # Fetch simple fixture list first
    # 1. Fetch Season & League Info
    res = await sportmonks_cric(f"/seasons/{series_id}", {"include": "league"})
    
    data = {}
    fixtures = []

    if res.get("ok"):
        data = res.get("data", {})
        # Then fetch fixtures separately with needed includes
        res_fix = await sportmonks_cric("/fixtures", {
            "filter[season_id]": series_id,
            "include": "runs,localteam,visitorteam"
        })
        if res_fix.get("ok"):
            fixtures = res_fix.get("data", [])
    else:
        logger.warning(f"Full fetch failed for {series_id}, trying split fetch...")
        # Split fetch: Season Info first
        res_info = await sportmonks_cric(f"/seasons/{series_id}", {"include": "league"})
        if not res_info.get("ok"):
             logger.error(f"Failed to fetch season info for {series_id}: {res_info.get('error')}")
             return False
        data = res_info.get("data", {})
        
        # Then Fixtures
        res_fix = await sportmonks_cric("/fixtures", {
            "filter[season_id]": series_id,
            "include": "runs,localteam,visitorteam"
        })
        if res_fix.get("ok"):
             fixtures = res_fix.get("data", [])
        else:
             logger.warning(f"Could not fetch fixtures for {series_id} separately: {res_fix.get('error')}")

    conn = get_history_conn()
    cursor = conn.cursor()

    # 1. Sync Series Info
    try:
        league_name = data.get("league", {}).get("name", "")
        season_name = data.get("name", "")
        full_name = f"{league_name} {season_name}".strip()
        
        # Extract Year safely
        import re
        year_match = re.search(r"(\d{4})", season_name)
        year = int(year_match.group(1)) if year_match else None

        cursor.execute("""
            INSERT OR REPLACE INTO series (id, name, year, start_date, end_date)
            VALUES (?, ?, ?, ?, ?)
        """, (
            str(series_id),
            full_name,
            year,
            None,
            None
        ))
    except Exception as e:
        logger.error(f"Error inserting series {series_id}: {e}")

    # 2. Process Matches
    for f in fixtures:
        match_id = str(f.get("id"))
        status = f.get("status")
        
        # Insert Basic Match Info
        cursor.execute("""
            INSERT OR REPLACE INTO matches (id, series_id, name, status, date, venue, match_winner)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            match_id,
            str(series_id),
            f.get("name") or f"{f.get('localteam', {}).get('name')} vs {f.get('visitorteam', {}).get('name')}",
            status,
            f.get("starting_at", "").split("T")[0],
            f.get("venue", {}).get("name") if isinstance(f.get("venue"), dict) else None,
            f.get("winner_team_id")
        ))

        # If match is finished, fetch DEEP details
        if status in ["Finished", "Completed"] or "won" in str(status).lower():
            # Fetch Full Match Details (Scorecard, Batting, Bowling, etc.)
            deep_res = await sportmonks_cric(f"/fixtures/{match_id}", {
                "include": "batting.batsman,bowling.bowler,runs,scoreboards,venue,manofmatch,localteam,visitorteam"
            })
            
            if deep_res.get("ok"):
                m_data = deep_res.get("data", {})
                
                # A. Store Full JSON (Truth Source)
                try:
                    json_str = json.dumps(m_data)
                    cursor.execute("UPDATE matches SET data_json = ? WHERE id = ?", (json_str, match_id))
                except Exception as e:
                    logger.error(f"JSON saving error for {match_id}: {e}")

                # B. Populate Player Stats (Aggregated)
                batting_data = m_data.get("batting", [])
                bowling_data = m_data.get("bowling", [])
                
                processed_players = {} # map player_id -> stats dict

                for b in batting_data:
                    pid = b.get("player_id")
                    if not pid: continue
                    if pid not in processed_players: processed_players[pid] = {"name": "", "runs": 0, "balls": 0, "4s": 0, "6s": 0, "w": 0, "o": 0, "conc": 0}
                    
                    p_obj = b.get("batsman") or {}
                    processed_players[pid]["name"] = p_obj.get("fullname") or p_obj.get("name") or "Unknown"
                    processed_players[pid]["runs"] += int(b.get("score") or b.get("runs") or 0)
                    processed_players[pid]["balls"] += int(b.get("ball") or b.get("balls") or 0)
                    processed_players[pid]["4s"] += int(b.get("four_x") or b.get("fours") or 0)
                    processed_players[pid]["6s"] += int(b.get("six_x") or b.get("sixes") or 0)

                for b in bowling_data:
                    pid = b.get("player_id")
                    if not pid: continue
                    if pid not in processed_players: processed_players[pid] = {"name": "", "runs": 0, "balls": 0, "4s": 0, "6s": 0, "w": 0, "o": 0, "conc": 0}
                    
                    p_obj = b.get("bowler") or {}
                    if not processed_players[pid]["name"]: 
                         processed_players[pid]["name"] = p_obj.get("fullname") or p_obj.get("name") or "Unknown"
                    
                    processed_players[pid]["w"] += int(b.get("wickets") or 0)
                    processed_players[pid]["o"] += float(b.get("overs") or 0)
                    processed_players[pid]["conc"] += int(b.get("runs") or 0)

                # Insert into player_stats
                for pid, stats in processed_players.items():
                    cursor.execute("""
                        INSERT OR REPLACE INTO player_stats (match_id, player_id, name, runs, balls, fours, sixes, wickets, overs_bowled, conceded)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        match_id, str(pid), stats["name"], 
                        stats["runs"], stats["balls"], stats["4s"], stats["6s"],
                        stats["w"], stats["o"], stats["conc"]
                    ))

                # C. Populate Scorecards (Team Totals)
                scores = m_data.get("runs", [])
                l_id = str(m_data.get("localteam_id"))
                v_id = str(m_data.get("visitorteam_id"))
                l_name = m_data.get("localteam", {}).get("name", "Local")
                v_name = m_data.get("visitorteam", {}).get("name", "Visitor")
                
                for s in scores:
                    tid = str(s.get("team_id"))
                    team_n = "Unknown"
                    if tid == l_id: team_n = l_name
                    elif tid == v_id: team_n = v_name
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO scorecards (match_id, team, runs, wickets, overs, inning)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        match_id,
                        team_n,
                        int(s.get("score") or s.get("runs") or 0),
                        int(s.get("wickets") or 0),
                        float(s.get("overs") or 0),
                        int(s.get("inning") or 1)
                    ))

                # D. Fantasy Points (MVP)
                for pid, stats in processed_players.items():
                    pts = stats["runs"] # 1 pt per run
                    pts += stats["w"] * 25 # 25 pts per wicket
                    pts += stats["4s"] * 1
                    pts += stats["6s"] * 2
                    if stats["runs"] >= 50: pts += 8
                    if stats["runs"] >= 100: pts += 16
                    if stats["w"] >= 3: pts += 8
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO fantasy_points (match_id, player_id, player_name, total_points)
                        VALUES (?, ?, ?, ?)
                    """, (match_id, str(pid), stats["name"], pts))

    conn.commit()
    conn.close()
    logger.info(f"Sync complete for series {series_id} (Matches: {len(fixtures)})")
    return True
