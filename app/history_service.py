
import os
import json
import sqlite3
import asyncio
from datetime import datetime
from app.utils_core import get_logger
from app.backend_core import sportmonks_cric, _normalize_sportmonks_to_app_format
from app.search_service import find_series_smart, find_player_id, _normalize

logger = get_logger("history_svc", "PAST_HISTORY.log")

from app.match_utils import _normalize, _is_team_match, _smart_ctx_match, _match_series_name

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

    # Dynamic Series Search Logic
    conn = get_history_conn()
    cursor = conn.cursor()
    
    potential_series = []
    if year:
        cursor.execute("SELECT * FROM series WHERE CAST(year AS TEXT) = ?", (str(year),))
    else:
        cursor.execute("SELECT * FROM series")
        
    all_series = [dict(r) for r in cursor.fetchall()]
    
    # Filter using logic
    tgt_series = None
    if series_name:
        for s in all_series:
            if _match_series_name(series_name, s['name']):
                tgt_series = s
                break
    elif year and len(all_series) > 0:
        # If no name but year provided, maybe return the biggest one? 
        # But safer to return None if ambiguity
        pass

    if not tgt_series:
        conn.close()
        logger.warning(f"Series summary not found for {series_name} ({year})")
        return None

    series_data = tgt_series
    series_id = series_data['id']


    try:
        cursor.execute("SELECT * FROM matches WHERE series_id=? ORDER BY starting_at DESC LIMIT 1", [series_id])
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
            
            # Infer Series Winner from Final Match
            # Note: winner_team_id is now the column, usually stores ID but can store name depending on sync logic (earlier we stored name in match_winner) 
            # If it's an ID, we might need name from `final_data['details']['matchWinner']`
            
            w_col = final_data.get('winner_team_id')
            if w_col:
                series_data['winner'] = w_col 
                # If w_col is an ID (digits), we try to get Name from details
                if str(w_col).isdigit() and final_data.get('details'):
                     series_data['winner'] = final_data['details'].get('matchWinner') or w_col

            elif "won by" in str(final_data.get('status', '')).lower():
                # Extract from status if needed
                pass
        
        # Calculate Standings / Points Table
        cursor.execute("SELECT name, status, winner_team_id FROM matches WHERE series_id=?", [series_id])
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
                winner = str(m['winner_team_id']) if m['winner_team_id'] else ""
                
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
             # Logic: Find series for year -> Get last match
             s_query = "SELECT id, name FROM series WHERE CAST(year AS TEXT)=?"
             cursor.execute(s_query, (str(year),))
             series_rows = [dict(r) for r in cursor.fetchall()]
             
             target_s_id = None
             # Try to match series name from query
             for s in series_rows:
                 # Check if any part of the query matches the series
                 # e.g. "IPL" in "IPL 2025 Final" -> Matches "Indian Premier League" via acronym logic
                 if _match_series_name(match_query_or_teams, s['name']):
                     target_s_id = s['id']
                     break
             
             # Fallback: If only one series exists for that year, assume it's that one
             if not target_s_id and len(series_rows) == 1:
                 target_s_id = series_rows[0]['id']
            
             if target_s_id:
                 # Get the last match of that series -> Determine it is the final
                 cursor.execute("SELECT * FROM matches WHERE series_id=? ORDER BY date DESC LIMIT 1", (target_s_id,))
                 match_row = cursor.fetchone()
                 if match_row:
                     m_data = dict(match_row)
                     if m_data.get('data_json'):
                         try: m_data['details'] = json.loads(m_data['data_json'])
                         except: pass
                     conn.close()
                     logger.info(f"✅ Found Final Match via Logic: {m_data.get('name')}")
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
        
    # 5. Series Filter (Logic based)
    if series:
        # We need to find the series ID or name first that matches
         cursor.execute("SELECT id, name FROM series") # Load all (cached ideally, but okay for now)
         all_s = cursor.fetchall()
         valid_ids = []
         for s in all_s:
             if _match_series_name(series, s['name']):
                 valid_ids.append(s['id'])
         
         if valid_ids:
             placeholders = ','.join(['?'] * len(valid_ids))
             conditions.append(f"s.id IN ({placeholders})")
             params.extend(valid_ids)
         else:
             # Name match fallback
             conditions.append("s.name LIKE ?")
             params.append(f"%{series}%")

    base_sql = """
        SELECT m.id, m.name, m.starting_at as date, m.status, m.venue, m.winner_team_id as match_winner, s.name as series_name, s.year
        FROM matches m
        JOIN series s ON m.series_id = s.id
    """
    
    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)
    
    base_sql += " ORDER BY m.starting_at DESC LIMIT ?"
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
         # Logic based ID resolution
         cursor.execute("SELECT id, name FROM series WHERE CAST(year AS TEXT)=?", (str(year),))
         rows = cursor.fetchall()
         target_ids = []
         for s in rows:
             if _match_series_name(series_name, s['name']):
                 target_ids.append(s['id'])
         
         if target_ids:
             placeholders = ','.join(['?'] * len(target_ids))
             series_filter_sql = f" AND s.id IN ({placeholders})"
             params.extend(target_ids)


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
         # Logic based ID resolution
         cursor.execute("SELECT id, name FROM series WHERE CAST(year AS TEXT)=?", (str(year),))
         rows = cursor.fetchall()
         target_ids = []
         for s in rows:
             if _match_series_name(series_name, s['name']):
                 target_ids.append(s['id'])
                 
         if target_ids:
             placeholders = ','.join(['?'] * len(target_ids))
             series_filter_sql = f" AND s.id IN ({placeholders})"
             params.extend(target_ids)

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
        SELECT name, starting_at as date, status, winner_team_id, venue
        FROM matches m
        JOIN series s ON m.series_id = s.id
        WHERE CAST(s.year AS TEXT) = ? AND (name LIKE ? OR name LIKE ?)
        ORDER BY starting_at ASC
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
        # Since matches doesn't have names in winner_team_id, our check if team_name in winner might fail if it's ID
        # We assume if winner_team_id is numeric, we might have mapped it. For now, we do a weak check or rely on status.
        # But 'matches' dict has 'winner_team_id'.
        
        wins = 0
        for m in matches:
             w_id = str(m.get('winner_team_id') or "")
             status = str(m.get('status') or "").lower()
             if team_name.lower() in w_id.lower() or f"{team_name} won" in status.lower(): # If it was a name 
                 wins += 1
             # If strictly ID, we can't count wins without ID-Name map. 
             # However, status usually says "X won by Y runs"
             elif "won" in status and team_name.lower() in status.lower():
                 wins += 1

        return {
            "team": team_name,
            "year": year,
            "matches": matches,
            "top_performers": top_players,
            "total_played": len(matches),
            "wins": wins
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
        if "highest" in uq and "total" in uq: category_key = "highest_team_total"
        elif "lowest" in uq and "total" in uq: category_key = "lowest_team_total"

    if not category_key:
        conn.close()
        return None

    try:
        # Special Handling for Team Totals (Querying 'matches' table instead of 'historical_records')
        if category_key in ["highest_team_total", "lowest_team_total"]:
            sort_order = "DESC" if category_key == "highest_team_total" else "ASC"
            
            # Find series_id for the year if provided
            series_clause = ""
            params = []
            if year:
                cursor.execute("SELECT id FROM series WHERE CAST(year AS TEXT) = ?", (str(year),))
                rows = cursor.fetchall()
                if rows:
                    placeholders = ",".join(["?"] * len(rows))
                    series_clause = f"AND m.series_id IN ({placeholders})"
                    params = [r[0] for r in rows]
            
            # Query scorecards joined with matches
            sql = f"""
                SELECT sc.runs, sc.team, m.name, m.starting_at, m.venue
                FROM scorecards sc
                JOIN matches m ON sc.match_id = m.id
                WHERE 1=1 {series_clause}
                ORDER BY sc.runs {sort_order} LIMIT 1
            """
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if row:
                res = {
                    "category": category_key,
                    "details": f"{row[0]} by {row[1]}",
                    "description": f"Match: {row[2]}, Venue: {row[4]}",
                    "match_info": row[2]
                }
                conn.close()
                return res
            else:
                 pass # Fallback to historical_records check if scorecards empty

        if year:
            cursor.execute("SELECT * FROM historical_records WHERE year=? AND category=?", (str(year), category_key))
        else:
            cursor.execute("SELECT * FROM historical_records WHERE category=? ORDER BY year DESC", (category_key,))
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
            INSERT OR REPLACE INTO matches (id, series_id, name, status, starting_at, venue, winner_team_id)
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
            await save_match_deep_details(match_id, series_id)

    conn.commit()
    conn.close()
    logger.info(f"Sync complete for series {series_id} (Matches: {len(fixtures)})")
    return True

async def save_match_deep_details(match_id, series_id):
    """
    Fetches and saves full details for a single completed match.
    """
    logger.info(f"Saving Deep Details for Match: {match_id}")
    
    # Fetch Full Match Details
    deep_res = await sportmonks_cric(f"/fixtures/{match_id}", {
        "include": "batting.batsman,bowling.bowler,runs,scoreboards,venue,manofmatch,localteam,visitorteam"
    })
    
    if not deep_res.get("ok"):
        logger.error(f"Failed to fetch details for {match_id}")
        return False
        
    m_data = deep_res.get("data", {})
    conn = get_history_conn()
    cursor = conn.cursor()
    
    try:
        # A. Store Full JSON
        json_str = json.dumps(m_data)
        cursor.execute("UPDATE matches SET data_json = ? WHERE id = ?", (json_str, str(match_id)))
        
        # B. Populate Player Stats
        batting_data = m_data.get("batting", [])
        bowling_data = m_data.get("bowling", [])
        processed_players = {}

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

        for pid, stats in processed_players.items():
            cursor.execute("""
                INSERT OR REPLACE INTO player_stats (match_id, player_id, name, runs, balls, fours, sixes, wickets, overs_bowled, conceded)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (str(match_id), str(pid), stats["name"], stats["runs"], stats["balls"], stats["4s"], stats["6s"], stats["w"], stats["o"], stats["conc"]))

        # C. Populate Scorecards
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
            """, (str(match_id), team_n, int(s.get("score") or s.get("runs") or 0), int(s.get("wickets") or 0), float(s.get("overs") or 0), int(s.get("inning") or 1)))

        # D. Fantasy Points
        for pid, stats in processed_players.items():
            pts = stats["runs"] + (stats["w"] * 25) + (stats["4s"] * 1) + (stats["6s"] * 2)
            if stats["runs"] >= 50: pts += 8
            if stats["runs"] >= 100: pts += 16
            if stats["w"] >= 3: pts += 8
            
            cursor.execute("""
                INSERT OR REPLACE INTO fantasy_points (match_id, player_id, player_name, total_points)
                VALUES (?, ?, ?, ?)
            """, (str(match_id), str(pid), stats["name"], pts))

        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error saving match details {match_id}: {e}")
        return False
    finally:
        conn.close()

async def sync_recent_finished_matches(days_back=2):
    """
    Checks for finished matches from recent days and saves them to DB.
    Automatic trigger function.
    """
    logger.info(f"Auto-Syncing Finished Matches (Last {days_back} days)")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Fetch Fixtures
    range_str = f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}"
    res = await sportmonks_cric("/fixtures", {
        "filter[starts_between]": range_str,
        "include": "localteam,visitorteam,venue"
    })
    
    if not res.get("ok"):
        logger.error("Failed to fetch recent fixtures for sync")
        return {"status": "error", "message": "API Fail"}
        
    fixtures = res.get("data", [])
    synced_count = 0
    
    conn = get_history_conn()
    cursor = conn.cursor()
    
    for f in fixtures:
        status = str(f.get("status", "")).lower()
        if status in ["finished", "completed"] or "won" in status:
            match_id = str(f.get("id"))
            series_id = str(f.get("season_id") or f.get("league_id"))
            
            # Insert Stub first if missing
            cursor.execute("""
                INSERT OR REPLACE INTO matches (id, series_id, name, status, starting_at, venue, winner_team_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                str(match_id),
                str(series_id),
                f.get("name") or f"{f.get('localteam', {}).get('name')} vs {f.get('visitorteam', {}).get('name')}",
                "Finished",
                f.get("starting_at", "").split("T")[0],
                f.get("venue", {}).get("name") if isinstance(f.get("venue"), dict) else None,
                f.get("winner_team_id")
            ))
            conn.commit()
            
            # Now Deep Save
            saved = await save_match_deep_details(match_id, series_id)
            if saved: synced_count += 1
            
    conn.close()
    result = {"status": "success", "synced_matches": synced_count, "total_checked": len(fixtures)}
    logger.info(f"Sync Result: {result}")
    return result

async def past_db_get_standings(year, series_name=None):
    """
    Wrapper to get standings from local DB via get_series_history_summary.
    """
    summary = await get_series_history_summary(series_name, year)
    if summary and "standings" in summary:
        return {"ok": True, "data": summary["standings"]}
    return {"ok": False, "error": "No historical standings found in DB"}

async def execute_smart_query(schema):
    """
    Executes a structured query against the local database based on the 'Schema-Aware' JSON.
    Handles complex logic like aggregations, comparisons, and rankings.
    """
    conn = get_history_conn()
    cursor = conn.cursor()
    
    intent = schema.get("intent", "").upper()
    struct = schema.get("structured_schema", {})
    query_type = struct.get("query_type", "fact")
    metrics = struct.get("metrics", [])
    filters = struct.get("filters", {})
    season = struct.get("season")
    tournament = struct.get("tournament")
    teams = struct.get("teams", [])
    players = struct.get("players", [])
    
    logger.info(f"🧠 SMART QUERY: Type={query_type}, Metrics={metrics}, Filters={filters}")
    
    # helper to resolve series IDs
    series_ids = []
    if season or tournament:
        sql = "SELECT id FROM series WHERE 1=1"
        p = []
        if season:
            sql += " AND CAST(year AS TEXT) = ?"
            p.append(str(season))
        if tournament:
            sql += " AND name LIKE ?"
            p.append(f"%{tournament}%")
        cursor.execute(sql, p)
        series_ids = [r[0] for r in cursor.fetchall()]
    
    results = {}
    
    try:
        # CASE 1: RANKING / LEADERBOARD (e.g. "Highest run scorer", "Most sixes")
        if query_type == "ranking" or (query_type == "fact" and ("most" in str(struct) or "highest" in str(struct))):
            metric_map = {
                "runs": "runs", "wickets": "wickets", "sixes": "sixes", 
                "fours": "fours", "catches": "catch" 
            }
            target_metric = None
            for m in metrics:
                if m in metric_map: target_metric = metric_map[m]; break
            
            if target_metric:
                limit = filters.get("limit", 5)
                # Build SQL
                where_clauses = []
                params = []
                if series_ids:
                    placeholders = ",".join("?" * len(series_ids))
                    where_clauses.append(f"m.series_id IN ({placeholders})")
                    params.extend(series_ids)
                
                venue_filter = filters.get("venue")
                if venue_filter:
                    where_clauses.append("m.venue LIKE ?")
                    params.append(f"%{venue_filter}%")
                
                where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                sql = f"""
                    SELECT ps.name, ps.team, SUM(ps.{target_metric}) as total, COUNT(ps.match_id) as innings
                    FROM player_stats ps
                    JOIN matches m ON ps.match_id = m.id
                    WHERE {where_str}
                    GROUP BY ps.name
                    ORDER BY total DESC
                    LIMIT ?
                """
                params.append(limit)
                cursor.execute(sql, params)
                rows = [dict(r) for r in cursor.fetchall()]
                results["ranking"] = rows
                results["summary"] = f"Found top {len(rows)} players for {target_metric}."

        # CASE 2: FACT / STAT LOOKUP
        # 2a. Series Stats (Winner, Final, Count)
        if "winner" in metrics or "standings" in metrics or "final" in str(struct):
            s_summary = await get_series_history_summary(tournament, season)
            if s_summary: results["series_summary"] = s_summary
        
        # 2b. Match Specific (e.g. "Scorecard of Final")
        if struct.get("match_type") == "final":
             # We rely on search_historical_matches or get_historical_match_details
             # But let's check specifically for final
             if season:
                 m_final = await get_historical_match_details(f"{tournament or 'IPL'} Final", year=season)
                 if m_final: results["final_match"] = m_final

        # 2c. Player Record (e.g. "Fastest 50")
        stat_cat = filters.get("stat_category")
        if stat_cat:
            recs = await get_season_records(season, stat_cat) 
            if recs: results["records"] = recs

        # CASE 3: COMPARISON (e.g. "Compare MI and CSK")
        if query_type == "comparison" and len(teams) >= 2:
             h2h = await get_head_to_head_history(teams[0], teams[1])
             results["h2h_summary"] = {"total_matches": len(h2h), "data": h2h[:5]}
             
             if season:
                 t1_sum = await get_team_season_summary(teams[0], season)
                 t2_sum = await get_team_season_summary(teams[1], season)
                 results["season_comparison"] = {teams[0]: t1_sum, teams[1]: t2_sum}

        # CASE 4: CLOSEST MATCHES (Last Ball Finish)
        if filters.get("match_result") == "close_finish":
            # Search for specific status keywords
            sql = """
                SELECT * FROM matches 
                WHERE (status LIKE '%won by 1 run%' OR status LIKE '%won by 1 wicket%' 
                OR status LIKE '%super over%' OR status LIKE '%last ball%')
            """
            params_close = []
            if series_ids:
                 placeholders = ",".join("?" * len(series_ids))
                 sql += f" AND series_id IN ({placeholders})"
                 params_close.extend(series_ids)
            
            sql += " ORDER BY starting_at DESC LIMIT 5"
            cursor.execute(sql, params_close)
            results["thrillers"] = [dict(r) for r in cursor.fetchall()]

    except Exception as e:
        logger.error(f"Smart Query Error: {e}")
        results["error"] = str(e)
    
    conn.close()
    return results
