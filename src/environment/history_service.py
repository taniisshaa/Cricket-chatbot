import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from src.utils.utils_core import get_logger
# Import the PG version of the engine
from src.core.universal_cricket_engine import handle_universal_cricket_query, _process_raw_json_results, DB_CONFIG
import json
from datetime import datetime, timedelta
from src.environment.backend_core import sportmonks_cric

logger = get_logger("history_svc_pg", "PAST_HISTORY_PG.log")

def get_history_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    # Return RealDictCursor factory to interact like sqlite3.Row (dict access)
    return conn

async def execute_smart_query(schema_or_query):
    """
    Acts as the primary entry point for ALL historical queries.
    Routes everything to the universal_cricket_engine (Agent-to-SQL).
    """
    query_text = ""
    context = {}

    if isinstance(schema_or_query, dict):
        intent = schema_or_query.get("intent", "past_query")
        context = schema_or_query.get("entities", {})
        
        if intent == "player_stats":
             query_text = f"Stats for {context.get('player')} in {context.get('season') or 'all seasons'}"
        elif intent == "match_info":
             query_text = f"Match result {context.get('team_a')} vs {context.get('team_b')} {context.get('season') or ''}"
        elif "user_query" in schema_or_query:
            query_text = schema_or_query["user_query"]
        else:
             query_text = f"Find details for {intent} with {context}"
        
    else:
        query_text = str(schema_or_query)

    return await handle_universal_cricket_query(query_text, context=context)

async def get_player_past_performance(player_name, series_name=None, year=None):
    q = f"Performance of {player_name}"
    if series_name: q += f" in {series_name}"
    if year: q += f" in {year}"
    
    payload = {
        "user_query": q,
        "entities": {
            "player": player_name,
            "series": series_name, 
            "year": year
        }
    }
    res = await execute_smart_query(payload)
    return res.get("data", [])

async def get_head_to_head_history(team_a, team_b):
    q = f"History between {team_a} and {team_b}"
    payload = {
        "user_query": q,
        "entities": {
            "team": team_a,
            "opponent": team_b
        }
    }
    res = await execute_smart_query(payload)
    return res.get("data", [])

async def get_series_history_summary(series_name, year=None):
    q = f"Summary of {series_name}"
    if year: q += f" {year}"
    payload = {
        "user_query": q,
        "entities": {
            "series": series_name,
            "year": year
        }
    }
    res = await execute_smart_query(payload)
    return res.get("data", [])

async def search_historical_matches(query=None, team=None, year=None, date=None, series=None, limit=5):
    """
    Basic search listing. Used for simple listing.
    """
    conn = get_history_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = "SELECT f.* FROM fixtures f JOIN seasons s ON f.season_id = s.id WHERE 1=1"
            p = []

            if query:
                sql += " AND f.name ILIKE %s"
                p.append(f"%{query}%")
            if team:
                sql += " AND f.name ILIKE %s"
                p.append(f"%{team}%")
            if year:
                sql += " AND s.year LIKE %s"
                p.append(f"%{year}%")
            
            sql += " ORDER BY f.starting_at DESC LIMIT %s"
            p.append(limit)

            cursor.execute(sql, tuple(p))
            rows = [dict(r) for r in cursor.fetchall()]
            
            # Process JSON results
            if rows:
                _process_raw_json_results(rows)
                for r in rows:
                    if "raw_json" in r:
                        del r["raw_json"]
            
            return rows
    finally:
        conn.close()

async def get_historical_match_details(match_query, year=None):
    q = f"Details of match {match_query}"
    if year: q += f" in {year}"
    payload = {
        "user_query": q,
        "entities": {
            "year": year,
            "match_query": match_query
        }
    }
    res = await execute_smart_query(payload)
    return res.get("data", [])

async def get_season_leaders(year, category="runs", series_name=None):
    q = f"Top {category} in {series_name} {year}"
    payload = {
        "user_query": q,
        "entities": {
            "year": year,
            "series": series_name,
            "category": category
        }
    }
    res = await execute_smart_query(payload)
    return res.get("data", [])

async def get_season_records(year, user_query):
    payload = {
        "user_query": f"{user_query} in {year}",
        "entities": {"year": year}
    }
    res = await execute_smart_query(payload)
    return res.get("data", [])

async def get_season_match_stats(year, type="highest", series_name=None):
    q = f"{type} match stats in {series_name} {year}"
    payload = {
        "user_query": q,
        "entities": {
            "year": year,
            "series": series_name,
            "stats_type": type
        }
    }
    res = await execute_smart_query(payload)
    return {"top_innings": res.get("data", []), "top_match_aggregates": []}

async def get_team_season_summary(team_name, year):
    q = f"Summary for {team_name} in {year}"
    payload = {
        "user_query": q,
        "entities": {
            "team": team_name,
            "year": year
        }
    }
    res = await execute_smart_query(payload)
    return res.get("data", [])

async def get_all_historical_matches(search_query=None, limit=50):
    return await search_historical_matches(query=search_query, limit=limit)

async def past_db_get_standings(year, series_name=None):
    q = f"Points table for {series_name} {year}"
    payload = {
        "user_query": q,
        "entities": {
            "year": year,
            "series": series_name,
            "intent": "standings"
        }
    }
    res = await execute_smart_query(payload)
    return {"data": res.get("data", [])}

async def sync_recent_finished_matches(days_back=7, season_id=None, start_date_str=None, end_date_str=None):
    """
    Syncs finished matches from SportMonks into local PostgreSQL DB.
    """
    logger.info(f"Starting PG Data Sync (Days={days_back}, Season={season_id})...")
    conn = get_history_conn()
    
    try:
        # Check params...
        if season_id:
            params = {"filter[season_id]": season_id, "per_page": 150}
        else:
             start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
             params = {"filter[starts_between]": f"{start_date},{datetime.now().strftime('%Y-%m-%d')}", "per_page": 150}

        params["include"] = "localteam,visitorteam,venue,batting.batsman,bowling.bowler,runs,scoreboards,manofmatch"
        params["sort"] = "-starting_at"

        res = await sportmonks_cric("/fixtures", params, use_cache=False)
        if not res.get("ok"):
            logger.error(f"Sync Failed: {res.get('error')}")
            return {"status": "error", "message": res.get("error")}

        fixtures = res.get("data", [])
        logger.info(f"Fetched {len(fixtures)} matches to sync.")
        
        count = 0
        with conn.cursor() as cursor:
            for f in fixtures:
                try:
                    f_id = f.get("id")
                    s_id = f.get("season_id")
                    local = f.get("localteam", {}).get("name") or "Team A"
                    visitor = f.get("visitorteam", {}).get("name") or "Team B"
                    name = f"{local} vs {visitor}"
                    start_at = f.get("starting_at")
                    status = f.get("status")
                    venue_id = f.get("venue_id")
                    winner_id = f.get("winner_team_id")
                    
                    raw_data = {
                        "batting": f.get("batting", []),
                        "bowling": f.get("bowling", []),
                        "scoreboards": f.get("scoreboards", []),
                        "manofmatch": f.get("manofmatch", {}),
                        "toss_won_team_id": f.get("toss_won_team_id"),
                        "winner_team_id": winner_id,
                        "localteam": f.get("localteam"),
                        "visitorteam": f.get("visitorteam"),
                        "venue": f.get("venue"),
                        "runs": f.get("runs", [])
                    }
                    
                    # Convert dict to JSON string for Postgres JSONB
                    raw_json_str = json.dumps(raw_data)
                    
                    cursor.execute("""
                        INSERT INTO fixtures (id, season_id, name, starting_at, status, venue_id, winner_team_id, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(id) DO UPDATE SET 
                            status=excluded.status, 
                            winner_team_id=excluded.winner_team_id,
                            raw_json=excluded.raw_json,
                            starting_at=excluded.starting_at,
                            name=excluded.name
                    """, (f_id, s_id, name, start_at, status, venue_id, winner_id, raw_json_str))
                    
                    count += 1
                except Exception as e:
                    logger.error(f"Error syncing match {f.get('id')}: {e}")
            
            conn.commit()
    finally:
        conn.close()
    
    logger.info(f"Sync Complete. Updated {count} matches.")
    return {"status": "success", "updated": count}
