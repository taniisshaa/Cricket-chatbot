import os
import sqlite3
from src.utils.utils_core import get_logger
from src.core.universal_cricket_engine import handle_universal_cricket_query
import json
from datetime import datetime, timedelta
from src.environment.backend_core import sportmonks_cric
logger = get_logger("history_svc", "PAST_HISTORY.log")
DB_PATH = os.path.join("data", "full_raw_history.db")
def get_history_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
async def execute_smart_query(schema_or_query):
    """
    Acts as the primary entry point for ALL historical queries.
    Routes everything to the universal_cricket_engine (Agent-to-SQL).
    """
    logger.info("Executing Schema-Aware Smart Query via Universal Engine...")
    query_text = ""
    if isinstance(schema_or_query, dict):
        intent = schema_or_query.get("intent", "past_query")
        entities = schema_or_query.get("entities", {})
        query_text = f"Find details for {intent} with {entities}"
        if "user_query" in schema_or_query:
            query_text = schema_or_query["user_query"]
    else:
        query_text = str(schema_or_query)
    return await handle_universal_cricket_query(query_text)
async def get_player_past_performance(player_name, series_name=None, year=None):
    q = f"Performance of {player_name}"
    if series_name: q += f" in {series_name}"
    if year: q += f"in {year}"
    res = await execute_smart_query(q)
    return res.get("data", [])
async def get_head_to_head_history(team_a, team_b):
    q = f"History between {team_a} and {team_b}"
    res = await execute_smart_query(q)
    return res.get("data", [])
async def get_series_history_summary(series_name, year=None):
    q = f"Summary of {series_name}"
    if year: q += f" {year}"
    res = await execute_smart_query(q)
    return res.get("data", [])
async def search_historical_matches(query=None, team=None, year=None, date=None, series=None, limit=5):
    """
    Basic search listing. Can be SQL-based or LLM-based.
    Let's keep it simple SQL for speed/listing if possible,
    BUT user wants 'accurate answers from database'.
    Let's use the DB directly for simple filtering to be fast.
    """
    conn = get_history_conn()
    cursor = conn.cursor()
    sql = "SELECT f.* FROM fixtures f JOIN seasons s ON f.season_id = s.id WHERE 1=1"
    p = []
    if query:
        sql += " AND f.name LIKE ?"
        p.append(f"%{query}%")
    if team:
        sql += " AND f.name LIKE ?"
        p.append(f"%{team}%")
    if year:
        sql += " AND s.year LIKE ?"
        p.append(f"%{year}%")
    sql += " ORDER BY f.starting_at DESC LIMIT ?"
    p.append(limit)
    cursor.execute(sql, p)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows
async def get_historical_match_details(match_query, year=None):
    q = f"Details of match {match_query}"
    if year: q += f" in {year}"
    res = await execute_smart_query(q)
    return res.get("data", [])
async def get_season_leaders(year, category="runs", series_name=None):
    q = f"Top {category} in {series_name} {year}"
    res = await execute_smart_query(q)
    return res.get("data", [])
async def get_season_records(year, user_query):
    return (await execute_smart_query(f"{user_query} in {year}")).get("data", [])
async def get_season_match_stats(year, type="highest", series_name=None):
    q = f"{type} match stats in {series_name} {year}"
    res = await execute_smart_query(q)
    return {"top_innings": res.get("data", []), "top_match_aggregates": []}
async def get_team_season_summary(team_name, year):
    q = f"Summary for {team_name} in {year}"
    res = await execute_smart_query(q)
    return res.get("data", [])
async def get_all_historical_matches(search_query=None, limit=50):
    return await search_historical_matches(query=search_query, limit=limit)
async def past_db_get_standings(year, series_name=None):
    q = f"Points table for {series_name} {year}"
    res = await execute_smart_query(q)
    return {"data": res.get("data", [])}
async def sync_recent_finished_matches(days_back=7, season_id=None, start_date_str=None, end_date_str=None):
    """
    Syncs finished matches from SportMonks into local SQLite DB.
    Allows re-populating historical data dynamically.
    """
    logger.info(f"Starting Data Sync (Days={days_back}, Season={season_id}, Range={start_date_str}-{end_date_str})...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    params = {
        "include": "localteam,visitorteam,venue,batting.batsman,bowling.bowler,runs,scoreboards,manofmatch",
        "sort": "-starting_at"
    }
    if season_id:
        params["filter[season_id]"] = season_id
        params["per_page"] = 150
    elif start_date_str and end_date_str:
        params["filter[starts_between]"] = f"{start_date_str},{end_date_str}"
        params["per_page"] = 150
    else:
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        params["filter[starts_between]"] = f"{start_date},{datetime.now().strftime('%Y-%m-%d')}"
    res = await sportmonks_cric("/fixtures", params, use_cache=False)
    if not res.get("ok"):
        logger.error(f"Sync Failed: {res.get('error')}")
        conn.close()
        return {"status": "error", "message": res.get("error")}
    fixtures = res.get("data", [])
    logger.info(f"Fetched {len(fixtures)} matches to sync.")
    count = 0
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
                "venue": f.get("venue")
            }
            raw_json_str = json.dumps(raw_data)
            cursor.execute("""
                INSERT INTO fixtures (id, season_id, name, starting_at, status, venue_id, winner_team_id, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    status=excluded.status,
                    winner_team_id=excluded.winner_team_id,
                    raw_json=excluded.raw_json,
                    starting_at=excluded.starting_at
            """, (f_id, s_id, name, start_at, status, venue_id, winner_id, raw_json_str))
            count += 1
        except Exception as e:
            logger.error(f"Error syncing match {f.get('id')}: {e}")
    conn.commit()
    conn.close()
    logger.info(f"Sync Complete. Updated {count} matches.")
    return {"status": "success", "updated": count}