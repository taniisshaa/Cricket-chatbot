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
    return conn

def _upsert_team(cursor, team):
    if not team or not team.get('id'): return
    cursor.execute("""
        INSERT INTO teams (id, name, code, raw_json)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET name=excluded.name, code=excluded.code, raw_json=excluded.raw_json
    """, (team['id'], team.get('name'), team.get('code'), json.dumps(team)))

def _upsert_player(cursor, player):
    if not player or not player.get('id'): return
    cursor.execute("""
        INSERT INTO players (id, fullname, dateofbirth, batting_style, bowling_style, country_id, raw_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET 
            fullname=excluded.fullname, dateofbirth=excluded.dateofbirth, 
            batting_style=excluded.batting_style, bowling_style=excluded.bowling_style,
            country_id=excluded.country_id, raw_json=excluded.raw_json
    """, (player['id'], player.get('fullname'), player.get('dateofbirth'), 
          player.get('batting_style'), player.get('bowling_style'), 
          player.get('country_id'), json.dumps(player)))

def _upsert_venue(cursor, venue):
    if not venue or not venue.get('id'): return
    cursor.execute("""
        INSERT INTO venues (id, name, city, capacity, raw_json)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET name=excluded.name, city=excluded.city, capacity=excluded.capacity, raw_json=excluded.raw_json
    """, (venue['id'], venue.get('name'), venue.get('city'), venue.get('capacity'), json.dumps(venue)))

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
    Syncs ONLY FINISHED matches from SportMonks into local PostgreSQL DB.
    Scheduled/Upcoming matches are NOT stored.
    """
    logger.info(f"🔄 Starting Smart Sync (Days={days_back}, Season={season_id})...")
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
            logger.error(f"❌ Sync Failed: {res.get('error')}")
            return {"status": "error", "message": res.get("error")}

        all_fixtures = res.get("data", [])
        logger.info(f"📥 Fetched {len(all_fixtures)} total matches from API")
        
        # FILTER: Only process FINISHED matches
        finished_statuses = ["Finished", "Completed", "FINISHED", "COMPLETED"]
        fixtures = [f for f in all_fixtures if f.get("status") in finished_statuses or "won" in str(f.get("note", "")).lower()]
        
        skipped = len(all_fixtures) - len(fixtures)
        logger.info(f"✅ Processing {len(fixtures)} FINISHED matches (Skipped {skipped} scheduled/in-progress)")
        
        count = 0
        with conn.cursor() as cursor:
            for f in fixtures:
                try:
                    f_id = f.get("id")
                    s_id = f.get("season_id")
                    local_team = f.get("localteam") or {}
                    visitor_team = f.get("visitorteam") or {}
                    local = local_team.get("name") or "Team A"
                    visitor = visitor_team.get("name") or "Team B"
                    name = f"{local} vs {visitor}"
                    start_at = f.get("starting_at")
                    status = f.get("status")
                    venue_id = f.get("venue_id")
                    winner_id = f.get("winner_team_id")
                    toss_id = f.get("toss_won_team_id")
                    mom_id = f.get("manofmatch", {}).get("id") if f.get("manofmatch") else None
                    
                    # 1. Upsert Entities
                    _upsert_team(cursor, local_team)
                    _upsert_team(cursor, visitor_team)
                    _upsert_venue(cursor, f.get("venue"))
                    
                    # 2. Upsert Players from match details
                    for b in f.get("batting", []):
                        if b.get("batsman"): _upsert_player(cursor, b["batsman"])
                    for b in f.get("bowling", []):
                        if b.get("bowler"): _upsert_player(cursor, b["bowler"])
                    if f.get("manofmatch"):
                        _upsert_player(cursor, f["manofmatch"])

                    # 3. Build Raw JSON
                    raw_data = {
                        "batting": f.get("batting", []),
                        "bowling": f.get("bowling", []),
                        "scoreboards": f.get("scoreboards", []),
                        "manofmatch": f.get("manofmatch", {}),
                        "toss_won_team_id": toss_id,
                        "winner_team_id": winner_id,
                        "localteam": local_team,
                        "visitorteam": visitor_team,
                        "venue": f.get("venue"),
                        "runs": f.get("runs", []),
                        "note": f.get("note", "")
                    }
                    raw_json_str = json.dumps(raw_data)
                    
                    # 4. Insert/Update Fixture (ONLY FINISHED)
                    cursor.execute("""
                        INSERT INTO fixtures (id, season_id, name, starting_at, status, venue_id, winner_team_id, toss_won_team_id, man_of_match_id, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(id) DO UPDATE SET 
                            status=excluded.status, 
                            winner_team_id=excluded.winner_team_id,
                            toss_won_team_id=excluded.toss_won_team_id,
                            man_of_match_id=excluded.man_of_match_id,
                            raw_json=excluded.raw_json,
                            starting_at=excluded.starting_at,
                            name=excluded.name,
                            venue_id=excluded.venue_id
                    """, (f_id, s_id, name, start_at, status, venue_id, winner_id, toss_id, mom_id, raw_json_str))
                    
                    count += 1
                except Exception as e:
                    logger.error(f"❌ Error syncing match {f.get('id')}: {e}")
            
            conn.commit()
    finally:
        conn.close()
    
    logger.info(f"✅ Sync Complete. Stored {count} FINISHED matches (Skipped {skipped} scheduled)")
    return {"status": "success", "updated": count, "skipped": skipped}

async def sync_specific_match(match_id):
    """
    Syncs one specific match ID immediately into PostgreSQL.
    """
    logger.info(f"⚡ FAST SYNC: Fetching Match {match_id} into PostgreSQL...")
    conn = get_history_conn()
    try:
        params = {
            "include": "localteam,visitorteam,venue,batting.batsman,bowling.bowler,runs,scoreboards,manofmatch",
            "per_page": 1
        }
        res = await sportmonks_cric(f"/fixtures/{match_id}", params, use_cache=False)
        if not res.get("ok"):
             return {"status": "error", "message": "Match not found in API"}
        
        f = res.get("data", {})
        if not f: return {"status": "error", "message": "Empty data"}
        
        with conn.cursor() as cursor:
            f_id = f.get("id")
            s_id = f.get("season_id")
            local_team = f.get("localteam") or {}
            visitor_team = f.get("visitorteam") or {}
            local = local_team.get("name") or "Team A"
            visitor = visitor_team.get("name") or "Team B"
            name = f"{local} vs {visitor}"
            start_at = f.get("starting_at")
            status = f.get("status")
            venue_id = f.get("venue_id")
            winner_id = f.get("winner_team_id")
            toss_id = f.get("toss_won_team_id")
            mom_id = f.get("manofmatch", {}).get("id") if f.get("manofmatch") else None
            
            # 1. Upsert Entities
            _upsert_team(cursor, local_team)
            _upsert_team(cursor, visitor_team)
            _upsert_venue(cursor, f.get("venue"))
            
            for b in f.get("batting", []):
                if b.get("batsman"): _upsert_player(cursor, b["batsman"])
            for b in f.get("bowling", []):
                if b.get("bowler"): _upsert_player(cursor, b["bowler"])
            if f.get("manofmatch"):
                _upsert_player(cursor, f["manofmatch"])

            # 2. Build Raw JSON
            raw_data = {
                "batting": f.get("batting", []),
                "bowling": f.get("bowling", []),
                "scoreboards": f.get("scoreboards", []),
                "manofmatch": f.get("manofmatch", {}),
                "toss_won_team_id": toss_id,
                "winner_team_id": winner_id,
                "localteam": local_team,
                "visitorteam": visitor_team,
                "venue": f.get("venue"),
                "runs": f.get("runs", [])
            }
            
            # 3. Insert Fixture
            cursor.execute("""
                INSERT INTO fixtures (id, season_id, name, starting_at, status, venue_id, winner_team_id, toss_won_team_id, man_of_match_id, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET 
                    status=excluded.status, 
                    winner_team_id=excluded.winner_team_id,
                    toss_won_team_id=excluded.toss_won_team_id,
                    man_of_match_id=excluded.man_of_match_id,
                    raw_json=excluded.raw_json,
                    starting_at=excluded.starting_at,
                    name=excluded.name,
                    venue_id=excluded.venue_id
            """, (f_id, s_id, name, start_at, status, venue_id, winner_id, toss_id, mom_id, json.dumps(raw_data)))
            conn.commit()
            logger.info(f"✅ FAST SYNC SUCCESS: Match {match_id} is now in PostgreSQL with full parameters.")
            return {"status": "success", "match": name}
    except Exception as e:
        logger.error(f"Fast Sync Failed for {match_id}: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()
