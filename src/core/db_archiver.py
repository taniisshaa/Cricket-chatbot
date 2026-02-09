import sqlite3
import json
import os
import asyncio
from src.environment.backend_core import sportmonks_cric, getMatchScorecard
from src.utils.utils_core import get_logger
logger = get_logger("db_archiver", "archiver.log")
DB_PATH = os.path.join("data", "full_raw_history.db")
_ARCHIVED_CACHE = set()
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
async def is_match_archived(match_id):
    if match_id in _ARCHIVED_CACHE:
        return True
    loop = asyncio.get_running_loop()
    def _check():
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM fixtures WHERE id = ?", (match_id,))
            exists = cursor.fetchone() is not None
            conn.close()
            return exists
        except: return False
    exists = await loop.run_in_executor(None, _check)
    if exists:
        _ARCHIVED_CACHE.add(match_id)
    return exists
async def archive_match(match_id):
    """
    Fetches full match details from API and saves to SQLite DB.
    """
    if await is_match_archived(match_id):
        return
    logger.info(f"Archiving Finished Match: {match_id}")
    includes = "localteam,visitorteam,venue,runs,scoreboards,batting.batsman,bowling.bowler,manofmatch"
    res = await sportmonks_cric(f"/fixtures/{match_id}", params={"include": includes})
    if not res.get("ok"):
        logger.error(f"Failed to fetch match {match_id} for archiving: {res.get('error')}")
        return
    f = res.get("data")
    if not f: return
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _save_to_db_sync, f)
    _ARCHIVED_CACHE.add(match_id)
    logger.info(f"Successfully Archived Match: {match_id}")
def _save_to_db_sync(f):
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Helper to get columns for a table
        def get_cols(table_name):
            cursor.execute(f"PRAGMA table_info({table_name})")
            return {row['name'] for row in cursor.fetchall()}

        # 1. Archive Teams
        team_cols = get_cols("teams")
        for t_key in ["localteam", "visitorteam"]:
            t = f.get(t_key)
            if t:
                data = {
                    "id": t.get("id"),
                    "name": t.get("name"),
                    "code": t.get("code"),
                    "country_id": t.get("country_id"),
                    "image_path": t.get("image_path")
                }
                valid_data = {k: v for k, v in data.items() if k in team_cols}
                cols = ",".join(valid_data.keys())
                placeholders = ",".join(["?"] * len(valid_data))
                cursor.execute(f"INSERT OR IGNORE INTO teams ({cols}) VALUES ({placeholders})", list(valid_data.values()))

        # 2. Archive Venues
        venue_cols = get_cols("venues")
        v = f.get("venue")
        if v:
            data = {
                "id": v.get("id"),
                "name": v.get("name"),
                "city": v.get("city"),
                "capacity": v.get("capacity"),
                "image_path": v.get("image_path")
            }
            valid_data = {k: v for k, v in data.items() if k in venue_cols}
            cols = ",".join(valid_data.keys())
            placeholders = ",".join(["?"] * len(valid_data))
            cursor.execute(f"INSERT OR IGNORE INTO venues ({cols}) VALUES ({placeholders})", list(valid_data.values()))

        # 3. Archive Players (from batting/bowling)
        player_cols = get_cols("players")
        players_to_save = []
        for b in f.get("batting", []):
            if b.get("batsman"): players_to_save.append(b.get("batsman"))
        for b in f.get("bowling", []):
            if b.get("bowler"): players_to_save.append(b.get("bowler"))
            
        for p in players_to_save:
            data = {
                "id": p.get("id"),
                "fullname": p.get("fullname"),
                "image_path": p.get("image_path"),
                "country_id": p.get("country_id")
            }
            valid_data = {k: v for k, v in data.items() if k in player_cols}
            cols = ",".join(valid_data.keys())
            placeholders = ",".join(["?"] * len(valid_data))
            cursor.execute(f"INSERT OR IGNORE INTO players ({cols}) VALUES ({placeholders})", list(valid_data.values()))

        # 4. Archive Fixture
        fixture_cols = get_cols("fixtures")
        match_id = f.get("id")
        insert_data = {
            "id": match_id,
            "season_id": f.get("season_id"),
            "name": f"{f.get('localteam',{}).get('name', 'TBA')} vs {f.get('visitorteam',{}).get('name', 'TBA')}",
            "starting_at": f.get("starting_at"),
            "status": f.get("status"),
            "venue_id": f.get("venue_id"),
            "winner_team_id": f.get("winner_team_id"),
            "localteam_id": f.get("localteam_id"),
            "visitorteam_id": f.get("visitorteam_id"),
            "raw_json": json.dumps(f)
        }
        valid_data = {k: v for k, v in insert_data.items() if k in fixture_cols}
        cols_str = ",".join(valid_data.keys())
        placeholders = ",".join(["?"] * len(valid_data))
        sql = f"INSERT OR REPLACE INTO fixtures ({cols_str}) VALUES ({placeholders})"
        cursor.execute(sql, list(valid_data.values()))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Insert Error for {f.get('id')}: {e}")
