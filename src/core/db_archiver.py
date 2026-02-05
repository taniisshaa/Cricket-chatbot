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
        cursor.execute("PRAGMA table_info(fixtures)")
        fixture_cols = {row['name'] for row in cursor.fetchall()}
        match_id = f.get("id")
        for t_key in ["localteam", "visitorteam"]:
            t = f.get(t_key)
            if t:
                cursor.execute("INSERT OR IGNORE INTO teams (id, name, code, country_id, image_path) VALUES (?, ?, ?, ?, ?)",
                               (t.get("id"), t.get("name"), t.get("code"), t.get("country_id"), t.get("image_path")))
        v = f.get("venue")
        if v:
            cursor.execute("INSERT OR IGNORE INTO venues (id, name, city, capacity, image_path) VALUES (?, ?, ?, ?, ?)",
                           (v.get("id"), v.get("name"), v.get("city"), v.get("capacity"), v.get("image_path")))
        for b in f.get("batting", []):
            p = b.get("batsman")
            if p: cursor.execute("INSERT OR IGNORE INTO players (id, fullname, image_path, country_id) VALUES (?, ?, ?, ?)",
                                 (p.get("id"), p.get("fullname"), p.get("image_path"), p.get("country_id")))
        for b in f.get("bowling", []):
            p = b.get("bowler")
            if p: cursor.execute("INSERT OR IGNORE INTO players (id, fullname, image_path, country_id) VALUES (?, ?, ?, ?)",
                                 (p.get("id"), p.get("fullname"), p.get("image_path"), p.get("country_id")))
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
        valid_keys = [k for k in insert_data.keys() if k in fixture_cols]
        cols_str = ",".join(valid_keys)
        placeholders = ",".join(["?"] * len(valid_keys))
        values = [insert_data[k] for k in valid_keys]
        sql = f"INSERT OR REPLACE INTO fixtures ({cols_str}) VALUES ({placeholders})"
        cursor.execute(sql, values)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Insert Error for {f.get('id')}: {e}")