import asyncio
import sqlite3
import os
from app.utils_core import get_logger

logger = get_logger("squad_svc")

PAST_DB_PATH = os.path.join("data", "past_ipl_data.db")

def past_db_get_connection():
    if not os.path.exists(PAST_DB_PATH):
        logger.warning(f"DB not found at {PAST_DB_PATH}")
    return sqlite3.connect(PAST_DB_PATH)

def normalize_team_name(team_input: str) -> str:
    """Expand team abbreviations using DB."""
    try:
        conn = past_db_get_connection()
        cursor = conn.execute("SELECT DISTINCT team_name FROM squads")
        all_teams = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not all_teams: return team_input
        
        input_lower = team_input.lower().strip()
        
        for team in all_teams:
            if team.lower() == input_lower: return team
            
        for team in all_teams:
            if input_lower in team.lower(): return team
            
        for team in all_teams:
            words = team.split()
            initials = ''.join([w[0] for w in words if w]).lower()
            if input_lower == initials: return team
            
        for team in all_teams:
            if input_lower in team.lower().split(): return team
            
        return team_input
    except Exception as e:
        logger.error(f"Error normalizing team {team_input}: {e}")
        return team_input

async def get_team_squad(team_name: str, series_name: str = None, year: int = None):
    try:
        normalized_name = normalize_team_name(team_name)
        conn = past_db_get_connection()
        conn.row_factory = sqlite3.Row
        
        query = """
            SELECT DISTINCT s.player_name, s.is_captain, s.is_keeper
            FROM squads s
            JOIN matches m ON s.match_id = m.id
            WHERE s.team_name LIKE ?
        """
        params = [f"%{normalized_name}%"]
        
        if year:
            query += " AND m.date LIKE ?"
            params.append(f"{year}%")
        if series_name:
            query += " AND (m.name LIKE ? OR m.series_id LIKE ?)"
            params.extend([f"%{series_name}%", f"%{series_name}%"])
            
        cursor = conn.execute(query, params)
        players = cursor.fetchall()
        conn.close()
        
        if not players: return None
        
        squad_list = []
        captain, keeper = None, None
        
        for p in players:
            info = {
                "name": p["player_name"],
                "captain": bool(p["is_captain"]),
                "keeper": bool(p["is_keeper"])
            }
            squad_list.append(info)
            if info["captain"]: captain = info["name"]
            if info["keeper"]: keeper = info["name"]
            
        return {
            "team": team_name,
            "total_players": len(squad_list),
            "captain": captain,
            "keeper": keeper,
            "players": squad_list
        }
    except Exception as e:
        logger.error(f"Error fetching squad: {e}")
        return None

async def compare_team_squads(team1: str, team2: str, series_name: str = None, year: int = None):
    squad1, squad2 = await asyncio.gather(
        get_team_squad(team1, series_name, year),
        get_team_squad(team2, series_name, year)
    )
    
    if not squad1 or not squad2:
        return {"error": "Squad data missing for one or both teams"}
        
    return {
        "team1": {"name": team1, "captain": squad1["captain"], "players": [p["name"] for p in squad1["players"]]},
        "team2": {"name": team2, "captain": squad2["captain"], "players": [p["name"] for p in squad2["players"]]},
        "summary": {"diff": len(squad1["players"]) - len(squad2["players"])}
    }
