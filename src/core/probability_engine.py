import sqlite3
import os
import json
from src.utils.utils_core import get_logger
logger = get_logger("prob_engine")
DB_PATH = os.path.join("data", "full_raw_history.db")
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
def resolve_team(name):
    """Fuzzy match team name to ID"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        search = f"%{name}%"
        cursor.execute("SELECT id, name FROM teams WHERE name LIKE ? LIMIT 1", (search,))
        res = cursor.fetchone()
        conn.close()
        return res if res else None
    except: return None
def get_head_to_head_stats(team_a_id, team_b_id):
    """Get last 10 matches result between two teams"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT winner_team_id, starting_at
            FROM fixtures
            WHERE
                (localteam_id = ? AND visitorteam_id = ?)
                OR
                (localteam_id = ? AND visitorteam_id = ?)
            ORDER BY starting_at DESC
            LIMIT 10
        """, (team_a_id, team_b_id, team_b_id, team_a_id))
        matches = cursor.fetchall()
        conn.close()
        wins_a = 0
        wins_b = 0
        total = 0
        for m in matches:
            if m["winner_team_id"] == team_a_id: wins_a += 1
            elif m["winner_team_id"] == team_b_id: wins_b += 1
            total += 1
        return {"total": total, "wins_a": wins_a, "wins_b": wins_b}
    except Exception as e:
        logger.error(f"H2H Error: {e}")
        return {"total": 0, "wins_a": 0, "wins_b": 0}
def get_venue_win_rate(team_id, venue_name):
    """Calculate win rate of a team at a specific venue"""
    if not venue_name: return 0.0
    try:
        conn = get_db()
        cursor = conn.cursor()
        v_search = f"%{venue_name}%"
        cursor.execute("SELECT id FROM venues WHERE name LIKE ? OR city LIKE ? LIMIT 1", (v_search, v_search))
        v_res = cursor.fetchone()
        if not v_res:
            conn.close()
            return 0.0
        venue_id = v_res["id"]
        cursor.execute("""
            SELECT count(*) as total,
                   sum(case when winner_team_id = ? then 1 else 0 end) as wins
            FROM fixtures
            WHERE venue_id = ? AND (localteam_id = ? OR visitorteam_id = ?)
        """, (team_id, venue_id, team_id, team_id))
        stats = cursor.fetchone()
        conn.close()
        if not stats or stats["total"] == 0: return 0.0
        return round((stats["wins"] / stats["total"]) * 100, 2)
    except Exception as e:
        logger.error(f"Venue Stats Error: {e}")
        return 0.0
def generate_prediction(team_a_name, team_b_name, venue_name=None):
    logger.info(f"Generating Prediction: {team_a_name} vs {team_b_name} @ {venue_name}")
    t1 = resolve_team(team_a_name)
    t2 = resolve_team(team_b_name)
    if not t1 or not t2:
        return {"error": "Could not identify one or both teams in database."}
    t1_id, t1_real = t1["id"], t1["name"]
    t2_id, t2_real = t2["id"], t2["name"]
    h2h = get_head_to_head_stats(t1_id, t2_id)
    v1_rate = get_venue_win_rate(t1_id, venue_name)
    v2_rate = get_venue_win_rate(t2_id, venue_name)
    score_a = 50.0
    if h2h["total"] > 0:
        h2h_win_rate_a = (h2h["wins_a"] / h2h["total"]) * 100
        swing = (h2h_win_rate_a - 50) * 0.4 # Weight 0.4
        score_a += swing
    if venue_name and (v1_rate > 0 or v2_rate > 0):
        if v1_rate > v2_rate:
            score_a += 5
        elif v2_rate > v1_rate:
            score_a -= 5
    score_a = max(10, min(90, score_a))
    score_b = 100 - score_a
    winner = t1_real if score_a > score_b else t2_real
    prob = max(score_a, score_b)
    narrative = []
    narrative.append(f"Based on historical data from {h2h['total']} head-to-head matches.")
    if h2h["total"] > 0:
        narrative.append(f"{t1_real} won {h2h['wins_a']}, {t2_real} won {h2h['wins_b']}.")
    if venue_name:
        narrative.append(f"At {venue_name}, {t1_real} has {v1_rate}% win record, {t2_real} has {v2_rate}%.")
    return {
        "ok": True,
        "prediction": {
            "winner": winner,
            "probability": f"{prob:.1f}%",
            "team_a": t1_real,
            "team_b": t2_real,
            "team_a_prob": f"{score_a:.1f}",
            "team_b_prob": f"{score_b:.1f}",
            "analysis": " ".join(narrative)
        }
    }