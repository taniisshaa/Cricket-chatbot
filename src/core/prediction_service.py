import numpy as np
import json
import sqlite3
import os
from src.utils.utils_core import get_logger
logger = get_logger("prediction_svc", "PREDICTION.log")
DB_PATH = os.path.join("data", "full_raw_history.db")
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
class PredictionService:
    def __init__(self):
        pass
    async def generate_match_prediction(self, team_a, team_b, date_str=None, venue=None, match_format="T20"):
        report = {
            "prediction_type": "pre_match",
            "teams": [team_a, team_b],
            "date": date_str,
            "venue": venue,
            "win_probability": {},
            "key_factors": [],
            "score_prediction": {},
            "player_watch": [],
            "reasoning_narrative": ""
        }
        logger.info(f"Predicting match: {team_a} vs {team_b} at {venue} ({date_str})")
        h2h_stats = self._analyze_h2h(team_a, team_b)
        h2h_prob_a = h2h_stats.get("win_rate_a", 50.0)
        if h2h_stats["total"] > 0:
            report["key_factors"].append(f"Head-to-Head ({h2h_stats['total']} matches): {team_a} won {h2h_stats['wins_a']}, {team_b} won {h2h_stats['wins_b']}.")
        form_a = self._analyze_form(team_a)
        form_b = self._analyze_form(team_b)
        if form_a["win_rate"] > form_b["win_rate"] + 20:
            report["key_factors"].append(f"Form: {team_a} is in better recent form ({int(form_a['win_rate'])}% wins vs {int(form_b['win_rate'])}%).")
        elif form_b["win_rate"] > form_a["win_rate"] + 20:
             report["key_factors"].append(f"Form: {team_b} is in better recent form ({int(form_b['win_rate'])}% wins vs {int(form_a['win_rate'])}%).")
        venue_stats = self._analyze_venue(venue, team_a, team_b)
        venue_prob_a = 50.0 # Default
        if venue_stats["pitch_type"] == "Batting":
            report["reasoning_narrative"] += f" {venue} is a high-scoring ground. "
        elif venue_stats["pitch_type"] == "Bowling":
            report["reasoning_narrative"] += f" {venue} aids bowlers. low scoring exp."
        if venue_stats["matches"] > 0:
            t1_wins = venue_stats.get("team_a_wins", 0)
            t2_wins = venue_stats.get("team_b_wins", 0)
            if t1_wins > t2_wins:
                 venue_prob_a += 10
                 report["key_factors"].append(f"Venue: {team_a} likes playing at {venue} ({t1_wins} wins).")
            elif t2_wins > t1_wins:
                 venue_prob_a -= 10
                 report["key_factors"].append(f"Venue: {team_b} likes playing at {venue} ({t2_wins} wins).")
        final_prob_a = 50.0
        final_prob_a += (h2h_prob_a - 50) * 0.4
        form_diff = form_a["win_rate"] - form_b["win_rate"]
        final_prob_a += (form_diff * 0.3)
        final_prob_a += (venue_prob_a - 50) * 0.3
        final_prob_a = max(15.0, min(85.0, final_prob_a))
        report["win_probability"][team_a] = round(final_prob_a, 1)
        report["win_probability"][team_b] = round(100 - final_prob_a, 1)
        report["fantasy_picks"] = await self._get_fantasy_picks_real(team_a, team_b)
        return report
    def _analyze_h2h(self, t1, t2):
        try:
            conn = get_db()
            cursor = conn.cursor()
            sql = """
                SELECT winner_team_id, raw_json
                FROM fixtures
                WHERE name LIKE ? AND name LIKE ? AND status IN ('Finished', 'Completed', 'Ended')
                ORDER BY starting_at DESC LIMIT 10
            """
            cursor.execute(sql, (f"%{t1}%", f"%{t2}%"))
            rows = cursor.fetchall()
            conn.close()
            wins_a = 0
            wins_b = 0
            total = 0
            for r in rows:
                raw = json.loads(r["raw_json"])
                w_id = r["winner_team_id"]
                if not w_id:
                     continue
                lt = raw.get("localteam", {})
                vt = raw.get("visitorteam", {})
                w_name = ""
                if str(lt.get("id")) == str(w_id): w_name = lt.get("name")
                elif str(vt.get("id")) == str(w_id): w_name = vt.get("name")
                if t1.lower() in w_name.lower(): wins_a += 1
                elif t2.lower() in w_name.lower(): wins_b += 1
                total += 1
            win_rate_a = 50.0
            if total > 0:
                win_rate_a = (wins_a / total) * 100
            return {"total": total, "wins_a": wins_a, "wins_b": wins_b, "win_rate_a": win_rate_a}
        except Exception as e:
            logger.error(f"H2H Error: {e}")
            return {"total": 0, "wins_a": 0, "wins_b": 0, "win_rate_a": 50.0}
    def _analyze_form(self, team):
        try:
            conn = get_db()
            cursor = conn.cursor()
            sql = """
                SELECT winner_team_id, raw_json
                FROM fixtures
                WHERE (name LIKE ?) AND status IN ('Finished', 'Completed', 'Ended')
                ORDER BY starting_at DESC LIMIT 5
            """
            cursor.execute(sql, (f"%{team}%",))
            rows = cursor.fetchall()
            conn.close()
            wins = 0
            total = 0
            for r in rows:
                raw = json.loads(r["raw_json"])
                w_id = r["winner_team_id"]
                if not w_id: continue
                lt = raw.get("localteam", {})
                vt = raw.get("visitorteam", {})
                w_name = ""
                if str(lt.get("id")) == str(w_id): w_name = lt.get("name")
                elif str(vt.get("id")) == str(w_id): w_name = vt.get("name")
                if team.lower() in w_name.lower(): wins += 1
                total += 1
            rate = 50.0
            if total > 0: rate = (wins / total) * 100
            return {"win_rate": rate}
        except:
             return {"win_rate": 50.0}
    def _analyze_venue(self, venue, t1, t2):
        if not venue: return {"matches": 0, "pitch_type": "Balanced"}
        try:
            conn = get_db()
            cursor = conn.cursor()
            v_id = None
            cursor.execute("SELECT id FROM venues WHERE name LIKE ? LIMIT 1", (f"%{venue}%",))
            row = cursor.fetchone()
            if row: v_id = row[0]
            rows = []
            if v_id:
                sql = "SELECT raw_json, winner_team_id FROM fixtures WHERE venue_id = ? AND status='Finished' ORDER BY starting_at DESC LIMIT 10"
                cursor.execute(sql, (v_id,))
                rows = cursor.fetchall()
            else:
                 pass
            conn.close()
            if not rows: return {"matches": 0, "pitch_type": "Balanced"}
            avg_runs = 0
            count = 0
            t1_wins = 0
            t2_wins = 0
            for r in rows:
                raw = json.loads(r["raw_json"])
                runs = raw.get("runs", [])
                if runs and len(runs) > 0:
                     s = runs[0].get("score", 0)
                     avg_runs += s
                     count += 1
                w_id = r["winner_team_id"]
                lt = raw.get("localteam", {})
                vt = raw.get("visitorteam", {})
                w_name = ""
                if str(lt.get("id")) == str(w_id): w_name = lt.get("name")
                elif str(vt.get("id")) == str(w_id): w_name = vt.get("name")
                if t1.lower() in w_name.lower(): t1_wins += 1
                elif t2.lower() in w_name.lower(): t2_wins += 1
            pitch = "Balanced"
            if count > 0:
                avg = avg_runs / count
                if avg > 170: pitch = "Batting"
                elif avg < 150: pitch = "Bowling"
            return {"matches": len(rows), "pitch_type": pitch, "team_a_wins": t1_wins, "team_b_wins": t2_wins}
        except:
            return {"matches": 0, "pitch_type": "Balanced"}
    async def _get_fantasy_picks_real(self, t1, t2):
        """
        Scans last 5 matches for each team and identifies top performing players.
        """
        try:
            conn = get_db()
            cursor = conn.cursor()
            
            # Fetch last 5 matches involving either team
            sql = """
                SELECT raw_json FROM fixtures 
                WHERE (name LIKE ? OR name LIKE ?) AND status IN ('Finished', 'Completed', 'Ended')
                ORDER BY starting_at DESC LIMIT 10
            """
            cursor.execute(sql, (f"%{t1}%", f"%{t2}%"))
            rows = cursor.fetchall()
            conn.close()
            
            player_stats = {}
            
            for r in rows:
                raw = json.loads(r["raw_json"])
                # Batting Points
                for b in raw.get("batting", []):
                    pid = b.get("player_id")
                    name = b.get("batsman", {}).get("fullname")
                    if not name: continue
                    
                    score = b.get("score", 0)
                    pts = score * 1.0  # Base run points
                    if score >= 50: pts += 10
                    if score >= 100: pts += 20
                    
                    if name not in player_stats: player_stats[name] = {"total": 0, "matches": 0, "role": "Batsman"}
                    player_stats[name]["total"] += pts
                    player_stats[name]["matches"] += 1
                    
                # Bowling Points
                for b in raw.get("bowling", []):
                    name = b.get("bowler", {}).get("fullname")
                    if not name: continue
                    
                    wkts = b.get("wickets", 0)
                    pts = wkts * 25  # Wicket points
                    if wkts >= 3: pts += 10
                    
                    if name not in player_stats: player_stats[name] = {"total": 0, "matches": 0, "role": "Bowler"}
                    if player_stats[name]["role"] == "Batsman": player_stats[name]["role"] = "All-Rounder"
                    player_stats[name]["total"] += pts
                    player_stats[name]["matches"] += 1 # Only count unique match if not counted in batting? Simplified here.

            # Calculate Avg
            final_picks = []
            for name, stats in player_stats.items():
                if stats["matches"] > 0:
                    avg = stats["total"] / stats["matches"]
                    final_picks.append({"player": name, "avg_pts": avg, "role": stats["role"]})
            
            # Sort Top 5
            final_picks.sort(key=lambda x: x["avg_pts"], reverse=True)
            return final_picks[:5]
            
        except Exception as e:
            logger.error(f"Fantasy Pick Error: {e}")
            return [{"player": "Star Player", "role": "Key Player"}]

    def _get_fantasy_picks_stub(self, t1, t2):
        # Redirect to real implementation
        # Since _get_fantasy_picks_real is async, we need to handle it.
        # But generate_match_prediction is async, so we can await it there.
        # Refactoring to call it directly in generate_match_prediction
        return [] 

    # Helper to call from async context
    async def get_fantasy_picks(self, t1, t2):
        return await self._get_fantasy_picks_real(t1, t2)
prediction_service = PredictionService()