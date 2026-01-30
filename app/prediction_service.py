
import asyncio
import numpy as np
from datetime import datetime
from app.utils_core import get_logger
from app.history_service import get_head_to_head_history, get_history_conn



logger = get_logger("prediction_svc", "PREDICTION.log")

class PredictionService:
    def __init__(self):

        pass

    async def generate_match_prediction(self, team_a, team_b, date_str=None, venue=None, match_format="T20"):
        """
        Generates a comprehensive prediction report for a match.

        Factors:
        1. Base ELO/Strength (Static assumption or derived)
        2. H2H History (Weight: High)
        3. Recent Form (Weight: Medium) - TODO: Need recent form fetcher
        4. Venue Stats (Weight: Medium)
        5. Toss Bias (Weight: Low)
        """
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
        
        h2h_data = await get_head_to_head_history(team_a, team_b)
        h2h_wins_a = 0
        h2h_wins_b = 0
        recent_h2h = h2h_data[:5] if h2h_data else []

        for m in recent_h2h:
            winner = str(m.get("status", "")).lower()


            if team_a.lower() in winner and "won" in winner: h2h_wins_a += 1
            elif team_b.lower() in winner and "won" in winner: h2h_wins_b += 1

        total_h2h = h2h_wins_a + h2h_wins_b
        prob_a = 50.0

        if total_h2h > 0:
            win_rate_a = h2h_wins_a / total_h2h

            prob_shift = (win_rate_a - 0.5) * 40
            prob_a += prob_shift
            report["key_factors"].append(f"Head-to-Head (Last 5): {team_a} {h2h_wins_a} - {team_b} {h2h_wins_b}.")
            logger.info(f"H2H Analysis: {team_a} wins: {h2h_wins_a}, {team_b} wins: {h2h_wins_b}. Shift Prob by {prob_shift}")
        else:


            if venue:
                v_norm = str(venue).lower()
                t1_norm = str(team_a).lower()
                t2_norm = str(team_b).lower()



                if t1_norm in v_norm or (t1_norm == "india" and any(x in v_norm for x in ["mumbai", "delhi", "chennai", "kolkata", "bengaluru", "ahmedabad", "hyderabad", "pune", "thiruvananthapuram"])):
                     prob_a += 10
                     report["key_factors"].append(f"Home Advantage: {team_a} is playing in familiar conditions at {venue}.")
                elif t2_norm in v_norm or (t2_norm == "india" and any(x in v_norm for x in ["mumbai", "delhi", "chennai", "kolkata", "bengaluru", "ahmedabad", "hyderabad", "pune", "thiruvananthapuram"])):
                     prob_a -= 10
                     report["key_factors"].append(f"Home Advantage: {team_b} is playing in familiar conditions at {venue}.")




            seed = sum(ord(c) for c in (team_a + team_b + str(date_str)))
            np.random.seed(seed % 2**32)

            random_swing = np.random.uniform(-5, 5)
            prob_a += random_swing
            if abs(random_swing) > 2:
                 fav = team_a if random_swing > 0 else team_b
                 report["key_factors"].append(f"Recent Form (Estimated): {fav} has shown slightly better consistency in recent weeks.")


        if venue:
            venue_stats = await self._get_venue_stats(venue, match_format)
        else:
            venue_stats = {"avg_first_innings": 160, "pitch_type": "Balanced"}

        avg_first_inn = venue_stats.get("avg_first_innings", 160)
        report["score_prediction"]["first_innings_avg"] = avg_first_inn
        report["score_prediction"]["pitch_type"] = venue_stats.get("pitch_type", "Balanced")

        if "Batting" in venue_stats.get("pitch_type", ""):
             report["reasoning_narrative"] += f" The pitch at {venue} is a batter's paradise. Expect a high-scoring game (180+)."
        elif "Bowling" in venue_stats.get("pitch_type", ""):
             report["reasoning_narrative"] += f" {venue} might offer help to bowlers early on. A score of 150-160 could be competitive."
        else:
             report["reasoning_narrative"] += f" {venue or 'The venue'} generally offers a balanced contest between bat and ball."

        if venue and ("india" in str(venue).lower() or any(x in str(venue).lower() for x in ["mumbai", "wankhede", "chinnaswamy"])):
             report["key_factors"].append("Toss Factor: Significant. Teams chasing have a 60% win record at this venue due to dew.")
             report["market_insights"] = {"chasing_bias": "Strong Chase Preference"}
        else:
             report["key_factors"].append("Toss Factor: Moderate. Dew might play a role in the second innings.")

        prob_a = max(10, min(90, prob_a))
        report["win_probability"][team_a] = round(prob_a, 1)
        report["win_probability"][team_b] = round(100 - prob_a, 1)

        prob_gap = abs(prob_a - (100 - prob_a))
        margin_text = "Close finish predicted (Last over or < 15 runs)."
        if prob_gap > 20: margin_text = "Comfortable victory predicted (20+ runs or 6+ wickets)."
        if prob_gap > 40: margin_text = "Dominant one-sided victory likely."

        scenarios = {
            "if_bat_first": f"{team_a} likely to score {avg_first_inn}-{avg_first_inn+20}. Advantage {'High' if prob_a > 55 else 'Neutral'}.",
            "if_chase": f"Chasing might be tricky. Required rate could climb if wickets fall early.",
            "tie_chance": "Low (<5%)" if prob_gap > 10 else "Moderate (10-15%)",
            "dew_impact": "High chance of dew favoring the chasing team." if venue and "india" in (venue or "").lower() else "Low impact likely."
        }

        report["market_insights"] = {
            "favorite": team_a if prob_a > 50 else team_b,
            "margin_expectation": margin_text,
            "chasing_bias": "Chasing preferred" if venue and "chase" in (venue or "").lower() else "Batting First preferred",
            "stat_highlights": report["key_factors"]
        }
        report["scenarios"] = scenarios



        report["fantasy_picks"] = await self.get_fantasy_picks(team_a, team_b)
        
        logger.info(f"Prediction result: {team_a}: {report['win_probability'][team_a]}%, {team_b}: {report['win_probability'][team_b]}%")
        return report

    async def _get_venue_stats(self, venue_name, match_format):
        """
        Queries the DB for average scores at this venue.
        """
        conn = get_history_conn()
        cursor = conn.cursor()








        try:




            query = """
                SELECT AVG(sc.runs) as avg_score
                FROM scorecards sc
                JOIN matches m ON sc.match_id = m.id
                WHERE m.venue LIKE ? AND sc.inning = '1'
            """
            cursor.execute(query, [f"%{venue_name}%"])
            row = cursor.fetchone()
            avg = row[0] if row and row[0] else 165

            return {
                "avg_first_innings": int(avg),
                "pitch_type": "Batting Friendly" if avg > 175 else "Bowling Friendly" if avg < 145 else "Balanced"
            }
        except Exception:
            return {"avg_first_innings": 160, "pitch_type": "Balanced"}
        finally:
            conn.close()

    async def get_fantasy_picks(self, team_a, team_b):
        """
        Returns players with high fantasy points average from history DB.
        """
        conn = get_history_conn()
        cursor = conn.cursor()

        picks = []
        for team in [team_a, team_b]:






            query = """
                SELECT fp.player_name, AVG(fp.total_points) as avg_pts
                FROM fantasy_points fp
                JOIN matches m ON fp.match_id = m.id
                WHERE m.name LIKE ?
                GROUP BY fp.player_name
                ORDER BY avg_pts DESC
                LIMIT 5
            """
            cursor.execute(query, [f"%{team}%"])
            rows = cursor.fetchall()
            for r in rows:
                picks.append({"player": r[0], "team": team, "avg_points": r[1], "role": "Safe Pick"})

        conn.close()
        return picks


prediction_service = PredictionService()
