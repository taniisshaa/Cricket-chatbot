"""
🔍 RAG RETRIEVER MODULE
======================
Smart database retrieval system that fetches EXACTLY the right data
for any cricket query without hallucination.

Features:
- Auto table detection
- Smart filtering
- JSON parsing
- Multi-source aggregation
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any, Optional
from src.utils.utils_core import get_logger

logger = get_logger("rag_retriever", "rag_retriever.log")

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

class SmartRetriever:
    """
    Intelligent data retrieval system that knows EXACTLY where to look
    for any cricket data.
    """
    
    def __init__(self):
        self.db_config = DB_CONFIG
        
    def _get_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(**self.db_config)
    
    def _execute_query(self, sql: str, params: tuple = None) -> List[Dict]:
        """Execute SQL and return results as list of dicts"""
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            
            results = cur.fetchall()
            cur.close()
            conn.close()
            
            # Convert to list of dicts and handle datetime
            output = []
            for row in results:
                row_dict = dict(row)
                for key, value in row_dict.items():
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                output.append(row_dict)
            
            return output
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"SQL: {sql}")
            return []
    
    async def retrieve_match_by_date(self, target_date: str, team_name: Optional[str] = None) -> List[Dict]:
        """
        Retrieve matches on a specific date.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            team_name: Optional team filter
        
        Returns:
            List of match records with scorecard data
        """
        logger.info(f"📅 Retrieving matches for date: {target_date}, team: {team_name}")
        
        if team_name:
            sql = """
            SELECT 
                f.id, f.name, f.starting_at, f.status,
                f.raw_json
            FROM fixtures f
            WHERE f.starting_at::date = %s
            AND f.name ILIKE %s
            ORDER BY f.starting_at DESC
            LIMIT 10
            """
            results = self._execute_query(sql, (target_date, f"%{team_name}%"))
        else:
            sql = """
            SELECT 
                f.id, f.name, f.starting_at, f.status,
                f.raw_json
            FROM fixtures f
            WHERE f.starting_at::date = %s
            ORDER BY f.starting_at DESC
            LIMIT 10
            """
            results = self._execute_query(sql, (target_date,))
        
        logger.info(f"✅ Found {len(results)} matches")
        return self._process_match_results(results)
    
    async def retrieve_player_stats(
        self, 
        player_name: str, 
        season_id: Optional[int] = None,
        year: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Retrieve comprehensive player statistics.
        
        Args:
            player_name: Player's name
            season_id: Optional season filter
            year: Optional year filter
        
        Returns:
            Dict with batting and bowling stats
        """
        logger.info(f"🏏 Retrieving stats for player: {player_name}, season: {season_id}, year: {year}")
        
        # Get player ID first
        player_sql = """
        SELECT id, fullname, position_name, country_id
        FROM players
        WHERE fullname ILIKE %s
        LIMIT 1
        """
        player_data = self._execute_query(player_sql, (f"%{player_name}%",))
        
        if not player_data:
            logger.warning(f"❌ Player not found: {player_name}")
            return {"error": f"Player '{player_name}' not found"}
        
        player = player_data[0]
        player_id = player["id"]
        
        # Build batting stats query
        batting_sql = """
        SELECT 
            bat->>'batsman'->>'fullname' AS player,
            COUNT(*) as innings,
            SUM((bat->>'score')::int) as total_runs,
            AVG((bat->>'score')::int) as average,
            MAX((bat->>'score')::int) as highest_score,
            SUM((bat->>'four_x')::int) as fours,
            SUM((bat->>'six_x')::int) as sixes,
            AVG(((bat->>'score')::float * 100.0 / NULLIF((bat->>'balls')::float, 0))) as strike_rate
        FROM fixtures f, jsonb_array_elements(f.raw_json->'batting') bat
        WHERE bat->'batsman'->>'id' = %s
        """
        
        params = [str(player_id)]
        
        if season_id:
            batting_sql += " AND f.season_id = %s"
            params.append(season_id)
        elif year:
            batting_sql += " AND EXTRACT(YEAR FROM f.starting_at) = %s"
            params.append(year)
        
        batting_sql += " GROUP BY bat->>'batsman'->>'fullname'"
        
        batting_stats = self._execute_query(batting_sql, tuple(params))
        
        # Build bowling stats query
        bowling_sql = """
        SELECT 
            bowl->>'bowler'->>'fullname' AS player,
            COUNT(*) as matches,
            SUM((bowl->>'wickets')::int) as total_wickets,
            SUM((bowl->>'runs')::int) as runs_conceded,
            AVG((bowl->>'rate')::float) as economy,
            MAX((bowl->>'wickets')::int) as best_figures
        FROM fixtures f, jsonb_array_elements(f.raw_json->'bowling') bowl
        WHERE bowl->'bowler'->>'id' = %s
        """
        
        params = [str(player_id)]
        
        if season_id:
            bowling_sql += " AND f.season_id = %s"
            params.append(season_id)
        elif year:
            bowling_sql += " AND EXTRACT(YEAR FROM f.starting_at) = %s"
            params.append(year)
        
        bowling_sql += " GROUP BY bowl->>'bowler'->>'fullname'"
        
        bowling_stats = self._execute_query(bowling_sql, tuple(params))
        
        result = {
            "player_info": player,
            "batting": batting_stats[0] if batting_stats else {},
            "bowling": bowling_stats[0] if bowling_stats else {}
        }
        
        logger.info(f"✅ Retrieved stats for {player_name}")
        return result
    
    async def retrieve_season_data(self, season_name: str, year: int) -> Dict[str, Any]:
        """
        Retrieve complete season information including winner, awards, standings.
        
        Args:
            season_name: Name of the season (e.g., "IPL", "World Cup")
            year: Year of the season
        
        Returns:
            Dict with season data
        """
        logger.info(f"🏆 Retrieving season data: {season_name} {year}")
        
        # Get season ID by joined name/code and year
        season_sql = """
        SELECT s.id, s.name, s.year, l.name as league_name
        FROM seasons s
        JOIN leagues l ON s.league_id = l.id
        WHERE (l.name ILIKE %s OR l.code ILIKE %s OR s.name ILIKE %s)
          AND s.year = %s
        LIMIT 1
        """
        season_data = self._execute_query(season_sql, (f"%{season_name}%", f"%{season_name}%", f"%{season_name}%", str(year)))
        
        if not season_data:
            logger.warning(f"❌ Season not found: {season_name} {year}")
            return {"error": f"Season '{season_name} {year}' not found"}
        
        season = season_data[0]
        season_id = season["id"]
        
        # Get champion
        champion_sql = """
        SELECT 
            wt.name as winner_team, 
            rt.name as runner_up_team,
            sc.winner_team_id,
            sc.runner_up_team_id,
            sc.final_match_id
        FROM season_champions sc
        JOIN teams wt ON sc.winner_team_id = wt.id
        LEFT JOIN teams rt ON sc.runner_up_team_id = rt.id
        WHERE sc.season_id = %s
        """
        champion_rows = self._execute_query(champion_sql, (season_id,))
        champion = champion_rows[0] if champion_rows else None
        
        # Extract Final Match ID from champions record if exists
        explicit_final_id = champion.get("final_match_id") if champion else None
        
        # Get awards
        awards_sql = """
        SELECT 
            sa.award_type,
            COALESCE(p.fullname, sa.player_name) as player_name,
            sa.team_name,
            sa.stats as value
        FROM season_awards sa
        LEFT JOIN players p ON sa.player_id = p.id
        WHERE sa.season_id = %s
        """
        awards = self._execute_query(awards_sql, (season_id,))
        
        # Get all matches list (lightweight)
        matches_sql = """
        SELECT 
            f.id, f.name, f.starting_at, f.status,
            f.raw_json->>'note' as result
        FROM fixtures f
        WHERE f.season_id = %s
        ORDER BY f.starting_at ASC
        """
        matches = self._execute_query(matches_sql, (season_id,))
        
        key_matches = []
        final_match = None
        
        if matches:
            # Strategy to find Final:
            # 1. Use explicit_final_id if exists
            # 2. Look for "Final" in name
            # 3. Last match by date
            
            final_candidate_id = explicit_final_id
            if not final_candidate_id:
                for m in reversed(matches):
                    if "Final" in m.get("name", ""):
                        final_candidate_id = m["id"]
                        break
            
            if not final_candidate_id:
                if matches:
                    final_candidate_id = matches[-1]["id"]

            # Fetch details for key matches (recent ones / Playoffs)
            # We combine the identified final + the last few matches
            candidate_ids = set([m["id"] for m in matches[-5:]])
            if final_candidate_id: candidate_ids.add(final_candidate_id)
            
            key_matches_sql = """
            SELECT 
                f.id, f.name, f.starting_at, f.status,
                f.raw_json,
                f.raw_json->>'note' as result
            FROM fixtures f
            WHERE f.id IN %s
            ORDER BY f.starting_at DESC
            """
            raw_key_matches = self._execute_query(key_matches_sql, (tuple(candidate_ids),))
            processed_key_matches = self._process_match_results(raw_key_matches)
            key_matches = processed_key_matches
            
            # Final is either the identified match or the top one in key matches
            final_match = next((m for m in key_matches if m["id"] == final_candidate_id), None)
            if not final_match and key_matches:
                final_match = key_matches[0]
        else:
            key_matches = []
            final_match = None
            
        result = {
            "season_info": season,
            "champion": champion,
            "awards": awards,
            "total_matches": len(matches),
            "matches": matches, 
            "key_matches": key_matches, 
            "final_match": final_match
        }
        
        logger.info(f"✅ Retrieved season data for {season_name} {year}")
        return result
    
    async def retrieve_head_to_head(
        self, 
        team_a: str, 
        team_b: str,
        limit: int = 10
    ) -> List[Dict]:
        """
        Retrieve head-to-head match history between two teams.
        
        Args:
            team_a: First team name
            team_b: Second team name
            limit: Maximum number of matches to return
        
        Returns:
            List of match records
        """
        logger.info(f"⚔️ Retrieving H2H: {team_a} vs {team_b}")
        
        sql = """
        SELECT 
            f.id, f.name, f.starting_at, f.status,
            f.raw_json->>'note' as result,
            f.raw_json
        FROM fixtures f
        WHERE f.name ILIKE %s
        AND f.name ILIKE %s
        ORDER BY f.starting_at DESC
        LIMIT %s
        """
        
        results = self._execute_query(sql, (f"%{team_a}%", f"%{team_b}%", limit))
        
        logger.info(f"✅ Found {len(results)} H2H matches")
        return self._process_match_results(results)
    
    async def retrieve_by_score(
        self, 
        score_value: int,
        team_name: Optional[str] = None,
        year: Optional[int] = None
    ) -> List[Dict]:
        """
        Find matches where a specific score was made.
        
        Args:
            score_value: The score to search for
            team_name: Optional team filter
            year: Optional year filter
        
        Returns:
            List of matching fixtures
        """
        logger.info(f"🎯 Searching for score: {score_value}, team: {team_name}, year: {year}")
        
        sql = """
        SELECT DISTINCT
            f.id, f.name, f.starting_at,
            sb->>'total' as score,
            sb->>'wickets' as wickets,
            sb->>'overs' as overs,
            f.raw_json
        FROM fixtures f,
        jsonb_array_elements(f.raw_json->'scoreboards') sb
        WHERE sb->>'type' = 'total'
        AND (sb->>'total')::int = %s
        """
        
        params = [score_value]
        
        if team_name:
            sql += " AND f.name ILIKE %s"
            params.append(f"%{team_name}%")
        
        if year:
            sql += " AND EXTRACT(YEAR FROM f.starting_at) = %s"
            params.append(year)
        
        sql += " ORDER BY f.starting_at DESC LIMIT 10"
        
        results = self._execute_query(sql, tuple(params))
        
        logger.info(f"✅ Found {len(results)} matches with score {score_value}")
        return self._process_match_results(results)
    
    def _process_match_results(self, results: List[Dict]) -> List[Dict]:
        """
        Process raw match results to extract key information from raw_json.
        Also enriches with Team Names from DB.
        """
        processed = []
        team_ids_to_fetch = set()
        
        # First pass: Process structure and collect IDs
        for match in results:
            if "raw_json" in match and match["raw_json"]:
                raw = match["raw_json"]
                
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except:
                        raw = {}
                
                # Try to get IDs directly from keys if available, else from scoreboards
                # Usually raw_json has localteam_id, visitorteam_id at top level
                # OR nested objects: raw_json->'localteam'->'id'
                
                def get_tid(key_prefix):
                    # Try flat key first: localteam_id
                    val = raw.get(f"{key_prefix}_id")
                    if val: return val
                    # Try object key: localteam -> id
                    obj = raw.get(key_prefix)
                    if isinstance(obj, dict): return obj.get("id")
                    return None

                lt_id = get_tid("localteam")
                vt_id = get_tid("visitorteam")
                wt_id = get_tid("winner_team") or raw.get("winner_team_id")
                
                if lt_id: team_ids_to_fetch.add(lt_id)
                if vt_id: team_ids_to_fetch.add(vt_id)
                if wt_id: team_ids_to_fetch.add(wt_id)

                match["innings_summary"] = []
                for sb in raw.get("scoreboards", []):
                    if sb.get("type") == "total":
                        tid = sb.get("team_id")
                        if tid: team_ids_to_fetch.add(tid)
                        
                        match["innings_summary"].append({
                            "team_id": tid,
                            "team_name": f"Team {tid}", # Placeholder
                            "score": sb.get("total"),
                            "wickets": sb.get("wickets"),
                            "overs": sb.get("overs")
                        })
                
                match["top_batsmen"] = []
                for bat in raw.get("batting", [])[:3]:  # Top 3
                    match["top_batsmen"].append({
                        "name": bat.get("batsman", {}).get("fullname"),
                        "runs": bat.get("score"),
                        "balls": bat.get("balls"),
                        "sr": bat.get("rate")
                    })
                
                match["top_bowlers"] = []
                for bowl in raw.get("bowling", [])[:3]:  # Top 3
                    match["top_bowlers"].append({
                        "name": bowl.get("bowler", {}).get("fullname"),
                        "wickets": bowl.get("wickets"),
                        "runs": bowl.get("runs"),
                        "overs": bowl.get("overs"),
                        "economy": bowl.get("rate")
                    })
                
                match["result"] = raw.get("note")
                match["winner_team_id"] = raw.get("winner_team_id")
                
                # Save IDs for enrichment
                match["_local_id"] = raw.get("localteam_id")
                match["_visitor_id"] = raw.get("visitorteam_id")
                
                if "raw_json" in match:
                     del match["raw_json"]
            
            processed.append(match)
            
        # Bulk Fetch Team Names
        if team_ids_to_fetch:
            try:
                ids_tuple = tuple(team_ids_to_fetch)
                sql = "SELECT id, name FROM teams WHERE id IN %s"
                team_rows = self._execute_query(sql, (ids_tuple,))
                team_map = {row["id"]: row["name"] for row in team_rows}
                
                # Second pass: Enrich with names
                for match in processed:
                    # Enrich innings summary
                    if "innings_summary" in match:
                        for inn in match["innings_summary"]:
                            tid = inn.get("team_id")
                            if tid in team_map:
                                inn["team_name"] = team_map[tid]
                    
                    # Enrich match name if needed (often match name is already "A vs B" but verification helps)
                    # We can leave match['name'] as is, it comes from DB 'name' column.
                    
                    # Ensure Winner Name if needed?
                    # The context builder uses winner_team_id for logic, but usually we just show Result string.
                    pass
            except Exception as e:
                logger.error(f"Failed to enrich team names: {e}")
                
        return processed

# Global instance
smart_retriever = SmartRetriever()
