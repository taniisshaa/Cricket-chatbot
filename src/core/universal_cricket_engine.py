import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from openai import AsyncOpenAI
from src.utils.utils_core import Config

# -------------------------------------------------------------------------
# 🚀 ULTRA EXPERT CRICKET SQL ENGINE (PostgreSQL Optimized)
# -------------------------------------------------------------------------
DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def _process_raw_json_results(rows):
    """Processes PostgreSQL JSONB results into flattened row format and cleans massive blobs."""
    for row in rows:
        if "raw_json" in row and row["raw_json"]:
            raw = row["raw_json"]
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except:
                    continue
            
            row["batting_summary"] = []
            for b in raw.get("batting", []):
                name = b.get("batsman", {}).get("fullname") or "Unknown"
                row["batting_summary"].append({
                    "p": name,
                    "r": b.get("score"), "b": b.get("ball"), "sr": b.get("rate")
                })
            
            row["bowling_summary"] = []
            for bw in raw.get("bowling", []):
                name = bw.get("bowler", {}).get("fullname") or "Unknown"
                row["bowling_summary"].append({
                    "p": name,
                    "o": bw.get("overs"), "r": bw.get("runs"), "w": bw.get("wickets"), "e": bw.get("rate")
                })
            
            row["innings_scores"] = []
            for sb in raw.get("scoreboards", []):
                if sb.get("type") == "total":
                    t_id = sb.get("team_id")
                    team_name = "Team"
                    local = raw.get("localteam") or {}
                    visitor = raw.get("visitorteam") or {}
                    if t_id == local.get("id"): team_name = local.get("name")
                    elif t_id == visitor.get("id"): team_name = visitor.get("name")
                    row["innings_scores"].append(f"{team_name}: {sb.get('total')}/{sb.get('wickets')} in {sb.get('overs')} ov")

            row["match_note"] = raw.get("note")
            row["winner_id"] = raw.get("winner_team_id")
            
            # CRITICAL: Delete the 200KB blob so it doesn't get truncated or blow up context
            del row["raw_json"]
SYSTEM_PROMPT = """You are the **SUPREME CRICKET SQL ARCHITECT**.
Your mission is to generate **100% accurate, high-performance PostgreSQL queries**.

### 🏗️ DATABASE SCHEMA (PRECISE)
- **fixtures**: `id` (int), `season_id` (int), `name` (text, e.g. 'India vs Pakistan'), `starting_at` (timestamp), `status` (text), `venue_id` (int), `winner_team_id` (int), `raw_json` (jsonb).
- **seasons**: `id` (int), `league_id` (int), `name` (text, e.g. 'Indian Premier League 2024'), `year` (text, e.g. '2024').
- **teams**: `id` (int), `name` (text, e.g. 'Mumbai Indians').
- **season_champions**: `season_id` (int), `winner_team_id` (int), `final_match_id` (int).

### 🏆 TOURNAMENT AWARDS & LEADERBOARDS
1. **Orange Cap (Most Runs)**:
   ```sql
   -- Tie-breakers: 1. Higher SR (runs*100/ball), 2. Fewer balls faced
   SELECT bat->'batsman'->>'fullname' AS player, 
          SUM((bat->>'score')::int) AS total_runs,
          (SUM((bat->>'score')::float) * 100.0 / NULLIF(SUM((bat->>'ball')::float), 0)) AS strike_rate
   FROM fixtures f
   JOIN seasons s ON f.season_id = s.id
   CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'batting') AS bat
   WHERE (s.name ILIKE '%Indian Premier League%' OR s.name ILIKE '%IPL%') AND s.year = '2025'
   GROUP BY 1 ORDER BY total_runs DESC, strike_rate DESC LIMIT 1;
   ```

2. **Purple Cap (Most Wickets)**:
   ```sql
   -- Tie-breakers: 1. Better Economy (runs/overs), 2. Fewer runs conceded
   SELECT bowl->'bowler'->>'fullname' AS player, 
          SUM((bowl->>'wickets')::int) AS total_wickets,
          (SUM((bowl->>'runs')::float) / NULLIF(SUM((bowl->>'overs')::float), 0)) AS economy
   FROM fixtures f
   JOIN seasons s ON f.season_id = s.id
   CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'bowling') AS bowl
   WHERE (s.name ILIKE '%Indian Premier League%' OR s.name ILIKE '%IPL%') AND s.year = '2025'
   GROUP BY 1 ORDER BY total_wickets DESC, economy ASC LIMIT 1;
   ```

3. **MVP (Most Valuable Player)**:
   ```sql
   -- Points: 1 Run=1pt, 1 Wicket=25pts, 1 Six=3pts bonus, 1 Maiden=20pts.
   SELECT player_name, SUM(points) as mvp_points
   FROM (
     SELECT bat->'batsman'->>'fullname' as player_name, 
            SUM((bat->>'score')::int * 1 + COALESCE((bat->>'six_x')::int, 0) * 3) as points
     FROM fixtures f
     JOIN seasons s ON f.season_id = s.id
     CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'batting') AS bat
     WHERE s.name ILIKE '%IPL%' AND s.year = '2025' GROUP BY 1
     UNION ALL
     SELECT bowl->'bowler'->>'fullname', 
            SUM((bowl->>'wickets')::int * 25 + COALESCE((bowl->>'medians')::int, 0) * 20)
     FROM fixtures f
     JOIN seasons s ON f.season_id = s.id
     CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'bowling') AS bowl
     WHERE s.name ILIKE '%IPL%' AND s.year = '2025' GROUP BY 1
   ) stats GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
   ```

### ⚠️ CRITICAL RULES:
- **IPL Mapping**: Database uses "Indian Premier League". If user says "IPL", use `s.name ILIKE '%Indian Premier League%'`.
- **Year Filter**: `seasons.year` is TEXT. Use `s.year = '2025'`.
- **JSONB Keys**: Batting uses `score` (runs), `ball` (balls), `six_x` (sixes). Bowling uses `wickets`, `runs`, `overs`, `medians` (maidens).
- **Tie-breakers**: Include tie-breaking logic (SR for batting, Economy for bowling) in `ORDER BY`.
- **Output**: ONLY the SQL query. No markdown. No comments.
"""

class UniversalCricketEngine:
    def __init__(self):
        self.config = DB_CONFIG
        self.client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)

    async def generate_sql(self, user_query, context=""):
        current_date = datetime.now().strftime("%Y-%m-%d")
        prompt = f"System Date: {current_date}\nUser Query: {user_query}\nContext: {context}\nGenerate the best PostgreSQL query."
        response = await self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        sql = response.choices[0].message.content.strip()
        # Clean up Markdown if present
        sql = sql.replace("```sql", "").replace("```", "").strip()
        return sql

    async def execute_query(self, sql):
        try:
            conn = psycopg2.connect(**self.config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql)
            results = cur.fetchall()
            cur.close()
            conn.close()
            
            # Handle datetime serialization
            for row in results:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
                        
            return {"status": "success", "data": results}
        except Exception as e:
            return {"status": "error", "message": str(e), "sql": sql}

    def build_evidence_pack(self, sql_result, user_query):
        """Constructs a technical context bundle for the Research Agent."""
        if sql_result["status"] == "error":
            return {"query_status": "error", "message": sql_result["message"]}
            
        data = sql_result["data"]
        if not data:
            return {"query_status": "no_data", "data": []}
            
        return {
            "query_status": "success",
            "count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        }

from src.utils.utils_core import get_logger
logger = get_logger("universal_engine", "universal_engine.log")

async def handle_universal_cricket_query(user_query, context=None):
    """Entry point for the agent workflow."""
    engine = UniversalCricketEngine()
    
    # 1. Generate SQL
    sql = await engine.generate_sql(user_query, context=json.dumps(context) if context else "")
    logger.info(f"Generated SQL: {sql}")
    
    # 2. Execute
    result = await engine.execute_query(sql)
    logger.info(f"Execution Result: {result.get('status')} | Rows: {len(result.get('data', [])) if result.get('data') else 0}")
    
    # 3. Handle Auto-Fix if error
    if result["status"] == "error":
        logger.warning(f"SQL Error: {result['message']}")
        prompt = f"The previous SQL failed with error: {result['message']}\nSQL was: {result['sql']}\nFix it and provide ONLY the corrected SQL."
        sql_fixed = await engine.generate_sql(user_query, context=prompt)
        logger.info(f"Fixed SQL: {sql_fixed}")
        result = await engine.execute_query(sql_fixed)
        logger.info(f"Fixed Execution Result: {result.get('status')} | Rows: {len(result.get('data', [])) if result.get('data') else 0}")
    
    # Success processing: Flatten JSONB if present
    if result["status"] == "success" and result.get("data"):
        _process_raw_json_results(result["data"])
    
    # 4. Wrap for Research Agent
    return engine.build_evidence_pack(result, user_query)
