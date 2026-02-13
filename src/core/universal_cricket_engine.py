import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from openai import AsyncOpenAI
from src.utils.utils_core import Config
from src.utils.utils_core import get_logger

# -------------------------------------------------------------------------
# 🚀 ULTRA EXPERT CRICKET SQL ENGINE (PostgreSQL Optimized - LOGIC MODE)
# -------------------------------------------------------------------------
DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

SYSTEM_PROMPT = """You are the **CRICKET SQL ARCHITECT**. Generate accurate PostgreSQL queries.

### 🏗️ SCHEMA (Tables & JSON)
- **fixtures**: id, season_id, venue_id, name, starting_at, status, winner_team_id, raw_json.
  - `raw_json`: `scoreboards` (list), `batting` (list), `bowling` (list), `localteam`, `visitorteam`, `venue`.
  - **IDs**: `COALESCE((raw_json->'localteam'->>'id')::int, (raw_json->>'localteam_id')::int)`
- **venues**: id, name, city, capacity. (Join `fixtures.venue_id = venues.id`).
- **seasons**: id, league_id, name, year, code.
- **leagues**: id, name, code ('IPL').
- **teams**: id, name, code.
- **players**: id, fullname.
- **season_champions**: season_id, winner_team_id.

### 🧠 LOGIC KERNEL
1. **Basics**: JOIN `leagues`->`seasons`->`fixtures`. Join `venues` if location/stadium is requested.
2. **Winners**: `season_champions` (Season), `fixtures.winner_team_id` (Match).
3. **✨ AWARDS (Dynamic Calculation)**:
   - **Orange Cap**: `SUM((x->>'score')::int)` from `raw_json->'batting'`. Order DESC. **Tie-Break**: Higher SR.
   - **Purple Cap**: `SUM((x->>'wickets')::int)` from `raw_json->'bowling'`. Order DESC. **Tie-Break**: Lower Eco.
4. **📊 POINTS TABLE**:
   - **Formula**: `(Wins * 2) + (No Result * 1)`.
   - **Query**: agg wins/NR from `fixtures`. Order by Points DESC.
5. **⚡ PHASE ANALYSIS (Wickets)**:
   - **Powerplay**: `(bat->>'fow_balls')::float < 6.0`.
   - **Death**: `(bat->>'fow_balls')::float >= 16.0`.
   - **Rate**: `Count(Wickets) / Count(Matches)`.

### ⛔ RULES
- **No Hallucinations**: Don't invent specific columns not listed.
- **JSON**: Use `jsonb_array_elements`. `COALESCE` nulls.

### 📝 FORMAT
[REASONING]
1. Goal: ...
2. Logic: ...
[SQL]
```sql
SELECT ...
```
"""


def _process_raw_json_results(rows):
    """Processes PostgreSQL JSONB results into flattened row format."""
    for row in rows:
        # Construct a temporary raw object if split columns exist
        raw = row.get("raw_json")
        if not raw:
            # UNIVERSAL SCAN: If raw_json is missing, find any dict value that looks like the match data
            # (Matches often have 'batting' or 'scoreboards' keys)
            for val in row.values():
                if isinstance(val, dict) and ("batting" in val or "scoreboards" in val or "localteam" in val):
                    raw = val
                    break
            
        if not raw:
            # Fallback for individual split columns
            raw = {}
            if "batting_data" in row: raw["batting"] = row["batting_data"]
            if "bowling_data" in row: raw["bowling"] = row["bowling_data"]
            if "score_summary" in row: raw["scoreboards"] = row["score_summary"]
            elif "scorecard" in row: raw["scoreboards"] = row["scorecard"]
            if "scoreboards" in row: raw["scoreboards"] = row["scoreboards"]
            
        # If still likely string (sometimes json key returns string if not cast properly)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except:
                continue
        
        if not isinstance(raw, dict):
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
                
                # Robust ID extraction matching SQL Logic
                l_id = local.get("id") or raw.get("localteam_id")
                v_id = visitor.get("id") or raw.get("visitorteam_id")
                
                if t_id == l_id: team_name = local.get("name") or "Local Team"
                elif t_id == v_id: team_name = visitor.get("name") or "Visitor Team"
                row["innings_scores"].append(f"{team_name}: {sb.get('total')}/{sb.get('wickets')} in {sb.get('overs')} ov")

        row["match_note"] = raw.get("note")
        row["winner_id"] = raw.get("winner_team_id")
        
        # CRITICAL: Delete the 200KB blob so it doesn't get truncated or blow up context
        if "raw_json" in row: del row["raw_json"]


logger = get_logger("universal_engine", "universal_engine.log")

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
        raw_response = response.choices[0].message.content.strip()
        
        # 🧠 PARSE REASONING vs SQL
        # 1. Look for explicit [SQL] block from new prompt structure
        import re
        sql_match = re.search(r"\[SQL\](.*)", raw_response, re.DOTALL)
        if sql_match:
            sql = sql_match.group(1).strip()
        else:
            # 2. Look for markdown code block
            code_match = re.search(r"```sql(.*?)```", raw_response, re.DOTALL)
            if code_match:
                sql = code_match.group(1).strip()
            else:
                # 3. Fallback: Assume entire content is SQL (Clean markdown)
                sql = raw_response.replace("```sql", "").replace("```", "").strip()
                
        # Remove any lingering [REASONING] block if it wasn't caught
        if "[REASONING]" in sql:
             sql = sql.split("[REASONING]")[0].strip()
        
        # GUARDRAIL: Prevent 'scorecard' key hallucination
        if "scorecard" in sql.lower():
            logger.info("🛡️ GUARDRAIL: Scorecard hallucination detected in SQL. Correcting...")
            for arrow in ["->'scorecard'", "-> 'scorecard'", "-> \"scorecard\"", "->\"scorecard\""]:
                if arrow in sql:
                    sql = sql.replace(arrow, "->'scoreboards'")
            if "raw_json" in sql and "scorecard" in sql:
                import re
                sql = re.sub(r"([a-z0-9_]+\.)?raw_json\s*->\s*['\"]scorecard['\"]", r"\1raw_json", sql, flags=re.IGNORECASE)

        # GUARDRAIL: Ensure robust team identification (COALESCE)
        if "JOIN teams t_local" in sql or "JOIN teams t_visitor" in sql:
            import re
            # Much more robust regex: catches any alias, any spacing
            old_local = r"JOIN teams t_local ON \([a-z0-9_.]+\s*->\s*'localteam'\s*->>\s*'id'\)::int\s*=\s*t_local\.id"
            if re.search(old_local, sql, re.IGNORECASE):
                logger.info("🛡️ GUARDRAIL: Old localteam JOIN detected. Injecting COALESCE...")
                sql = re.sub(old_local, "JOIN teams t_local ON COALESCE((f.raw_json->'localteam'->>'id')::int, (f.raw_json->>'localteam_id')::int) = t_local.id", sql, flags=re.IGNORECASE)
            
            old_visitor = r"JOIN teams t_visitor ON \([a-z0-9_.]+\s*->\s*'visitorteam'\s*->>\s*'id'\)::int\s*=\s*t_visitor\.id"
            if re.search(old_visitor, sql, re.IGNORECASE):
                logger.info("🛡️ GUARDRAIL: Old visitorteam JOIN detected. Injecting COALESCE...")
                sql = re.sub(old_visitor, "JOIN teams t_visitor ON COALESCE((f.raw_json->'visitorteam'->>'id')::int, (f.raw_json->>'visitorteam_id')::int) = t_visitor.id", sql, flags=re.IGNORECASE)

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
            
            # Post-process JSON fields (critical for detailed views)
            try:
                _process_raw_json_results(results)
            except Exception as e:
                print(f"JSON Processing Warning: {e}")

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
    # 3. Handle Auto-Fix if error (Max 3 retries)
    retries = 0
    max_retries = 3
    
    while result["status"] == "error" and retries < max_retries:
        retries += 1
        logger.warning(f"SQL Error (Attempt {retries}/{max_retries}): {result['message']}")
        
        # Enhanced Fix Prompt
        fix_prompt = (
            f"⚠️ SQL EXECUTION FAILED.\n"
            f"Error Message: {result['message']}\n"
            f"Failed SQL: {result['sql']}\n"
            f"Task: Fix the SQL query to resolve this error completely.\n"
            f"Check: 1) Table names (fixtures, seasons, leagues, teams, players) 2) Column existence 3) JSONB syntax (raw_json->>'key').\n"
            f"Output: ONLY the corrected SQL query. No explanation."
        )
        
        sql_fixed = await engine.generate_sql(user_query, context=fix_prompt)
        logger.info(f"Generated Fix SQL (Attempt {retries}): {sql_fixed}")
        
        result = await engine.execute_query(sql_fixed)
        logger.info(f"Fixed Execution Result: {result.get('status')} | Rows: {len(result.get('data', [])) if result.get('data') else 0}")
        
    if result["status"] == "error":
        logger.error(f"❌ Failed to fix SQL after {max_retries} attempts.")
    
    
    # 4. Wrap for Research Agent
    evidence = engine.build_evidence_pack(result, user_query)
    evidence["executed_sql"] = result.get("sql") or (sql if result["status"] == "success" else None)
    return evidence
