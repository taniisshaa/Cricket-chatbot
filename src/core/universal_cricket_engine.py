import os
import json
import sqlite3
from typing import Dict, Any, List
from openai import AsyncOpenAI
from src.utils.utils_core import get_logger, Config
logger = get_logger("universal_engine", "PAST_HISTORY.log")
DB_PATH = os.path.join("data", "full_raw_history.db")
SYSTEM_PROMPT = """
You are the **Ultimate Grandmaster of Cricket Intelligence & SQL Engineering**. 🏏🧠
**YOUR MISSION:**
Transform any natural language cricket query—be it slang, complex analysis, or historical trivia—into a single, flawless, high-performance SQLite query. You possess deep, intrinsic knowledge of cricket rules, formats (IPL, BBL, PSL, T20I, ODI, Tests), and statistical significance.
---
Query ONLY these tables. Access specific match data via `fixtures.raw_json`.
*   `leagues` (id, name, type)
*   `seasons` (id, league_id, name, year) -> Join with `fixtures` on `season_id`.
*   `teams` (id, name, code)
*   `players` (id, fullname, batting_style, bowling_style, country_id)
*   `venues` (id, name, city, capacity)
*   `fixtures` (id, season_id, name, starting_at, status, venue_id, winner_team_id, raw_json)
    *   `status`: 'Finished', 'Completed', 'Aban.', 'Live', 'NS'.
    *   `winner_team_id`: NULL for ties/abandoned/NS.
Most detailed stats reside here. Use `json_each` to flatten arrays.
**Path Examples:**
*   `json_each(f.raw_json, '$.batting')` -> `bat.value ->> '$.score'` (runs), `$.ball` (balls), `$.four_x`, `$.six_x`, `$.rate`, `$.sort` (**Batting Position/Order**), `$.fow_score`, `$.fow_balls` (**Partnership markers**).
*   `json_each(f.raw_json, '$.bowling')` -> `bowl.value ->> '$.wickets'`, `$.runs`, `$.overs`, `$.medians`, `$.rate` (economy).
*   `json_each(f.raw_json, '$.scoreboards')` -> `sb.value ->> '$.total'`, `$.wickets`, `$.overs`, `$.type` (Use 'total').
*   **Other Fields**: `f.raw_json ->> '$.note'` (Match Result Summary), `CAST(f.raw_json ->> '$.toss_won_team_id' AS INTEGER)`, `f.raw_json ->> '$.elected'` (Toss decision), `f.raw_json ->> '$.manofmatch.id'` (**CRITICAL**: Use this for Man of the Match player_id), `f.raw_json ->> '$.venue.name'` (Venue name).
*   **Team Names**: Access via `f.raw_json ->> '$.localteam.name'` and `f.raw_json ->> '$.visitorteam.name'`.
1.  **Tournament/Season**: For tournament queries (e.g., "IPL 2025"), ALWAYS filter `seasons.name LIKE '%IPL%' OR seasons.name LIKE '%Indian Premier League%'` AND `seasons.year = 2025`.
2.  **Match Result/Summary**: Select `fixtures.name` and `f.raw_json ->> '$.note'`.
3.  **Head-to-Head**: Filter `fixtures` where `(team1_id AND team2_id)` are present in team names or IDs.
4.  **Points Table**: Calculate by grouping `winner_team_id`. Win = 2 pts, Aban/NR/Draw = 1 pt.
5.  **Batting order**: Use `bat.value ->> '$.sort'`. Sort 1-2 are openers, 3-7 middle order, etc.
6.  **Partnerships**: Derivable by comparing `fow_score` of consecutive wickets. If not possible, summarize standard batting stats.
7.  **Milestones**:
    *   *Century*: `bat.value ->> '$.score' >= 100`.
    *   *5-Wicket Haul*: `bowl.value ->> '$.wickets' >= 5`.
8.  **Form History**: Order matches by `starting_at` for a specific player/team and show the last N matches.
9.  **Venue Stats**: Average score = `AVG(total)` where `sb.type = 'total'`. Filter by `venue_id`.
10. **Lowest/Highest Totals**: `MIN/MAX(CAST(sb.value ->> '$.total' AS INTEGER))` where `sb.value ->> '$.type' = 'total'`.
---
1.  **JSON Syntax**: When using `json_each(table.raw_json, '$.path') AS alias`, fields are accessed as `alias.value ->> '$.field'`.
2.  **Casting**: ALWAYS `CAST(... AS INTEGER)` or `CAST(... AS REAL)` for math (comparison/sum/avg).
3.  **Ambiguity**: If a team name is fuzzy (e.g., "MI", "CSK"), use `LIKE '%Mumbai Indians%'` or `LIKE '%Chennai Super Kings%'`.
4.  **Joins**: Joined `players.id` with `bat/bowl.value ->> '$.player_id'`. Joined `teams.id` with `fixtures.winner_team_id` or `sb.value ->> '$.team_id'`.
5.  **Single Result**: For queries like "IPL 2025 Winner", find the winner of the match with the latest `starting_at` in that season.
6.  **ESCAPING (CRITICAL)**: If a search string contains a single quote (e.g., "Men's", "Women's"), you **MUST** escape it by doubling the single quote (e.g., `'Men''s'`, `'Women''s'`).
7.  **Output**: **ONLY** the SQL query. No chat, no markdown blocks, no prefix/suffix. Just the string.
8.  **Limit**: Unless specified otherwise, limit results to the most relevant top N records (usually `LIMIT 1` or `LIMIT 10`).
---
**USER QUERY:** {user_query}
**SQL:**
"""
async def generate_sql(user_query: str) -> str:
    """Uses LLM to generate SQL from natural language."""
    client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY, timeout=60.0)
    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Query: {user_query}"}
            ],
            temperature=0,
            max_tokens=800
        )
        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        sql_keywords = ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE']
        for keyword in sql_keywords:
            if keyword in sql.upper():
                idx = sql.upper().find(keyword)
                sql = sql[idx:]
                break
        if ';' in sql:
            last_semi = sql.rfind(';')
            sql = sql[:last_semi + 1]
        sql = ' '.join(sql.split())
        return sql
    except Exception as e:
        logger.error(f"SQL Generation Error: {e}")
        return ""
async def execute_query(sql: str) -> List[Dict[str, Any]]:
    """Executes the generated SQL against the database."""
    if not sql: return []
    logger.info(f"Executing SQL: {sql}")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]
        conn.close()
        return results
    except Exception as e:
        logger.error(f"SQL Execution Error: {e}")
        return [{"error": str(e), "sql": sql}]
async def fix_sql(original_sql: str, error_msg: str, user_query: str) -> str:
    """Uses LLM to fix a broken SQL query based on the error message."""
    client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY, timeout=30.0)
    try:
        REPAIR_PROMPT = f"""
        You are an expert SQL Debugger.
        The following SQLite query failed to execute.
        **User Query**: "{user_query}"
        **Broken SQL**: {original_sql}
        **Error Message**: {error_msg}
        **YOUR TASK**:
        1. Fix the SQL syntax error.
        2. Specifically check for **unescaped single quotes** in string literals (e.g. 'Women's' -> 'Women''s').
        3. Return ONLY the fixed SQL string. No explanations.
        """
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": REPAIR_PROMPT}],
            temperature=0,
            max_tokens=800
        )
        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        if ';' in sql: sql = sql[:sql.rfind(';')+1]
        return sql
    except Exception as e:
        logger.error(f"SQL Fix Error: {e}")
        return original_sql
async def handle_universal_cricket_query(user_query: str) -> Dict[str, Any]:
    """Main entry point for smart past data queries."""
    sql = await generate_sql(user_query)
    data = await execute_query(sql)
    if data and isinstance(data, list) and len(data) == 1 and "error" in data[0]:
        error_msg = data[0]["error"]
        logger.warning(f"SQL Execution Failed: {error_msg}. Attempting AI Repair...")
        fixed_sql = await fix_sql(sql, error_msg, user_query)
        if fixed_sql != sql:
            logger.info(f"Retrying with Fixed SQL: {fixed_sql}")
            data = await execute_query(fixed_sql)
            sql = fixed_sql # Update for interpretation
    return {
        "interpretation": {"query_type": "DYNAMIC_SQL", "sql": sql},
        "data": data,
        "query": user_query
    }