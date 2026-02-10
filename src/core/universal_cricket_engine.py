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
                if t_id == local.get("id"): team_name = local.get("name")
                elif t_id == visitor.get("id"): team_name = visitor.get("name")
                row["innings_scores"].append(f"{team_name}: {sb.get('total')}/{sb.get('wickets')} in {sb.get('overs')} ov")

        row["match_note"] = raw.get("note")
        row["winner_id"] = raw.get("winner_team_id")
        
        # CRITICAL: Delete the 200KB blob so it doesn't get truncated or blow up context
        if "raw_json" in row: del row["raw_json"]

SYSTEM_PROMPT = """You are the **SUPREME CRICKET SQL ARCHITECT**.
Your mission is to generate **100% accurate, high-performance PostgreSQL queries**.

### 🏗️ DATABASE SCHEMA (PRECISE)
- **fixtures**: `id` (int), `season_id` (int), `name` (text), `starting_at` (timestamp), `status` (text), `venue_id` (int), `winner_team_id` (int), `toss_won_team_id` (int), `man_of_match_id` (int), `raw_json` (jsonb).
  - **`raw_json` Structure**:
    - `scoreboards`: Array `[{ "team_id": int, "total": int, "wickets": int, "overs": float, "type": "total" }]`
    - `batting`: Array `[{ "batsman": { "fullname": text, "id": int }, "score": int, "ball": int, "four_x": int, "six_x": int, "rate": float }]`
    - `bowling`: Array `[{ "player_id": int, "overs": float, "runs": int, "wickets": int, "rate": float, "medians": int (Maidens), "wide": int, "noball": int }]`
    - `localteam` / `visitorteam`: Object `{"id": int, "name": text}`
- **seasons**: `id` (int), `league_id` (int), `name` (text), `year` (text), `code` (text), `raw_json` (jsonb).
- **leagues**: `id` (int), `name` (text), `code` (text), `type` (text), `raw_json` (jsonb).
- **teams**: `id` (int), `name` (text), `code` (text), `raw_json` (jsonb).
- **players**: `id` (int), `fullname` (text), `dateofbirth` (text), `batting_style` (text), `bowling_style` (text), `country_id` (int), `raw_json` (jsonb).
- **season_champions**: `id` (int), `season_id` (int), `league_id` (int), `year` (text), `winner_team_id` (int), `runner_up_team_id` (int), `final_match_id` (int).
- **season_awards**: `id` (int), `season_id` (int), `award_type` (text), `player_id` (int), `player_name` (text), `team_name` (text), `stats` (text).
- **venues**: `id` (int), `name` (text), `city` (text), `capacity` (int), `raw_json` (jsonb).

### ⛔ FORBIDDEN PATTERNS (DO NOT USE)
- ❌ `raw_json->'scorecard'` (DOES NOT EXIST)
- ❌ `raw_json->'match_summary'` (DOES NOT EXIST)
- ❌ `raw_json->'statistics'` (DOES NOT EXIST)
- ❌ `dots` / `dot_balls` (DOES NOT EXIST in bowling data)
- ✅ USE ONLY: `batting`, `bowling` (has maidens/wides/noballs), `scoreboards`, `winner_team_id`.

### 🏆 CORE SQL LOGICS (NO HARDCODING)

1. **Match Result (Winner/Runner-up)**:
   ```sql
   -- Winner: t_win.name | Runner-up: t_lose.name
   SELECT f.name as match, t_win.name as winner, t_lose.name as runner_up
   FROM fixtures f
   JOIN teams t_win ON f.winner_team_id = t_win.id
   -- Fallback for runner-up logic: Check local/visitor IDs from JSON
   JOIN teams t_lose ON (
       CASE 
           WHEN f.winner_team_id = COALESCE((f.raw_json->>'localteam_id')::int, (f.raw_json->'localteam'->>'id')::int) 
           THEN COALESCE((f.raw_json->>'visitorteam_id')::int, (f.raw_json->'visitorteam'->>'id')::int) 
           ELSE COALESCE((f.raw_json->>'localteam_id')::int, (f.raw_json->'localteam'->>'id')::int) 
       END
   ) = t_lose.id
   JOIN seasons s ON f.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') 
     AND f.name ILIKE '%[TeamA]%' AND f.name ILIKE '%[TeamB]%' AND s.year = '[Year]';
   ```

2. **Tournament Outcomes (Champion/Runner-up)**:
   ```sql
   -- Champion from season_champions
   SELECT t.name as champion, l.name as tournament, s.year
   FROM season_champions sc
   JOIN seasons s ON sc.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   JOIN teams t ON sc.winner_team_id = t.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]';
   
   -- Runner-up (Derived from final match loser)
   -- Runner-up (Derived from final match loser)
   SELECT t_lose.name AS runner_up
   FROM season_champions sc
   JOIN fixtures f ON sc.final_match_id = f.id
   JOIN teams t_lose ON (
       CASE 
           WHEN f.winner_team_id = COALESCE((f.raw_json->>'localteam_id')::int, (f.raw_json->'localteam'->>'id')::int) 
           THEN COALESCE((f.raw_json->>'visitorteam_id')::int, (f.raw_json->'visitorteam'->>'id')::int) 
           ELSE COALESCE((f.raw_json->>'localteam_id')::int, (f.raw_json->'localteam'->>'id')::int) 
       END
   ) = t_lose.id
   JOIN seasons s ON sc.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]';
   ```


6. **Detailed Match Analysis (Batting/Bowling)**:
   ```sql
   -- LOGIC: Retrieve ENRICHED match details with PLAYER NAMES.
   -- Replaces raw IDs with Full Names via LATERAL JOINs.
   SELECT f.name as match, f.status, f.venue_id, t.name as winner_name,
          (
            SELECT jsonb_agg(jsonb_build_object('player', COALESCE(p.fullname, 'Unknown'), 'runs', (x->>'score')::int, 'balls', (x->>'ball')::int, '4s', (x->>'four_x')::int, '6s', (x->>'six_x')::int, 'sr', (x->>'rate')::float))
            FROM jsonb_array_elements(f.raw_json->'batting') x
            LEFT JOIN players p ON (x->>'player_id')::int = p.id
          ) as batting_data,
          (
            SELECT jsonb_agg(jsonb_build_object('player', COALESCE(p.fullname, 'Unknown'), 'o', (x->>'overs')::float, 'r', (x->>'runs')::int, 'w', (x->>'wickets')::int, 'eco', (x->>'rate')::float))
            FROM jsonb_array_elements(f.raw_json->'bowling') x
            LEFT JOIN players p ON (x->>'player_id')::int = p.id
          ) as bowling_data,
          f.raw_json->'scoreboards' as score_summary
   FROM fixtures f
   JOIN season_champions sc ON f.id = sc.final_match_id
   JOIN seasons s ON sc.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   LEFT JOIN teams t ON f.winner_team_id = t.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%')
     AND s.year = '[Year]';
   ```

   -- Alternative for non-final matches:
   ```sql
   SELECT f.name as match, f.status, f.venue_id, f.winner_team_id, t.name as winner_name,
          (
            SELECT jsonb_agg(jsonb_build_object('player', COALESCE(p.fullname, 'Unknown'), 'runs', (x->>'score')::int, 'balls', (x->>'ball')::int, 'sr', (x->>'rate')::float))
            FROM jsonb_array_elements(f.raw_json->'batting') x
            LEFT JOIN players p ON (x->>'player_id')::int = p.id
          ) as batting_data,
          (
            SELECT jsonb_agg(jsonb_build_object('player', COALESCE(p.fullname, 'Unknown'), 'o', (x->>'overs')::float, 'r', (x->>'runs')::int, 'w', (x->>'wickets')::int, 'eco', (x->>'rate')::float))
            FROM jsonb_array_elements(f.raw_json->'bowling') x
            LEFT JOIN players p ON (x->>'player_id')::int = p.id
          ) as bowling_data,
          f.raw_json->'scoreboards' as score_summary
   FROM fixtures f
   JOIN seasons s ON f.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   LEFT JOIN teams t ON f.winner_team_id = t.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%')
     AND s.year = '[Year]'
     AND (f.name ILIKE '%[TeamA]%' OR f.name ILIKE '%[TeamB]%')
   ORDER BY f.starting_at DESC LIMIT 1;
   ```

   16. **Team Season Journey / All Matches**:
    ```sql
    -- LOGIC: Retrieve ALL matches for a specific team in a season.
    -- Essential for "Journey", "Performance", "Road to Final" queries.
    -- IMPORTANT: For RCB (Royal Challengers Bangalore/Bengaluru), check BOTH names or use code 'RCB' via JOIN.
    SELECT f.name as match, f.status, f.venue_id, t_win.name as winner_name,
           (
             SELECT jsonb_agg(jsonb_build_object('player', COALESCE(p.fullname, 'Unknown'), 'runs', (x->>'score')::int, 'balls', (x->>'ball')::int, 'sr', (x->>'rate')::float))
             FROM jsonb_array_elements(f.raw_json->'batting') x
             LEFT JOIN players p ON (x->>'player_id')::int = p.id
           ) as batting_data,
           f.raw_json->'scoreboards' as score_summary
    FROM fixtures f
    JOIN seasons s ON f.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    LEFT JOIN teams t_win ON f.winner_team_id = t_win.id
    -- Strict Team Filter via JOIN
    JOIN teams t_local ON (f.raw_json->'localteam'->>'id')::int = t_local.id
    JOIN teams t_visitor ON (f.raw_json->'visitorteam'->>'id')::int = t_visitor.id
    WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%')
      AND s.year = '[Year]'
      AND (
          t_local.name ILIKE '%[TeamName]%' OR t_local.code = '[TeamCode]' 
          OR t_visitor.name ILIKE '%[TeamName]%' OR t_visitor.code = '[TeamCode]'
          -- Special Case for RCB:
          OR t_local.name ILIKE '%Bangalore%' OR t_local.name ILIKE '%Bengaluru%'
          OR t_visitor.name ILIKE '%Bangalore%' OR t_visitor.name ILIKE '%Bengaluru%'
      )
    ORDER BY f.starting_at ASC;
    ```

   17. **Head-to-Head / Rivalry**:
    ```sql
    -- LOGIC: Retrieve ALL matches between TWO specific teams in a season.
    -- Essential for "PBKS vs RCB head-to-head", "Previous encounters", "Rivalry stats".
    -- IMPORTANT: Check both Home/Away combinations.
    SELECT f.name as match, f.status, f.venue_id, t_win.name as winner_name,
           f.raw_json->'scoreboards' as score_summary
    FROM fixtures f
    JOIN seasons s ON f.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    LEFT JOIN teams t_win ON f.winner_team_id = t_win.id
    -- Use COALESCE to handle both data structures
    JOIN teams t_local ON COALESCE((f.raw_json->>'localteam_id')::int, (f.raw_json->'localteam'->>'id')::int) = t_local.id
    JOIN teams t_visitor ON COALESCE((f.raw_json->>'visitorteam_id')::int, (f.raw_json->'visitorteam'->>'id')::int) = t_visitor.id
    WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%')
      AND s.year = '[Year]'
      AND (
          (
            (t_local.name ILIKE '%[TeamA]%' OR t_local.code = '[TeamACode]' OR t_local.name ILIKE '%Bangalore%' OR t_local.name ILIKE '%Bengaluru%') 
            AND 
            (t_visitor.name ILIKE '%[TeamB]%' OR t_visitor.code = '[TeamBCode]' OR t_visitor.name ILIKE '%Bangalore%' OR t_visitor.name ILIKE '%Bengaluru%')
          )
          OR
          (
            (t_local.name ILIKE '%[TeamB]%' OR t_local.code = '[TeamBCode]' OR t_local.name ILIKE '%Bangalore%' OR t_local.name ILIKE '%Bengaluru%') 
            AND 
            (t_visitor.name ILIKE '%[TeamA]%' OR t_visitor.code = '[TeamACode]' OR t_visitor.name ILIKE '%Bangalore%' OR t_visitor.name ILIKE '%Bengaluru%')
          )
      )
    ORDER BY f.starting_at ASC;
    ```

   18. **Bowling Extras & Maidens (Deep Stats)**:
    ```sql
    -- LOGIC: Aggregate maidens (medians), wides, noballs.
    -- KEY: 'medians' = Maidens in this DB.
    -- Use for "Most maidens", "Most wides", "Most no balls".
    SELECT p.fullname as bowler, t.name as team,
           SUM((bowl->>'medians')::int) as total_maidens,
           SUM((bowl->>'wide')::int) as total_wides,
           SUM((bowl->>'noball')::int) as total_noballs,
           SUM((bowl->>'wickets')::int) as wickets,
           COUNT(f.id) as innings
    FROM fixtures f
    JOIN seasons s ON f.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'bowling') AS bowl
    JOIN players p ON (bowl->>'player_id')::int = p.id
    JOIN teams t ON (bowl->>'team_id')::int = t.id
    WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%')
      AND s.year = '[Year]' AND f.status = 'Finished'
    GROUP BY p.fullname, t.name
    ORDER BY total_maidens DESC -- Change sort based on user query (wides/noballs)
    LIMIT 5;
    ```

   19. **Venue Analytics (Avg Scores)**:
    ```sql
    -- LOGIC: Calculate Avg 1st Innings Score and Chasing Stats for a Venue.
    -- S1 = 1st Innings, S2 = 2nd Innings.
    SELECT v.name as venue, v.city,
           COUNT(f.id) as matches,
           AVG((s1->>'total')::int)::int as avg_1st_inns,
           AVG((s2->>'total')::int)::int as avg_2nd_inns,
           MAX((s2->>'total')::int) FILTER (WHERE f.winner_team_id = (s2->>'team_id')::int) as highest_chase
    FROM fixtures f
    JOIN venues v ON f.venue_id = v.id
    JOIN seasons s ON f.season_id = s.id
    LEFT JOIN LATERAL jsonb_array_elements(f.raw_json->'scoreboards') s1 ON (s1->>'scoreboard' = 'S1')
    LEFT JOIN LATERAL jsonb_array_elements(f.raw_json->'scoreboards') s2 ON (s2->>'scoreboard' = 'S2')
    WHERE (v.name ILIKE '%[VenueName]%' OR v.city ILIKE '%[City]%')
      AND f.status = 'Finished'
      AND s.year >= '2024' -- Force recent years for 'last 2 years' queries
    GROUP BY v.name, v.city;
    ```

   10. **Team Comparison / Points Table (Top N Teams)**:
    ```sql
    -- LOGIC: Aggregate wins, losses, and NRR (if available) for the requested tournament.
    -- Useful for "Compare top 2 teams" or "Points Table".
    WITH TeamStats AS (
        SELECT t.name as team_name,
               COUNT(*) FILTER (WHERE f.winner_team_id = t.id) as wins,
               COUNT(*) FILTER (WHERE f.winner_team_id IS NOT NULL AND f.winner_team_id != t.id AND f.status = 'Finished') as losses,
               COUNT(*) as matches_played
        FROM fixtures f
        JOIN seasons s ON f.season_id = s.id
        JOIN leagues l ON s.league_id = l.id
        JOIN teams t ON (
            COALESCE((f.raw_json->>'localteam_id')::int, (f.raw_json->'localteam'->>'id')::int) = t.id 
            OR 
            COALESCE((f.raw_json->>'visitorteam_id')::int, (f.raw_json->'visitorteam'->>'id')::int) = t.id
        )
        WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
        AND f.status = 'Finished'
        GROUP BY t.name
    )
    SELECT team_name, matches_played, wins, losses
    FROM TeamStats
    ORDER BY wins DESC, matches_played ASC
    LIMIT [Limit]; -- Set to 2 for "Top 2 teams", 10 for full table
    ```

7. **Orange Cap (Most Runs)**:
   -- LOGIC: Check season_awards FIRST (Use ILIKE for fuzzy match). Fallback to aggregation if empty using UNION ALL.
   SELECT award_type as category, player_name as player, stats as value
   FROM season_awards sa
   JOIN seasons s ON sa.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
     AND sa.award_type ILIKE '%Orange Cap%'
   UNION ALL
   SELECT 'Calculated Orange Cap' as category, p.fullname AS player, SUM((bat->>'score')::int)::text AS value
   FROM fixtures f
   JOIN seasons s ON f.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'batting') AS bat
   JOIN players p ON (bat->>'player_id')::int = p.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
     AND NOT EXISTS (SELECT 1 FROM season_awards sa2 JOIN seasons s2 ON sa2.season_id = s2.id WHERE s2.year = '[Year]' AND sa2.award_type ILIKE '%Orange Cap%')
   GROUP BY p.fullname 
   ORDER BY value DESC 
   LIMIT 1;
   ```

8. **Purple Cap (Most Wickets)**:
   -- LOGIC: Check season_awards FIRST (Use ILIKE for fuzzy match). Fallback to aggregation if empty.
   SELECT award_type as category, player_name as player, stats as value
   FROM season_awards sa
   JOIN seasons s ON sa.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
     AND sa.award_type ILIKE '%Purple Cap%'
   UNION ALL
   SELECT 'Calculated Purple Cap' as category, p.fullname AS player, SUM((bowl->>'wickets')::int)::text AS value
   FROM fixtures f
   JOIN seasons s ON f.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'bowling') AS bowl
   JOIN players p ON (bowl->>'player_id')::int = p.id
   WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
     AND NOT EXISTS (SELECT 1 FROM season_awards sa2 JOIN seasons s2 ON sa2.season_id = s2.id WHERE s2.year = '[Year]' AND sa2.award_type ILIKE '%Purple Cap%')
   GROUP BY p.fullname 
   ORDER BY value DESC 
   LIMIT 1;
   ```
   
   11. **Team Bowling Performance (Best Bowling Unit)**:
    ```sql
    -- LOGIC: Aggregate wickets taken by each team's bowlers.
    -- Use this for queries like "Which team had the best bowling unit?" or "Most wickets by a team".
    SELECT t.name as team_name, SUM((bowl->>'wickets')::int) as total_wickets, AVG((bowl->>'rate')::float) as avg_economy
    FROM fixtures f
    JOIN seasons s ON f.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'bowling') AS bowl
    JOIN teams t ON (bowl->>'team_id')::int = t.id
    WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
    GROUP BY t.name
    ORDER BY total_wickets DESC
    LIMIT 10;
    ```

5. **MVP (Valuable Player - Dynamic Formula)**:
   ```sql
   -- FORMULA: (runs * 2.5) + (fours * 3.5) + (sixes * 5.0) + (wickets * 3.5) + (dot_balls * 1.0) + (catches/stumping * 2.5)
   SELECT player_name, SUM(mvp_points) as total_points
   FROM (
     -- Batting Points (Runs, 4s, 6s)
     SELECT p.fullname as player_name, 
            SUM(
                COALESCE((bat->>'score')::int, 0) * 2.5 + 
                COALESCE((bat->>'four_x')::int, 0) * 3.5 + 
                COALESCE((bat->>'six_x')::int, 0) * 5.0
            ) as mvp_points
     FROM fixtures f JOIN seasons s ON f.season_id = s.id
     JOIN leagues l ON s.league_id = l.id
     CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'batting') AS bat
     JOIN players p ON (bat->>'player_id')::int = p.id
     WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]' GROUP BY 1
     
     UNION ALL
     
     -- Bowling Points (Wickets)
     SELECT p.fullname, 
            SUM(COALESCE((bowl->>'wickets')::int, 0) * 3.5)
     FROM fixtures f JOIN seasons s ON f.season_id = s.id
     JOIN leagues l ON s.league_id = l.id
     CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'bowling') AS bowl
     JOIN players p ON (bowl->>'player_id')::int = p.id
     WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]' GROUP BY 1
     
     UNION ALL
     
     -- Fielding Points (Catches & Stumpings)
     SELECT p.fullname, 
            COUNT(*) * 2.5
     FROM fixtures f JOIN seasons s ON f.season_id = s.id
     JOIN leagues l ON s.league_id = l.id
     CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'batting') AS bat
     JOIN players p ON (bat->>'catch_stump_player_id')::int = p.id
     WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]' 
       AND (bat->>'catch_stump_player_id') IS NOT NULL
     GROUP BY 1
   ) stats 
   GROUP BY 1 
    ORDER BY 2 DESC 
    LIMIT 10;
    ```

9. **Highest and Lowest Team Totals**:
   ```sql
   -- LOGIC: Use jsonb_array_elements on 'scoreboards' to find team totals.
   -- IMPORTANT: Filter total > 0 to avoid unplayed/cancelled matches.
   -- ALSO retrieve batting/bowling data to explain the high score.
   SELECT f.name as match, (sb->>'total')::int as total_score, t.name as team,
          f.winner_team_id,
          f.raw_json->'batting' as batting_data,
          f.raw_json->'bowling' as bowling_data,
          f.raw_json->'scoreboards' as score_summary
   FROM fixtures f
   JOIN seasons s ON f.season_id = s.id
   JOIN leagues l ON s.league_id = l.id
   CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'scoreboards') AS sb
   JOIN teams t ON (sb->>'team_id')::int = t.id
   WHERE (l.code = 'IPL' OR l.name ILIKE '%Indian Premier League%') 
     AND s.year = '2025' AND f.status = 'Finished' AND (sb->>'total')::int > 0
   ORDER BY total_score ASC  -- Use ASC for lowest, DESC for highest
   LIMIT 5;
   ```


### ⚠️ CRITICAL RULES:
- **No Hardcoding**: Examples use placeholders like `[Tournament]`. Replace with user's actual request.
- **Tournament Identification**: ALWAYS JOIN `leagues` table on `s.league_id = l.id`.
  - Use flexible filtering: `(l.code = 'IPL' OR l.name ILIKE '%Indian Premier League%')`.
- **Year Filter**: `seasons.year` is TEXT. Use `s.year = '2025'`.
- **JSONB Joins**: ALWAYS JOIN with `players` table for names. ID is `(bat->>'player_id')::int`.
- **Award Priority**: Check `season_awards` FIRST for winner awards (Orange/Purple Cap), fall back to `fixtures` calculation if empty.
- **Winner logic**: Match Winner is `winner_team_id`. ALWAYS JOIN `teams` table (`LEFT JOIN teams t ON f.winner_team_id = t.id`) to select `t.name as winner_name`. This is mandatory to prevent reversing results.
    LIMIT 10;
    ```

   13. **Highest Individual Score**:
    ```sql
    -- LOGIC: Find the single highest score in an innings.
    SELECT p.fullname as player, (bat->>'score')::int as score, (bat->>'ball')::int as balls, f.name as match
    FROM fixtures f
    JOIN seasons s ON f.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'batting') AS bat
    JOIN players p ON (bat->>'player_id')::int = p.id
    WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
    ORDER BY score DESC
    LIMIT 5;
    ```

   14. **Fastest Fifty (Estimate)**:
    ```sql
    -- LOGIC: Find scores >= 50 with lowest balls faced.
    SELECT p.fullname as player, (bat->>'score')::int as score, (bat->>'ball')::int as balls, f.name as match
    FROM fixtures f
    JOIN seasons s ON f.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'batting') AS bat
    JOIN players p ON (bat->>'player_id')::int = p.id
    WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%') AND s.year = '[Year]'
      AND (bat->>'score')::int >= 50
    ORDER BY balls ASC
    LIMIT 5;
    ```
- **Output**: ONLY the SQL query. No markdown. No comments.

   15. **Multi-Year/Trend Analysis**:
    ```sql
    -- LOGIC: For multi-year trends (e.g., "Compare 2023 vs 2025"), ONLY query the years present in DB (2024, 2025 onwards).
    -- DO NOT fail if 2023 is missing. The AI Presenter will fill in 2023 from internal knowledge.
    SELECT s.year, COUNT(f.id) as matches, AVG((sb->>'total')::int) as avg_score
    FROM fixtures f
    JOIN seasons s ON f.season_id = s.id
    JOIN leagues l ON s.league_id = l.id
    CROSS JOIN LATERAL jsonb_array_elements(f.raw_json->'scoreboards') AS sb
    WHERE (l.code = '[TournamentCode]' OR l.name ILIKE '%[TournamentName]%')
      AND (s.year = '2024' OR s.year = '2025') -- LIMIT TO DB YEARS
      AND f.status = 'Finished' AND (sb->>'total')::int > 0
    GROUP BY s.year
    ORDER BY s.year;
    ```
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
        
        
        # GUARDRAIL: Prevent 'scorecard' key hallucination (with variations)
        # We replace the specific selection with a broader one that we can process
        if "scorecard" in sql.lower():
            logger.info("🛡️ GUARDRAIL: Scorecard hallucination detected in SQL. Correcting...")
            # Replace arrow access variants
            for arrow in ["->'scorecard'", "-> 'scorecard'", "-> \"scorecard\"", "->\"scorecard\""]:
                if arrow in sql:
                    sql = sql.replace(arrow, "->'scoreboards'") # Fallback to summary
            
            # If it's used as a column name, we might need the whole raw_json to decode it
            if "raw_json" in sql and "scorecard" in sql:
                # If they did SELECT f.raw_json->'scorecard', we want more.
                # A safe bet is to just select the whole raw_json as raw_json
                import re
                sql = re.sub(r"([a-z0-9_]+\.)?raw_json\s*->\s*['\"]scorecard['\"]", r"\1raw_json", sql, flags=re.IGNORECASE)

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
    
    
    # 4. Wrap for Research Agent
    evidence = engine.build_evidence_pack(result, user_query)
    evidence["executed_sql"] = result.get("sql") or (sql if result["status"] == "success" else None)
    return evidence
