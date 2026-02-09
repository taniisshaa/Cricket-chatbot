# Cricket Database Structure - Complete Guide

## Database: `full_raw_history.db`

### Main Tables

#### 1. `fixtures` (Match Data)
**Columns:**
- `id` (INTEGER) - Unique match ID
- `season_id` (INTEGER) - Links to seasons table
- `name` (TEXT) - Match name (e.g., "Royal Challengers Bengaluru vs Punjab Kings")
- `starting_at` (TEXT) - Match start time
- `status` (TEXT) - Match status ('Finished', 'Live', 'Upcoming')
- `venue_id` (INTEGER) - Links to venues table
- `winner_team_id` (INTEGER) - Winning team ID
- `toss_won_team_id` (INTEGER) - Toss winner team ID
- `man_of_match_id` (INTEGER) - Man of match player ID
- **`raw_json` (TEXT)** - **MOST IMPORTANT** - Contains ALL match details in JSON format

---

## Understanding `raw_json` Structure

The `raw_json` column contains a JSON object with these main arrays:

### 1. **scoreboards** Array (Team Scores)
Each match has 4 scoreboard entries:

```json
{
  "scoreboards": [
    {
      "type": "extra",      // Extras for innings 1
      "scoreboard": "S1",
      "wide": 9,
      "bye": 0,
      "leg_bye": 0
    },
    {
      "type": "total",      // ✅ TEAM 1 TOTAL SCORE
      "scoreboard": "S1",   // S1 = First innings
      "total": 190,         // Total runs
      "wickets": 9,         // Wickets lost
      "overs": 20           // Overs bowled
    },
    {
      "type": "extra",      // Extras for innings 2
      "scoreboard": "S2"
    },
    {
      "type": "total",      // ✅ TEAM 2 TOTAL SCORE
      "scoreboard": "S2",   // S2 = Second innings
      "total": 184,
      "wickets": 7,
      "overs": 20
    }
  ]
}
```

**How to Extract Team Scores:**
```sql
-- Team 1 Score (First Innings)
SELECT sb.value ->> '$.total' 
FROM json_each(f.raw_json, '$.scoreboards') AS sb 
WHERE sb.value ->> '$.type' = 'total' 
AND sb.value ->> '$.scoreboard' = 'S1'

-- Team 2 Score (Second Innings)
SELECT sb.value ->> '$.total' 
FROM json_each(f.raw_json, '$.scoreboards') AS sb 
WHERE sb.value ->> '$.type' = 'total' 
AND sb.value ->> '$.scoreboard' = 'S2'
```

---

### 2. **batting** Array (Player Batting Stats)

```json
{
  "batting": [
    {
      "scoreboard": "S1",        // Which innings
      "player_id": 46,
      "score": 43,               // Runs scored
      "ball": 35,                // Balls faced
      "four_x": 3,               // Fours hit
      "six_x": 0,                // Sixes hit
      "rate": 123,               // Strike rate
      "batsman": {
        "fullname": "Virat Kohli",
        "battingstyle": "right-hand-bat"
      }
    }
  ]
}
```

**How to Extract Top Batsmen:**
```sql
SELECT 
  bat.value ->> '$.batsman.fullname' as player_name,
  CAST(bat.value ->> '$.score' AS INTEGER) as runs,
  CAST(bat.value ->> '$.ball' AS INTEGER) as balls,
  CAST(bat.value ->> '$.four_x' AS INTEGER) as fours,
  CAST(bat.value ->> '$.six_x' AS INTEGER) as sixes
FROM json_each(f.raw_json, '$.batting') AS bat
WHERE bat.value ->> '$.scoreboard' = 'S1'
ORDER BY CAST(bat.value ->> '$.score' AS INTEGER) DESC
LIMIT 5
```

---

### 3. **bowling** Array (Player Bowling Stats)

```json
{
  "bowling": [
    {
      "scoreboard": "S1",        // Bowled in which innings
      "player_id": 4880,
      "overs": 4,                // Overs bowled
      "runs": 40,                // Runs conceded
      "wickets": 3,              // Wickets taken
      "wide": 2,                 // Wides bowled
      "rate": 10,                // Economy rate
      "bowler": {
        "fullname": "Arshdeep Singh",
        "bowlingstyle": "left-arm-fast-medium"
      }
    }
  ]
}
```

**How to Extract Top Bowlers:**
```sql
SELECT 
  bowl.value ->> '$.bowler.fullname' as player_name,
  CAST(bowl.value ->> '$.wickets' AS INTEGER) as wickets,
  CAST(bowl.value ->> '$.runs' AS INTEGER) as runs,
  CAST(bowl.value ->> '$.overs' AS INTEGER) as overs
FROM json_each(f.raw_json, '$.bowling') AS bowl
WHERE bowl.value ->> '$.scoreboard' = 'S1'
ORDER BY CAST(bowl.value ->> '$.wickets' AS INTEGER) DESC
LIMIT 5
```

---

### 4. Other Useful Fields in `raw_json`

```json
{
  "note": "Royal Challengers Bengaluru won by 6 runs",
  "tosswon": {
    "name": "Royal Challengers Bengaluru"
  },
  "manofmatch": {
    "fullname": "Virat Kohli"
  },
  "round": "Final"  // or "League" or "Qualifier 1", etc.
}
```

---

## Complete Scorecard Query Example

```sql
-- Get full scorecard for IPL 2025 final
SELECT 
  f.name as match_name,
  f.raw_json ->> '$.note' as result,
  f.raw_json ->> '$.tosswon.name' as toss_winner,
  f.raw_json ->> '$.manofmatch.fullname' as man_of_match,
  
  -- Team 1 Score
  (SELECT sb.value ->> '$.total' 
   FROM json_each(f.raw_json, '$.scoreboards') AS sb 
   WHERE sb.value ->> '$.type' = 'total' 
   AND sb.value ->> '$.scoreboard' = 'S1') as team1_score,
  (SELECT sb.value ->> '$.wickets' 
   FROM json_each(f.raw_json, '$.scoreboards') AS sb 
   WHERE sb.value ->> '$.type' = 'total' 
   AND sb.value ->> '$.scoreboard' = 'S1') as team1_wickets,
  
  -- Team 2 Score
  (SELECT sb.value ->> '$.total' 
   FROM json_each(f.raw_json, '$.scoreboards') AS sb 
   WHERE sb.value ->> '$.type' = 'total' 
   AND sb.value ->> '$.scoreboard' = 'S2') as team2_score,
  (SELECT sb.value ->> '$.wickets' 
   FROM json_each(f.raw_json, '$.scoreboards') AS sb 
   WHERE sb.value ->> '$.type' = 'total' 
   AND sb.value ->> '$.scoreboard' = 'S2') as team2_wickets

FROM fixtures f
JOIN seasons s ON f.season_id = s.id
WHERE s.year = '2025' 
AND s.name LIKE '%Indian Premier League%'
AND f.status = 'Finished'
ORDER BY f.starting_at DESC 
LIMIT 1;
```

**Result:**
```
match_name: Royal Challengers Bengaluru vs Punjab Kings
result: Royal Challengers Bengaluru won by 6 runs
toss_winner: Royal Challengers Bengaluru
man_of_match: Virat Kohli
team1_score: 190
team1_wickets: 9
team2_score: 184
team2_wickets: 7
```

---

## Other Tables

### 2. `seasons`
- `id`, `league_id`, `name`, `year`, `code`, `raw_json`

### 3. `teams`
- `id`, `name`, `code`, `raw_json`

### 4. `players`
- `id`, `fullname`, `dateofbirth`, `batting_style`, `bowling_style`, `country_id`, `raw_json`

### 5. `venues`
- `id`, `name`, `city`, `capacity`, `raw_json`

### 6. `season_awards`
- `id`, `season_id`, `award_type`, `player_id`, `player_name`, `team_name`, `stats`
- Used for Orange Cap, Purple Cap, MVP

---

## Key Insights

1. **NO separate scoreboards table** - All scorecard data is in `fixtures.raw_json`
2. **Use json_each()** to iterate through arrays in raw_json
3. **Filter by type='total'** to get team scores (not type='extra')
4. **S1 = First innings, S2 = Second innings**
5. **All player stats** (batting, bowling) are in raw_json arrays
6. **Match summary** is in raw_json.note field

---

## Common Mistakes to Avoid

❌ **WRONG**: `SELECT * FROM scoreboards WHERE match_id = 123`
- There is NO scoreboards table!

❌ **WRONG**: `json_extract(f.raw_json, '$.runs[0].score')`
- There is NO runs array with scores!

✅ **CORRECT**: `json_each(f.raw_json, '$.scoreboards')` with filter `type='total'`

---

## Testing Queries

To test if data exists:
```sql
-- Check if IPL 2025 matches have scorecard data
SELECT 
  f.name,
  json_extract(f.raw_json, '$.scoreboards') IS NOT NULL as has_scoreboards,
  LENGTH(json_extract(f.raw_json, '$.scoreboards')) as scoreboard_count
FROM fixtures f
JOIN seasons s ON f.season_id = s.id
WHERE s.year = '2025' AND s.name LIKE '%IPL%'
LIMIT 5;
```
