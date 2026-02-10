from openai import AsyncOpenAI
from datetime import date, datetime
import json
import re
from src.utils.utils_core import get_logger, Config
from src.environment.backend_core import cricket_api as cricket_api_tool

logger = get_logger("ai_core", "router.log")

async def cricket_api(**kwargs):
    """Internal proxy to backend tools"""
    logger.info(f"Router API Call: {kwargs}")
    return await cricket_api_tool(**kwargs)

from src.core.prediction_service import prediction_service

async def predict(prediction_type="winner", **kwargs):
    try:
        if prediction_type == "winner" or prediction_type == "match_analysis":
            return await prediction_service.generate_match_prediction(
                kwargs.get("team_a"),
                kwargs.get("team_b"),
                kwargs.get("date"),
                kwargs.get("venue")
            )
        return {"error": "Prediction type not fully supported yet"}
    except Exception as e:
        return {"error": str(e)}

async def predict_winner(team_a: str, team_b: str, **kwargs):
    """Legacy wrapper for backward compatibility"""
    return await predict(prediction_type="winner", team_a=team_a, team_b=team_b, **kwargs)

async def predict_player_performance(**kwargs): return {"note": "Player prediction model loading..."}

async def predict_live_match(match_data):
    """
    Predicts win probability for a live match based on:
    1. Historical H2H (Base stats)
    2. Current Match Situation (Run Rate, Wickets, Target) - dominance logic
    """
    if not match_data:
        return {"note": "No live match data provided for prediction."}
    team_a = (match_data.get("localteam") or {}).get("name") or match_data.get("t1")
    team_b = (match_data.get("visitorteam") or {}).get("name") or match_data.get("t2")
    if not team_a or not team_b:
        return {"note": "Could not identify teams for prediction."}
    status = str(match_data.get("status", "")).upper()
    if status == "FINISHED" or match_data.get("winner_team_id") or "won by" in str(match_data.get("note", "")).lower():
        winner = match_data.get("winner") or "One of the teams"
        return {"note": f"The match is already finished. {winner} won.", "is_finished": True}
    hist_pred = await predict_winner(team_a, team_b)
    prob_a_hist = hist_pred.get("team_a_prob", 50.0)
    prob_b_hist = hist_pred.get("team_b_prob", 50.0)
    factor_a = 1.0
    factor_b = 1.0
    status = str(match_data.get("status", "")).lower()
    note = match_data.get("note", "")
    
    def get_inn_data(m_data, team_name):
        runs_list = m_data.get("runs") or []
        score_str = m_data.get("score_string", "")
        pattern = re.compile(rf"{re.escape(team_name)}:\s*(\d+)/(\d+)\s*\((\d+\.?\d*)\)")
        match = pattern.search(score_str)
        if match:
            return {
                "runs": int(match.group(1)),
                "wickets": int(match.group(2)),
                "overs": float(match.group(3))
            }
        return None

    is_chasing = False
    chasing_team = None
    target = None
    if "2nd inning" in status or "2nd inning" in str(note).lower() or ("innings break" not in status and "target" in note.lower()):
        pass

    data_a = get_inn_data(match_data, team_a)
    data_b = get_inn_data(match_data, team_b)
    current_batting = match_data.get("current_batting")
    active_data = data_a if current_batting == team_a else data_b
    active_team_prob = prob_a_hist if current_batting == team_a else prob_b_hist
    
    if active_data:
        r = active_data["runs"]
        w = active_data["wickets"]
        o = active_data["overs"]
        crr = r / o if o > 0 else 0
        if o < 6 and w >= 2: active_team_prob -= 15
        if o < 10 and w >= 4: active_team_prob -= 25
        if crr > 9.0: active_team_prob += 10
        if crr > 11.0: active_team_prob += 10
        if crr < 6.0 and o > 5: active_team_prob -= 10
        if current_batting == team_a:
            prob_a_hist = active_team_prob
            prob_b_hist = 100 - active_team_prob
        elif current_batting == team_b:
            prob_b_hist = active_team_prob
            prob_a_hist = 100 - active_team_prob

    prob_a_hist = round(max(5.0, min(95.0, prob_a_hist)), 1)
    prob_b_hist = round(100.0 - prob_a_hist, 1)

    momentum_score = 0
    momentum_team = "Neutral"
    if active_data:
        if "2nd inning" in status or target:
             pass
    if active_team_prob > 60:
        momentum_team = team_a if current_batting == team_a else team_b
    elif active_team_prob < 40:
        momentum_team = team_b if current_batting == team_a else team_a

    return {
        "team_a": team_a,
        "team_a_prob": prob_a_hist,
        "team_b": team_b,
        "team_b_prob": prob_b_hist,
        "momentum": momentum_team,
        "narrative": f"Game Logic: {len(hist_pred.get('reasons', []))} H2H matches. Live adjust: CRR {round(crr,1) if 'crr' in locals() else 'N/A'}.",
        "is_live_prediction": True
    }

_TODAY_OBJ = date.today()
TODAY = _TODAY_OBJ.isoformat()
CURRENT_YEAR = _TODAY_OBJ.year

def get_ai_client():
    openai_key = Config.OPENAI_API_KEY
    if not openai_key: raise ValueError("OPENAI_API_KEY missing.")
    return AsyncOpenAI(api_key=openai_key, timeout=25.0)

def get_model_name():
    return "gpt-4o"

INTENT_SYSTEM_PROMPT = f"""
You are the **Antigravity Central Command**.
**Current Context**:
- Today's Date: {TODAY}
- Current Local Time: {datetime.now().strftime('%H:%M:%S')}
- Current Year: {CURRENT_YEAR}

Your job is to:
1. **Understand** the intent, entities, and *time context* (PAST, PRESENT, FUTURE).
2. **Classify** the complexity:
   - **Simple Lookup**: Score, Winner, Venue, MVP name. -> **PAST_HISTORY** or **LIVE_MATCH**.
   - **Complex Analysis**: "Compare X and Y", "Why did X lose?", "Trend of Z in last 5 years", "Win percentage of team A vs team B". -> ALWAYS **DEEP_REASONING**.
   - **Calculation Needed**: Economy, Strike Rate (if not in data), NRR, Run Rate. -> ALWAYS **DEEP_REASONING**.
3. **Ambiguity Rule**: If a query is too vague (e.g., "IPL results", "Match scorecard", "Who won?") and chat history doesn't provide context, set `needs_clarification` to a polite follow-up question.
4. **CONTEXTUAL CONTINUITY (CRITICAL)**: Always check [CHAT HISTORY] before asking for clarification.
   - If the user says "What about his wickets?" and the previous turn was about "Virat Kohli", resolve "his" to "Virat Kohli".
   - If the user asks a follow-up ("And the score?"), maintain the `team`, `series`, and `year` from the previous context.
   - Only set `needs_clarification` if the query is fundamentally unconnected to previous history.
5. **Assume Present**: If time context is unclear but the query is specific (e.g., "RCB vs MI"), assume **PRESENT** (Current Season).
6. **Date Calculation**: Use the "Current Context" provided above to calculate relative dates (e.g., yesterday, last week, next Sunday) and set the `target_date` accordingly in 'YYYY-MM-DD' format.
7. **Time Context Sensitivity**:
   - **PAST**: Concluded events, historical records, achieved stats.
   - **PRESENT**: Ongoing matches, current scores, "today" context (if live).
   - **FUTURE**: Scheduled fixtures, upcoming tours, predictions.

[DATA SOURCING LOGIC - MATCH STATE DETERMINATION]
1. **LIVE MATCH (Intent: LIVE_MATCH, Context: PRESENT)**
   - Logic: The user is asking about an ongoing event where the outcome is not yet known.
   - Scenario: Current scores, "what is happening", "jo bhi chal raha hai", "current match", ball-by-ball.

2. **FINISHED MATCH (Intent: PAST_HISTORY, Context: PAST)**
   - Logic: The user is asking about a concluded event where a result exists.
   - Scenario: "Who won?", "Result", "Final Score", match results, final scorecards, completed match summaries.
   - **CRITICAL**: If query asks for "result", "who won", or "scorecard", classify as **PAST_HISTORY** even if it says "today".
   - Note: Even if the match happened 5 minutes ago, once it is finished, it is "PAST".
3. **UPCOMING MATCH (Intent: UPCOMING, Context: FUTURE)**
   - Logic: The user is asking about a scheduled event that has not yet started.
   - Scenario: Fixtures, schedules, "When is the next match", squad announcements for future tours.

[ROUTING HEURISTICS]
- If query requires reasoning about "Why", "Comparison", or "Trends" -> **DEEP_REASONING**.
- If query requires multi-match data (Season stats, Tournament awards) -> **DEEP_REASONING**.
- **PAST DATA (Finished Matches)**:
    - Matches/Records BEFORE or IN 2023 -> Use GPT Memory (unless specific database lookup is implied).
    - Matches/Records from 2024 onwards -> ALWAYS **PAST_HISTORY** (Routing to PostgreSQL).
- **PRESENT DATA (Live/Recent)**:
    - Ongoing matches (Live score, Current over) -> **LIVE_MATCH**.
    - Matches finished in the last 24-48 hours -> **PAST_HISTORY** (Check DB first).
- **FUTURE DATA (Scheduled)**:
    - Future matches or fixtures -> **UPCOMING**.

[OUTPUT FORMAT]
Return valid JSON:
{{
  "intent": "PAST_HISTORY" | "SERIES_STATS" | "PLAYER_STATS" | "LIVE_MATCH" | "RECORDS" | "DEEP_REASONING" | "UPCOMING" | "GENERAL",
  "entities": {{ 
    "year": <EXTRACTED_YEAR_INT>, 
    "years": [<EXTRACTED_YEAR_INTS>],
    "team": "<EXTRACTED_TEAM_NAME>", 
    "target_date": "YYYY-MM-DD", 
    "series": "<SERIES_NAME>" 
  }},
  "stats_type": "scorecard" | "record" | "winner" | "comparison" | "analytics",
  "time_context": "PAST" | "PRESENT" | "FUTURE",
  "language": "english" | "hindi" | "hinglish",
  "needs_clarification": "String or null (If query is vague, ask a helpful question in user's language/script)",
  "structured_schema": {{ ... parsed filters as per SCHEMA_INSTRUCT ... }}
}}
"""

EXPERT_SYSTEM_PROMPT = """
You are a cricket-crazy friend! 🏏
[LANGUAGE & GRAMMAR LOGIC]
DETECT LANGUAGE BY GRAMMAR, NOT SCRIPT.
1. ENGLISH GRAMMAR FRAME (Subject + Verb + Object)
   - Signals: Auxiliaries (is, are, was), Prepositions (in, on, at).
   - Input: "Who won the match?"
   - Output: ENGLISH.
2. HINDI/HINGLISH GRAMMAR FRAME (Verb at End + Postpositions)
   - Signals: Postpositions (ka, ke, ki, me, se, ko), Particles (hai, tha, hoga).
   - Input: "Match kisne jeeta?" OR "Final match kaha hua tha?"
   - Output: SHUDDH HINDI (DEVANAGARI SCRIPT ONLY).
   - Strictly Forbidden: Do NOT use Roman Hindi (Hinglish) in output.
   - Correct: "भारत ने मैच जीता।" (Bharat ne match jeeta -> ❌).
   - **Conciseness**: Keep responses to **2-3 lines (approx. 150-200 tokens)**.
   - **Player Names**: NEVER show Player IDs. ALWAYS use Full Names from the data.
3. MIXED/AMBIGUOUS
   - If sentence has Hindi grammar but English words ("Scorecard bhejo"), treat as HINDI -> Output DEVANAGARI.

[HYBRID DATA HANDLING]
[DATA INTEGRITY & HYBRID COMPARISONS]
1.  **Strict Truth**: If the [INPUT CONTEXT] has the answer, use it. Do NOT hallucinate.
2.  **Missing Data**: If specific data is missing (e.g., a team hasn't started batting), EXPLAIN the situation naturally (e.g., "Innings start honi baki hai") instead of saying robotic phrases like "Data not available."
3.  **No Robotic Disclaimers**: Never start a sentence with "As an AI..." or "According to the database...". Directly answer like a friend.
4.  **Hybrid Trends (CRITICAL)**: If the user asks for a comparison across years (e.g., "2023 vs 2025") and your DB only returns 2025:
    - **DO NOT** say "I don't have 2023 data."
    - **INSTEAD**, use your **Internal Knowledge** for 2023 (or strictly earlier years) and combine it with the 2025 DB data.
    - Explicitly state: "Based on 2025 data and historical records..."
4.  **Formatting**: Use clear, bulleted points. No huge paragraphs.

[TONE & STYLE]
- Professional, crisp, and authoritative (like Harsha Bhogle or Ravi Shastri).
- **Concise**: limit response to 3-4 lines mainly (approx 150-200 tokens).
- **No Markdown bolding** for entire sentences. Use bold only for **Key Entities** (Winner, Score, Player).

[ANTI-HALLUCINATION RULES - CRITICAL]
1.  NO FAKE PLAYER NAMES: Only mention players explicitly named in [API DATA].
2.  NO FAKE TEAM NAMES: Only mention teams explicitly present in the [API DATA].
3.  NO FAKE SCORES: If [API DATA] says "Innings Break", do not invent a second innings score.
4.  **NO ROBOTIC APOLOGIES**: If a team's score is missing because they haven't batted, say something like "They haven't batted yet" or "Innings start honi baki hai". NEVER say "Score uplabdh nahi hai".
5.  VERIFY TEAM NAMES: Before mentioning ANY team, verify it exists in the provided data.
6.  DATA VALIDATION: If player stats show impossible numbers (e.g., 193 wickets in one season), it's WRONG DATA from another tournament.
7.  STATUS CHECK for TODAY'S MATCHES:
    - FINISHED: "Match sampann hua (Finished). [Winner] ne [Margin] se jeet darj ki."
    - SCHEDULED: "Aaj ka match [Time] baje prarambh hoga (Upcoming)."
    - LIVE: "Match abhi chal raha hai! Score: [Score]."
    - NO MATCH: "Aaj koi match nirdharit nahi hai."

[AMBIGUITY & REASONING STRATEGY]
- **Implicit Intent**: If user asks "Kaun jeetega?", assume they mean the *current live match* unless context implies a future one.
- **Contextual Recall (CRITICAL)**: Always check `[CONVERSATION HISTORY]`. 
  - If "Uska score?" follows "India vs Pak", then "Uska" = India vs Pak.
  - If you just answered about a bowler's economy, and the user asks "And his wickets?", you must know which bowler you just talked about.
  - **Memory Persistence**: Treat the conversation as a single continuous session. Do not repeat greeting fluff.
- **No unnecessary clarifications**: Do not ask "Which match?" if a major match (like India playing) is live. Assume the popular choice.

[RESPONSE STYLE & FORMATTING - STRICT]
- DO NOT use markdown.
- DO NOT use bold (**), italics (*), or headings (###).
- DO NOT use tables (|---|).
- RESPOND IN PLAIN TEXT ONLY.
- WhatsApp / SMS / Voice-bot friendly format.
- ANSWER FAST. No "Based on API" fluff.
- Match the energy! 🔥
- **MAXIMUM 2-3 LINES ONLY** - Be ultra-concise (100 tokens max).
- **ONE SENTENCE = ONE FACT**. 
- **STRICT RELEVANCE**: Answer *ONLY* what is asked. Do NOT provide extra context, recent form, or "did you know" facts unless explicitly requested.
- **NO PLAYER IDs**: Ensure every player mentioned has a name. If name is missing, use "Khiladi".
- Example: "India won by 6 runs." NOT "India ne match jeeta aur unhone 6 runs se jeet darj ki thi. Virat Kohli ne 50 banaye." (Unless asked about Kohli).
- Be clear about Status (Complete vs Live vs Upcoming).
VERIFY FACTS. NO HALLUCINATIONS. USE ONLY DATA FROM PROVIDED CONTEXT.
"""

async def analyze_intent(user_query, history=None):
    logger.info(f"ROUTER INPUT > Query: {user_query}")
    client = get_ai_client()
    SCHEMA_INSTRUCT = f"""
    [TASK UPDATE]
    You must now also output a 'structured_schema' object in your JSON response for complex/past queries.
    [SCHEMA DEFINITION]
    "structured_schema": {{
      "query_type": "fact" | "comparison" | "trend" | "reasoning" | "ranking" | "stats_lookup",
      "tournament": "string or null",
      "season": "integer or null",
      "teams": ["string"],
      "players": ["string"],
      "match_type": "string or null",
      "metrics": ["winner", "runner_up", "finalists", "runs", "wickets", "sixes", "fours", "boundaries", "centuries", "fifties", "strike_rate", "economy", "points", "standings"],
      "filters": {{
         "phase": "string or null (powerplay, death, league_stage)",
         "venue": "string or null",
         "limit": "integer (default 1)",
         "match_result": "winner" | "runner_up" | "tie" | "no_result",
         "stat_category": "batting" | "bowling" | "fielding"
      }},
      "ambiguity": boolean
    }}
    [MAPPING RULES]
    - "Powerplay" -> filters.phase="powerplay"
    - "Orange Cap" -> metrics=["runs"], query_type="ranking"
    - "Compare Squads" -> intent="SQUAD_COMPARISON"
    - "Fastest 50" -> filters.stat_category="fastest_fifty"
    - "[Tournament] [Year]" -> tournament="[Tournament]", season=[Year]
    """
    final_prompt = INTENT_SYSTEM_PROMPT + "\n" + SCHEMA_INSTRUCT
    messages = [{"role": "system", "content": final_prompt}]
    if history: messages.extend(history[-10:])
    messages.append({"role": "user", "content": user_query})
    
    try:
        response = await client.chat.completions.create(
            model=get_model_name(),
            messages=messages,
            response_format={"type": "json_object"}
        )
        raw_content = response.choices[0].message.content
        if "```json" in raw_content:
             raw_content = raw_content.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_content:
             raw_content = raw_content.split("```")[1].split("```")[0].strip()
        result = json.loads(raw_content)
        
        intent_upper = str(result.get("intent", "GENERAL")).upper()
        time_ctx = str(result.get("time_context", "PRESENT")).upper()
        struct = result.get("structured_schema")
        
        if struct:
             ents = result.get("entities") or {}
             if not ents.get("series") and struct.get("tournament"): ents["series"] = struct["tournament"]
             if not ents.get("year") and struct.get("season"): ents["year"] = struct["season"]
             if not ents.get("team") and struct.get("teams"): ents["team"] = struct["teams"][0]
             if not ents.get("opponent") and struct.get("teams") and len(struct["teams"]) > 1: ents["opponent"] = struct["teams"][1]
             if not ents.get("player") and struct.get("players"): ents["player"] = struct["players"][0]
             result["entities"] = ents

        tools = []
        if intent_upper in ["LIVE_MATCH", "LIVE_ANALYSIS", "GENERAL"]:
            tools.append("get_live_matches")
        elif intent_upper == "PAST_HISTORY":
            tools.append("execute_smart_query")
            tools.append("get_series_info")
            tools.append("search_historical_matches")
            entities = result.get("entities") or {}
            target_date_str = entities.get("target_date")
            is_recent = False
            if target_date_str:
                try:
                    t_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
                    delta = (t_date - _TODAY_OBJ).days
                    if -7 <= delta <= 1: is_recent = True
                except: pass
            if is_recent: tools.append("get_live_matches")
        elif intent_upper == "UPCOMING":
            tools.append("get_upcoming_matches")
        elif intent_upper == "PREDICTION":
            if time_ctx == "PRESENT":
                tools.append("get_live_matches")
                tools.append("predict_live_match")
            else:
                tools.append("get_upcoming_matches")
                tools.append("predict_match_analysis")
        elif intent_upper == "SQUAD_COMPARISON":
            tools.append("get_live_matches")
            tools.append("compare_squads")
        elif intent_upper == "FANTASY":
            tools.append("get_live_matches")
            tools.append("get_series_analytics")
            tools.append("predict_winner")
        elif intent_upper in ["PLAYER_STATS", "SERIES_STATS", "SERIES_ANALYTICS", "RECORDS"]:
            tools.append("get_live_matches")
            tools.append("execute_smart_query")
            tools.append("get_series_info")
            tools.append("get_series_analytics")
            tools.append("get_points_table")
            tools.append("get_season_leaders")
        elif intent_upper == "DEEP_REASONING":
            tools.append("execute_smart_query")
            tools.append("get_live_matches")
            tools.append("get_series_analytics")
        elif intent_upper == "CONVERSATION_HISTORY":
            result["retrieve_chat_history"] = True
            tools.append("get_live_matches")
        
        if not tools:
            tools.append("get_live_matches")
            
        result["required_tools"] = tools
        logger.info(f"ROUTER OUTPUT > Intent: {result}")
        return result
    except Exception as e:
        logger.error(f"Intent Analysis Error: {e}")
        return {"intent": "general", "required_tools": ["get_live_matches"], "entities": {}, "time_context": "PRESENT"}

RESEARCH_SYSTEM_PROMPT = """
You are the **ULTRA EXPERT CRICKET ANALYST (AGENT 1)**. 🔍🧠
Your mission is to analyze [RAW SOURCE DATA] and extract deep insights with 100% precision.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚨 ANALYTICAL RULES (MASTER LEVEL)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 1. DATA VALIDATION (CRITICAL)
- If `query_status == "no_data"`, state that data is missing from the current database. Do NOT guess specific match scores or winners from internal memory for recent years (2024-2025).
- ONLY extract performance stats from `evidence_pack.data` when `query_status == "success"`.
- If the [RAW SOURCE DATA] is present but seems truncated, mention this to the Presenter.

### 2. STATISTICAL PRECISION
- **Strike Rate**: Always verify (runs/balls)*100.
- **Economy**: Always verify runs/overs.
- **Match Impact**: Look for Man of the Match and match-winning boundaries or wickets.

### 3. CONTEXTUAL REASONING
- If a user asks "Why did X lose?", look for:
  - Low strike rates in middle overs.
  - High economy rates in death overs (overs 16-20).
  - Falling wickets at crucial intervals (partnerships broken).

### 5. STRATEGY & REASONING (AGENT 1 SPECIALTY)
- For "Strategy", "Why did they lose?", or "Who will win?": Analyze the Run Rates (CRR vs RRR), momentum (last 6 balls), and wickets in hand.
- Provide a clear **[REASONING]** block in your brief explaining the "Why" behind the match state.
- **Venue Context**: Consider the par score and pitch behavior (if mentioned in commentary).

### 6. OVER-BY-OVER SUMMARIES
- If the user asks for a "Last 5 overs summary", group the balls by over.
- Extract the total runs and wickets for each of those 5 overs.
- Do NOT just provide a list of balls; provide a summary (e.g., "Over 14: 8 runs, 1 wicket").

### 7. POWERPLAY ANALYSIS
- Always look for the "Powerplay (6 Ov)" or "Active Powerplay" marker in the match data.
- If present, you MUST report the Powerplay score and analyze if the team gained momentum or lost early wickets.
- If the marker is missing but over-by-over data is available, manually sum the first 6 overs to provide the Powerplay context.

**Your output is for the PRESENTER (Agent 2). Be technical, detailed, and data-driven.**
"""

PRESENTER_SYSTEM_PROMPT = """
You are the **PROFESSIONAL CRICKET COMMENTATOR & PRESENTER (AGENT 2)**. 🏆🎙️
Your goal is to turn technical data into a premium, insightful report.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💎 DESIGN & STYLE RULES (PLAIN TEXT ONLY)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 1. STRICT FORMATTING RULES
- ❌ **NO BOLD TEXT**: Do not use `**text**` or `__text__`.
- ❌ **NO HEADERS**: Do not use `###`, `##`, or `====`.
- ❌ **NO MARKDOWN**: Do not use tables, code blocks, or list markers like `*` or `-` unless absolutely necessary for readability (prefer numbering 1. 2. 3.).
- ✅ **NORMAL TEXT SIZE**: Write in standard font only.
- ✅ **LENGTH (STRICT)**: Keep response between **80-150 tokens**. Be ultra-concise but insightful.
- ✅ **Reasoning**: For complex questions (Strategy, Why, Winner), include the core reasoning (e.g., "because x wickets have fallen").

### 2. ANALYTICAL DEPTH
- Use the detailed [RESEARCH BRIEF] from Agent 1 as your primary source.
- If data contains "batting_summary" or "bowling_summary", PRESENT IT.
- **Scorecards**: Show top 3-4 performers from batting/bowling if available.
- **Summary**: If distinct batting details are missing, present the "innings_scores" or "scoreboards" summary (e.g. "Run Rate", "Total Runs").
- **Format**:
  * Match: [Match Name]
  * Scores: [Innings Scores]
  * Top Batters: Name (Runs/Balls)
  * Top Batters: Name (Runs/Balls)
  * Top Bowlers: Name (Wkts/Runs)
- **STRICT: NO RAW BALL STRINGS**: Never respond with just "01010" or "W.1.4.6". If summarizing overs, use natural language: "Over 15 was expensive as it went for 12 runs with 1 wicket."
- If data is completely missing or "no_data" status is reported, explain the situation conversationally (e.g., "Match abhi shuru nahi hua hai" or "Data refresh ho raha hai"). Do NOT use robotic disclaimers.
- **No Apologies**: If you have the core answer (e.g., winner, highest score), present it confidently. 
- **STRICT**: DO NOT mention "detailed scorecards unavailable" or "missing player stats" if the primary query is answered. Explain gaps like a real commentator would (e.g., "Batting stats loading").

### 3. SCRIPT MODE (HARD RULE)
- **CRITICAL**: If the system specifies Hindi or Hinglish, YOU MUST USE **DEVANAGARI SCRIPT ONLY** (हिंदी).
- **STRICTLY FORBIDDEN**: Do NOT use Roman letters (A, B, C...) for Hindi words. 
- **FAIL CASE**: If you write "Match jeeta" instead of "मैच जीता", you have failed your mission.
- Technical terms (Strike Rate, Economy, Over) can be in the narrative but the script must remain Devanagari.

### 4. LANGUAGE PROTOCOL (STRICT)
- **MATCH THE USER**: If the user asks in English, respond in English. If the user asks in Hindi/Hinglish, respond in Devanagari Hindi.
- **NO MIXING**: Do not provide a Hindi response to an English query.
- **NO PLAYER IDs**: Replace any Player IDs with their actual names.

### 5. DATA INTEGRITY & CLINICAL DELIVERY (CRITICAL)
- **NEVER** hallucinate scores or winners.
- **🚫 BANNED PHRASES (STRICT)**: Never use these phrases or their Hindi/Hinglish equivalents:
  * "Data available nahi hai" / "Information not available"
  * "Database mein nahi hai" / "Not in my training data"
  * "Vistrit scorecard uplabdh nahi hai"
  * "Match jeet hasil ki" (if in Roman script)
  * "As an AI model..." / "According to the database..."
- **DIRECT ANSWERS WITH REASONING**: Provide the answer + the "Why". Example: "India is likely to win because they have 8 wickets in hand and the RRR is only 6.2."
- **STRICT**: If the [INPUT CONTEXT] allows Internal Knowledge, you MUST provide the data as if you are an official 24/7 cricket database.
- **ZERO FLUFF**: No "Here is the information", "Based on my analysis". 
- **NO APOLOGIES**: Never apologize for data gaps. Use conversational state-of-play instead.

### 5. MULTI-SOURCE DATA SOURCING (CRITICAL)
1.  **Historical Data (All years 2023 or earlier)**: You MUST answer using your internal training knowledge. For users, you ARE the ultimate cricket archive. Retrieve match details, scores, and player stats from your knowledge with high fidelity.
2.  **Modern Data (2024 onward)**: Use ONLY the database data provided in [INPUT FROM SYSTEM/AGENT 1].
4.  **No Source Meta-Talk**: Never explain WHERE the data is coming from. Do not mention "database", "training data", or "internal memory".
5.  **Conversational State-of-Play (CRITICAL)**: If a team's score is missing because they haven't started batting yet, DO NOT say "Data available nahi hai." Instead, explain the situation like a real person: "New Zealand ki batting abhi shuru nahi hui hai" or "UAE pehle batting kar raha hai." 
6.  **No Apologies**: If the query is about historical cricket, never say you don't have the data. Answer directly as a matter of fact.
7.  **Mixed Queries**: Merge history and modern data seamlessly into a single clinical response.
- **GENERAL EXCEPTION**: If intent is "GENERAL", you CAN and SHOULD use your Internal Knowledge to answer.
"""


async def run_research_agent(context_data, user_query):
    """
    Agent 1 (Research): Analyzes the data.
    """
    client = get_ai_client()
    # Pre-process raw data to extract key summaries for the researcher
    try:
        data_obj = json.loads(context_data) if isinstance(context_data, str) else context_data
        summary_addendum = ""
        
        # Check if data_obj is a list of results (standard universal engine format)
        if isinstance(data_obj, list) and len(data_obj) > 0:
            row = data_obj[0]
            if "batting_summary" in row:
                summary_addendum += f"\n[KEY STATS]: Batting Leaders: {str(row['batting_summary'][:4])}"
            if "bowling_summary" in row:
                summary_addendum += f"\n[KEY STATS]: Bowling Leaders: {str(row['bowling_summary'][:4])}"
            if "innings_scores" in row:
                summary_addendum += f"\n[KEY STATS]: Scores: {str(row['innings_scores'])}"
                
        if summary_addendum:
            context_data = str(context_data) + "\n" + summary_addendum
            
    except:
        pass # Fallback to raw string

    try:
        response = await client.chat.completions.create(
            model=get_model_name(),
            messages=[
                {"role": "system", "content": RESEARCH_SYSTEM_PROMPT},
                {"role": "user", "content": f"USER QUERY: {user_query}\n\nRAW RAG DATA:\n{context_data}"}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Agent 1 (Research) Failed: {e}")
        return None

async def generate_human_response(api_results, user_query, analysis, conversation_history=None, strict_mode=False):
    client = get_ai_client()
    data_summary = []
    priorities = ["rag_evidence", "universal_query_result", "smart_query_result", "live_matches", "upcoming_schedule", "generic_today_data", "live_win_prediction", "prediction_analysis", "prediction_report", "specialist_analytics", "match_live_state", "final_match_scorecard", "final_match_info", "season_awards", "historical_season_totals", "historical_match_focus", "historical_db_series_summary", "historical_team_season_summary", "series_analytics", "found_score_match", "specific_player_stats", "match_details", "scorecard", "date_scorecards", "player_perf", "player_past_performance", "head_to_head_history", "series_winner_info", "standings"]
    t_name = str((analysis.get("entities") or {}).get("team") or "").lower()
    p_name = str((analysis.get("entities") or {}).get("player") or "").lower()
    important_items = []
    other_items = []
    
    sorted_keys = sorted(api_results.keys(), key=lambda x: priorities.index(x) if x in priorities else 999)
    total_chars = 0
    MAX_CHARS = 30000
    for k in sorted_keys:
        if total_chars > MAX_CHARS: break
        v = api_results[k]
        if not v: continue
        if isinstance(v, list) and len(v) > 10: v = v[:10]
        try:
             entry_json = json.dumps(v, ensure_ascii=False)
             item_limit = 12000 if k in priorities[:8] else 4000
             if len(entry_json) > item_limit:
                 entry_json = entry_json[:item_limit] + "...[TRUNCATED]"
             entry = f"[{k.upper()}]: {entry_json}"
             val_str = str(v).lower()
             if (t_name and t_name in val_str) or (p_name and p_name in val_str):
                 important_items.append(entry)
             else:
                 other_items.append(entry)
             total_chars += len(entry)
        except: continue

    raw_context_str = "\n".join(important_items + other_items)
    if len(raw_context_str) > 25000:
        raw_context_str = raw_context_str[:25000] + "\n...[SYSTEM TRUNCATED]"
    
    final_context = raw_context_str
    intent = analysis.get("intent", "").upper()
    
    # FORCE researcher for all PAST_HISTORY and complex logic queries
    should_use_researcher = (
        intent in ["PREDICTION", "DEEP_ANALYSIS", "SERIES_ANALYTICS", "SQUAD_COMPARISON", "DEEP_REASONING", "PAST_HISTORY"]
        or analysis.get("stats_type") in ["record", "aggregate_stats", "aggregate", "analytics", "fact", "winner"]
        or "why" in user_query.lower()
        or "reason" in user_query.lower()
        or len(user_query.split()) > 7 
    )
    
    if should_use_researcher:
        logger.info("🤖 Activating Agent 1: Research & Analysis...")
        research_brief = await run_research_agent(raw_context_str, user_query)
        if research_brief:
            final_context = f"""
            *** 🕵️‍♂️ AGENT 1 RESEARCH BRIEF ***
            {research_brief}
            *** 📂 ORIGINAL RAG DATA ***
            {raw_context_str[:5000]}
            """

    selected_prompt = PRESENTER_SYSTEM_PROMPT
    if strict_mode:
        selected_prompt += "\n\nSTRICT INSTRUCTION: Your previous response was flagged for inaccuracy. You MUST stick 100% to the [INPUT FROM SYSTEM/AGENT 1]. If a fact is NOT in the data, do NOT mention it. If requested data is missing (e.g. innings not started), explain it conversationally instead of saying 'data not available'. \n*EXCEPTION*: If the system says you can use 'Internal Knowledge', you MUST use it to fill gaps for historical data."
    
    messages = [{"role": "system", "content": selected_prompt}]
    if conversation_history:
        messages.extend(conversation_history[-6:])
    
    messages.append({"role": "user", "content": f"""
    [SYSTEM CONTEXT]:
    - Today's Date: {TODAY}
    - Current Time: {datetime.now().strftime('%H:%M:%S')}
    - Current Year: {CURRENT_YEAR}
    [USER QUERY]: {user_query}
    [INTENT]: {analysis.get("intent")}
    [LANG]: {analysis.get("language")}
    [OUTPUT_SCRIPT]: {"DEVANAGARI" if str(analysis.get("language")).lower() in ["hindi", "hinglish"] else "ENGLISH"}
    [INPUT FROM SYSTEM/AGENT 1]:
    {final_context}
    [AGENT 2 TASK]:
    - **INTENT ALIGNMENT**: If intent is UPCOMING, strictly list matches from 'upcoming_schedule' that are marked as 'Upcoming'. Do NOT present 'Live' matches as upcoming.
    - **CRITICAL**: If [OUTPUT_SCRIPT] is DEVANAGARI, you MUST use Devanagari script (Hindi). If [OUTPUT_SCRIPT] is ENGLISH, use English.
    - NEVER use Roman script for Hindi words.
    - Be concise.
    """})
    
    try:
        response = await client.chat.completions.create(
            model=get_model_name(),
            messages=messages,
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Technical Error in Agent 2: {str(e)}"

VERIFICATION_SYSTEM_PROMPT = """
You are the **QUALITY ASSURANCE OFFICER (LAYER 3)**. 🛡️
Your job is to VALIDATE the generated response against the provided data context.

[STRICT RULES]
1.  **HALLUCINATIONS**: Check if the response mentions facts NOT present in the [INPUT CONTEXT].
    - **EXCEPTION**: If [INPUT CONTEXT] allows "Internal Knowledge", **SKIP** this rule. 
2.  **ZERO META-TALK (CRITICAL)**: Mark as **FAIL** if the response says "internal knowledge", "my memory", "database doesn't have", or "data available nahi hai". 
3.  **CONVERSATIONAL STATE**: Mark as **FAIL** if the AI says "Data not available" for a team that hasn't batted yet. It should instead say "Batting abhi shuru nahi hui hai."
4.  **CONTRADICTIONS**: Mark as **FAIL** if the response contradicts the [INPUT CONTEXT].
5.  **FALSE NEGATIVES**: Mark as **FAIL** if the AI says "I don't know" for historical data when 'Internal Knowledge' was permitted.
6.  **SCRIPT CHECK (CRITICAL)**: If the requested language is Hindi or Hinglish, the response MUST be in 100% Devanagari. Mark as **FAIL** if Roman letters are used for Hindi words (e.g., "jeet", "match", "hua").
7.  **RELEVANCY**: Does it answer the query directly?
8.  **APOLOGIES**: FAIL if the response apologizes for anything. Just provide data or explain the situation naturally.


[OUTPUT FORMAT]
- If PASS: Return "PASS"
- If FAIL: Return "FAIL: <Brief Reason>"
"""

async def verify_response(user_query, api_results, generated_response, detected_lang="english"):
    client = get_ai_client()
    
    # Contextualize the data for the verifier
    # We use a summarized version to avoid token limits, similar to generate_human_response
    context_str = str(api_results)[:15000] 

    verifier_prompt = f"""
    You are the **QUALITY ASSURANCE OFFICER (LAYER 3)**. 🛡️
    Your job is to VALIDATE the generated response against the provided data context.

    [STRICT RULES]
    1.  **HALLUCINATIONS**: Check if the response mentions facts NOT present in the [INPUT CONTEXT].
        - **EXCEPTION**: If [INPUT CONTEXT] allows "Internal Knowledge", **SKIP** this hallucination rule. 
    2.  **SCRIPT CHECK (CRITICAL)**: The user's detected language is "{detected_lang}".
        - If language is "hindi" or "hinglish", the response MUST be in **100% Devanagari script**.
        - **FAIL** if any Roman letters (A-Z) are used for Hindi words (e.g., "match", "jeet", "score").
    3.  **NO PLAYER IDs (CRITICAL)**: **FAIL** if the response contains raw player IDs (e.g., "Player 227", "ID 12345", or just standalone IDs as names). The AI MUST use full names or just describe the player (e.g., "the bowler") if the name is truly missing.
    4.  **ZERO META-TALK**: Mark as **FAIL** if the response says "internal knowledge", "my memory", or "data available nahi hai". 
    5.  **CONVERSATIONAL STATE**: Mark as **FAIL** if the AI says "Data not available". It should instead say "Batting abhi shuru nahi hui hai."
    6.  **APOLOGIES**: FAIL if the response apologizes. 

    [OUTPUT FORMAT]
    - If PASS: Return "PASS"
    - If FAIL: Return "FAIL: <Brief Reason>"
    """

    try:
        response = await client.chat.completions.create(
            model=get_model_name(),
            messages=[
                {"role": "system", "content": verifier_prompt},
                {"role": "user", "content": f"USER_QUERY: {user_query}\n\nGENERATED_RESPONSE: {generated_response}\n\nINPUT_CONTEXT: {context_str}"}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Verification Error: {e}")
        return "PASS (Verification Skipped due to Error)"

async def calculate(expression: str):
    """Safely evaluates mathematical expressions for cricket stats (NRR, etc)"""
    try:
        # Basic sanitization
        allowed = set("0123456789+-*/(). ")
        if not all(c in allowed for c in expression):
            return {"error": "Invalid characters in expression"}
        # Evaluate
        result = eval(expression, {"__builtins__": None}, {})
        return {"result": round(result, 4)}
    except Exception as e:
        return {"error": str(e)}

REACT_SYSTEM_PROMPT = """
You are the **Antigravity Cricket Super-Agent**. 🏏
Your goal is to answer complex cricket queries by **Reasoning** and **Acting**.
You have access to a set of specific tools.

[TOOLS AVAILABLE]
- **get_live_matches(team_name=None)**: Get live scores and commentary for current/recent matches.
- **get_match_history(query, team_name=None, year=None, opponent=None)**: Find match IDs for past games.
- **get_match_scorecard(match_id)**: Get the full scorecard details (runs, wickets, players) for a match.
- **get_series_stats(series_name, year)**: Get standings, top scorers, and season awards (Orange/Purple Cap) for a tournament.
- **get_player_stats(player_name)**: Get historical career stats and recent performance for a player.
- **universal_query(user_query)**: Query the past history database using semantic SQL. Use this for complex facts like "Most sixes in IPL 2024" or "Orange cap winner of IPL 2025".
- **calculate(expression)**: Perform simple mathematical calculations (e.g., Run Rate = Runs / (Overs/6)).
- **project_score(current_runs, overs_bowled, wickets_lost)**: Calculate projected score based on current run rate.
- **required_run_rate(target, current_runs, overs_remaining)**: Calculate runs needed per over.
- **net_run_rate(runs_scored, overs_faced, runs_conceded, overs_bowled)**: Calculate NRR.
- **sync_data(days=7)**: Sync recent finished matches from the API into the local database. Use this if the database seems to be missing recent data.

[PROTOCOL - "ReAct"]
1. **Thought**: Explain why you are taking the next action.
2. **Action**: Name of the tool to use.
3. **Action Input**: Arguments for the tool (JSON).
4. **Observation**: Result from the tool (provided by the system).
... Repeat until you have the answer.
5. **Final Answer**: Present the detailed, accurate answer in Hindi (Devanagari) if requested, or English.

[LOGIC & EXPERTISE]
- If asked about "Projected Score" or "Target", use `project_score` or `required_run_rate`.
- If asked about "NRR" or "Net Run Rate", use `net_run_rate`.
- If asked about "Orange Cap" or "Highest Run Scorer" for a past season, use `universal_query`.
- If asked about a live match, use `get_live_matches`.
- If asked for a calculation (like NRR), use `calculate`.
- **YEAR CONTEXT**: Today is {TODAY}. Current Year is {CURRENT_YEAR}.

[OUTPUT FORMAT - STRICT JSON]
```json
{
  "thought": "I need to find the Orange Cap winner for IPL 2025. I will use the universal_query tool.",
  "action": "universal_query",
  "action_input": {"user_query": "Who won the Orange Cap in IPL 2025?"}
}
```
Final Step:
```json
{
  "thought": "I have the information from the database.",
  "final_answer": "आईपीएल 2025 के ऑरेंज कैप विजेता [Player Name] थे, जिन्होंने [Runs] रन बनाए।"
}
```
"""

class ReActAgent:
    def __init__(self, user_query, history=None):
        self.client = get_ai_client()
        self.query = user_query
        self.history = history or []
        self.max_steps = 5
        self.current_step = 0
        self.log = []

    async def run(self):
        messages = [{"role": "system", "content": REACT_SYSTEM_PROMPT.format(TODAY=TODAY, CURRENT_YEAR=CURRENT_YEAR)}]
        if self.history:
            messages.extend(self.history[-4:]) 
        messages.append({"role": "user", "content": f"QUERY: {self.query}"})
        
        final_answer = None
        while self.current_step < self.max_steps:
            self.current_step += 1
            try:
                response = await self.client.chat.completions.create(
                    model=get_model_name(),
                    messages=messages,
                    temperature=0.1, 
                    response_format={"type": "json_object"}
                )
                content = response.choices[0].message.content
                messages.append({"role": "assistant", "content": content})
                
                try:
                    step_data = json.loads(content)
                except json.JSONDecodeError:
                    if "final_answer" in content:
                        final_answer = content
                        break
                    else:
                        messages.append({"role": "user", "content": "Error: Invalid JSON format. Please output strictly JSON."})
                        continue
                        
                thought = step_data.get("thought", "")
                action = step_data.get("action")
                action_input = step_data.get("action_input", {})
                fin_ans = step_data.get("final_answer")
                
                logger.info(f"ReAct Step {self.current_step} | Think: {thought} | Act: {action}")
                
                if fin_ans:
                    final_answer = fin_ans
                    break
                
                if not action:
                    messages.append({"role": "user", "content": "Error: 'action' field missing. Please specify a tool or provide 'final_answer'."})
                    continue

                observation = await self._execute_tool(action, action_input)
                obs_str = f"[OBSERVATION]: {json.dumps(observation, default=str)}"
                messages.append({"role": "user", "content": obs_str})
                
            except Exception as e:
                logger.error(f"ReAct Loop Error: {e}")
                messages.append({"role": "user", "content": f"System Error: {str(e)}"})
        
        return final_answer or "I could not retrieve the complete information in time."

    async def _execute_tool(self, tool_name, args):
        """Map tool names to actual functions in backend_core or history_service"""
        try:
            from src.environment.backend_core import get_live_matches, get_match_scorecard
            from src.environment.history_service import (
                search_historical_matches,
                get_series_history_summary,
                get_player_past_performance
            )
            from src.core.search_service import find_match_by_score
            from src.core.universal_cricket_engine import handle_universal_cricket_query
            from src.core.cricket_calculator import cricket_calculator
            
            if tool_name == "get_live_matches":
                return await get_live_matches(**args)
            elif tool_name == "get_match_history":
                return await search_historical_matches(
                    query=args.get("query"),
                    team=args.get("team_name"),
                    year=args.get("year"),
                    limit=3
                )
            elif tool_name == "get_match_scorecard":
                return await get_match_scorecard(args.get("match_id"))
            elif tool_name in ["get_series_stats", "get_series_info"]:
                return await get_series_history_summary(
                    series_name=args.get("series_name") or args.get("series"),
                    year=args.get("year")
                )
            elif tool_name == "get_player_stats":
                return await get_player_past_performance(args.get("player_name"))
            elif tool_name == "universal_query":
                return await handle_universal_cricket_query(args.get("user_query") or args.get("query"))
            elif tool_name == "calculate":
                return await calculate(args.get("expression"))
            elif tool_name == "project_score":
                return cricket_calculator.calculate_projected_score(
                    args.get("current_runs", 0), 
                    args.get("overs_bowled", 1), 
                    args.get("wickets_lost", 0)
                )
            elif tool_name == "required_run_rate":
                return cricket_calculator.calculate_required_run_rate(
                    args.get("target", 0),
                    args.get("current_runs", 0),
                    args.get("overs_remaining", 0)
                )
            elif tool_name == "net_run_rate":
                return cricket_calculator.calculate_nrr(
                    args.get("runs_scored", 0),
                    args.get("overs_faced", 1),
                    args.get("runs_conceded", 0),
                    args.get("overs_bowled", 1)
                )
            elif tool_name == "sync_data":
                from src.environment.history_service import sync_recent_finished_matches
                days = args.get("days", 7)
                return await sync_recent_finished_matches(days_back=days)
            elif tool_name == "find_match_by_event":
                return await find_match_by_score(
                    args.get("team"),
                    args.get("description")
                )
            else:
                return {"error": f"Unknown tool '{tool_name}'"}
        except Exception as e:
            return {"error": f"Tool Execution Failed: {str(e)}"}

async def run_reasoning_agent(user_query, history=None):
    """Entry point for the ReAct Agent"""
    agent = ReActAgent(user_query, history)
    result = await agent.run()
    return result