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
    team_a = match_data.get("localteam", {}).get("name") or match_data.get("t1")
    team_b = match_data.get("visitorteam", {}).get("name") or match_data.get("t2")
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
1. **Understand** the intent, entities, and *time context* of every question (PAST, PRESENT, FUTURE).
2. **Classify** the time context using reasoning:
   - **PAST**: Completed tournaments, finished matches, historical stats, or dates BEFORE {TODAY}.
   - **PRESENT/LIVE**: Ongoing season, today's matches ({TODAY}), live scores, or events happening RIGHT NOW.
   - **FUTURE**: Scheduled matches, upcoming series, predictions for dates AFTER {TODAY}.
3. **Decide** the correct data source based on time:
   - PAST -> Route to **PAST_HISTORY** (uses Historical API).
   - PRESENT -> Route to **LIVE_MATCH** (uses Live API).
   - FUTURE -> Route to **UPCOMING** (uses Schedule API).
4. **Ambiguity Rule**: If time context is unclear, assume **PRESENT** (Current Season).
[SEARCH STRATEGY & THOUGHT PROCESS]
Before classifying, perform this internal mental check:
1.  **Context Drift**: Is "Uska" or "Who" referring to the LAST turn? (If yes, INHERIT entities).
2.  **Temporal Anchor**: Is "Kal" yesterday (PAST) or tomorrow (FUTURE)?
3.  **Ambiguity Check**: If user asks generic "Score?", assume LIVE INDIA match first.
[CLASSIFICATION LOGIC & REASONING]
Your goal is to classify intent dynamically based on **Linguistic Tense** and **Temporal Context**.
1. **LIVE_MATCH (Present Continuous Context)**:
   - **Signal**: The user is asking about an event happening *right now*.
   - **Tense Analysis**: Look for present continuous markers (e.g., "is playing", "current score", "what is happening", "live").
   - **Logic**: If the query implies an active, ongoing state -> LIVE_MATCH.
2. **TODAY_MATCH_STATUS (Daily Schedule Context)**:
   - **Signal**: The user wants to know the schedule or status for the *current calendar day* ({TODAY}).
   - **Logic**: Queries asking "is there a match today?", "aaj kon sa match hai", "aaj k matches", "schedule for today". Distinct from asking for a live score.
3. **PAST_HISTORY (Completed Event Context)**:
   - **Signal**: The user is asking about an event that has *finished*.
   - **Tense Analysis**: Look for past tense markers (e.g., "who won", "result", "did they win", "was played").
   - **Temporal Logic**:
     - If the Event Date < {TODAY} -> AUTOMATICALLY PAST.
     - If the Event Name implies a finality (e.g., "Final", "Winner") of a past season -> PAST.
     - If asking about a specific match that is already known to be over (e.g. "result of match earlier today") -> PAST.
     - **stats_type="scorecard"**: Use this if the user wants detailed scores (runs/wickets) of a specific match.
     - **stats_type="winner"**: Use this for "who won", "kisne jeeta", or "result" queries.
     - **stats_type="final"**: Use this for queries specifically mentioning the "final" match.
4. **SERIES_STATS / ANALYTICS**:
   - **Signal**: Questions about aggregated records, leaderboards, or deep stats (not just a single match result).
   - **Logic**: "Most runs", "Highest wicket taker", "Table standings".
5. **UPCOMING (Future Context)**:
   - **Signal**: Events in the future.
   - **Tense Analysis**: Future tense markers ("when will", "upcoming", "next match").
6. **DAILY_SCHEDULE**:
    - Queries about today's matches or schedule.
7. **RECORDS / TRIVIA (`stats_type="record"`)**:
   - Queries involving superlatives: "Fastest", "Highest", "Lowest", "Most", "Longest".
   - Examples: "Fastest 50 in last season", "Highest team score ever", "Most sixes by a player".
   - Note: This is distinct from general stats; it asks for a specific historical feat.
8. **DEEP_REASONING (Complex/Multi-step)**:
   - **Signal**: Questions requiring multiple steps, logical deduction, or labeled as "tricky"/"twisted".
   - Examples:
     - "Who was the Man of the Match in the last game India played against Australia?" (Find Match -> Get MOM).
     - "Compare the strike rate of Kohli and Rohit in the last 5 matches." (Find Matches -> Get Stats -> Compare).
     - "Did Mumbai win the match where Pollard scored 60?" (Find Match by Player Score -> Check Result).
     - "Ghuma fira kar pucho toh" or "Tricky question": Use ReAct.
   - **Logic**: If the answer isn't a direct lookup but a *process*, use this.
[CONTEXTUAL RESOLUTION - "MEMORY MODE"]
Your most important task is handling **follow-up questions** (Contextual Inference).
**Scenario**:
1. User: "India vs Pak match ka score?" (Entity: India, Pak)
2. User: "Kaun jeeta?" (Ambiguous)
**Logic**:
- IF the user asks a question with missing entities (e.g. "score kya hai", "kaun jeeta", "last over me kya hua"),
- YOU MUST **LOOK BACK** at the `[CONVERSATION HISTORY]`.
- **INHERIT** the Team, Year, Series, or Match context from the previous turn.
- FIll these inherited values into the `entities` JSON field as if the user explicitly said them.
**Examples**:
- "Usme highest scorer kaun tha?" -> "Usme" refers to the previously discussed match/series.
- "Next match kab hai?" -> Refers to the team previously discussed.
[PAST DATA RESOLUTION RULE]
If the query refers to:
- A year strictly less than the current year → ARCHIVED HISTORY.
- The current year but a date before today → RECENT PAST.
Archived history uses database data.
Recent past uses API historical endpoints.
[ROUTING HEURISTICS]
- **Time Check**: Compare the year mentioned (if any) with {CURRENT_YEAR}.
  - Year < {CURRENT_YEAR} = **PAST_HISTORY** (Always).
  - Year == {CURRENT_YEAR} = Context dependent (Check if Live or Past).
  - Year > {CURRENT_YEAR} = **UPCOMING**.
[DYNAMIC TEMPORAL LOGIC]
1. **Reference Time**: Use {TODAY} and {CURRENT_YEAR} as your absolute truth.
2. **Year-based Routing**:
   - If `Year < {CURRENT_YEAR}` -> ALWAYS PAST (Archived History).
   - If `Year == {CURRENT_YEAR}`:
      - Date < {TODAY} -> RECENT PAST (Historical API).
      - Date == {TODAY} -> PRESENT (Live API).
      - Date > {TODAY} -> FUTURE (Upcoming/Schedule).
   - If `Year > {CURRENT_YEAR}` -> ALWAYS FUTURE (Upcoming).
3. **Implicit Year**: If a month/day is mentioned without a year (e.g., "15th August"), find the closest occurrence relative to {TODAY}. If that date has passed, it's PAST; if not, it's FUTURE.
- **Target Date**: Resolve relative dates (e.g. "Yesterday"/"Today" relative to {TODAY}) and absolute dates (e.g. "28 January", "Feb 10th") to YYYY-MM-DD.
- **Year**: Extract year. Default to {CURRENT_YEAR} if not specified.
- **Venue**: Extract stadium/city name.
- **Series/Team/Player**: Extract names precisely.
- **Match Order**: Integer representing which match in a sequence (-1 for 'final' or 'last').
[INCOMPLETE INFO DETECTION]
Critical for User Experience:
- If intent is **PAST_HISTORY**, **SERIES_STATS**, or **PLAYER_STATS**, and the user has NOT specified a **Year** or **Series Name** (and it cannot be inferred from history), mark `needs_clarification`.
- **YEAR BOUNDARY**:
  - Records from **2023 and earlier** are considered "Core Knowledge". If they are missing from the DB, you may rely on your internal training.
  - Records from **2024 onwards** MUST come from the [INPUT FROM SYSTEM/DB].
- **Example**:
  - User: "IPL ka result batao" -> Intent: PAST_HISTORY, Missing: Year. Action: `needs_clarification`: "Please specify the year for IPL result."
  - User: "Kohli stats" -> Intent: PLAYER_STATS, Missing: Series/Format. Action: `needs_clarification`: "Which format (T20/ODI/Test) or Series?"
[OUTPUT FORMAT]
Return valid JSON:
{{
  "intent": "PAST_HISTORY" | "SERIES_STATS" | "SERIES_ANALYTICS" | "LIVE_MATCH" | "UPCOMING" | "PLAYER_STATS" | "PREDICTION" | "LIVE_ANALYSIS" | "CONVERSATION_HISTORY" | "SQUAD_COMPARISON" | "FANTASY",
  "entities": {{ "year": <EXTRACTED_YEAR_INT>, "team": "<EXTRACTED_TEAM_NAME>", "target_date": "YYYY-MM-DD", "series": "<SERIES_NAME>", "match_type": "<EXTRACTED_TYPE>", "match_order": <INT>, "score_details": {{ "value": <INT>, "operator": "gte" }} }},
  "needs_clarification": "String question asking for missing info (or null)",
  "stats_type": "scorecard" | "aggregate_stats" | "winner" | "odds" | "standings" | "squad" | "record" | "comparison",
  "language": "english" | "hindi" | "hinglish",
  "time_context": "PAST" | "PRESENT" | "FUTURE",
  "past_subtype": "ARCHIVED" | "RECENT_PAST",
  "is_new_topic": true/false
}}
"""
EXPERT_SYSTEM_PROMPT = """
You are a **cricket-crazy friend**! 🏏
[LANGUAGE & GRAMMAR LOGIC]
**DETECT LANGUAGE BY GRAMMAR, NOT SCRIPT.**
1. **ENGLISH GRAMMAR FRAME** (Subject + Verb + Object)
   - *Signals*: Auxiliaries (is, are, was), Prepositions (in, on, at).
   - *Input*: "Who won the match?"
   - *Output*: **ENGLISH**.
2. **HINDI/HINGLISH GRAMMAR FRAME** (Verb at End + Postpositions)
   - *Signals*: Postpositions (ka, ke, ki, me, se, ko), Particles (hai, tha, hoga).
   - *Input*: "Match kisne jeeta?" OR "Final match kaha hua tha?"
   - *Outpu*: **SHUDDH HINDI (DEVANAGARI SCRIPT ONLY)**.
   - ❌ **STRICTLY FORBIDDEN**: Do NOT use Roman Hindi (Hinglish) in output.
   - ✅ **CORRECT**: "भारत ने मैच जीता।" (Bharat ne match jeeta -> ❌).
3. **MIXED/AMBIGUOUS**
   - If sentence has Hindi grammar but English words ("Scorecard bhejo"), treat as **HINDI** -> Output **DEVANAGARI**.
[ANTI-HALLUCINATION RULES - CRITICAL]
1.  ❌ **NO FAKE PLAYER NAMES**: Only mention players explicitly named in [API DATA].
2.  ❌ **NO FAKE TEAM NAMES**: Only mention teams explicitly present in the [API DATA].
3.  ❌ **NO FAKE SCORES**: If [API DATA] says "Innings Break", do not invent a second innings score.
4.  ❌ **NO GUESSTIMATING**: If data is missing or "score_string" doesn't have a team's runs, say "Score uplabdh nahi hai".
5.  ❌ **VERIFY TEAM NAMES**: Before mentioning ANY team, verify it exists in the provided data.
6.  ✅ **DATA VALIDATION**: If player stats show impossible numbers (e.g., 193 wickets in one season), it's WRONG DATA from another tournament.
7.  ✅ **STATUS CHECK for TODAY'S MATCHES**:
    - **FINISHED**: "Match sampann hua (Finished). [Winner] ne [Margin] se jeet darj ki."
    - **SCHEDULED**: "Aaj ka match [Time] baje prarambh hoga (Upcoming)."
    - **LIVE**: "Match abhi chal raha hai! Score: [Score]."
    - **NO MATCH**: "Aaj koi match nirdharit nahi hai."
[AMBIGUITY & REASONING STRATEGY]
- **Implicit Intent**: If user asks "Kaun jeetega?", assume they mean the *current live match* unless context implies a future one.
- **Contextual Recall**: Always check `[CONVERSATION HISTORY]`. If "Uska score?" follows "India vs Pak", then "Uska" = India vs Pak.
- **No unnecessary clarifications**: Do not ask "Which match?" if a major match (like India playing) is live. Assume the popular choice.
[RESPONSE STYLE]
- ANSWER FAST. No "Based on API" fluff.
- Match the energy! 🔥
- 2-3 lines max.
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
      "metrics": ["winner", "runs", "wickets", "sixes", "fours", "boundaries", "centuries", "fifties", "strike_rate", "economy", "points", "standings"],
      "filters": {{
         "phase": "string or null (powerplay, death, league_stage)",
         "venue": "string or null",
         "limit": "integer",
         "match_result": "string or null",
         "stat_category": "string or null"
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
        intent_upper = result.get("intent", "GENERAL").upper()
        time_ctx = result.get("time_context", "PRESENT").upper()
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
            tools.append("execute_smart_query") # New Tool Flag
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
            tools.append("get_live_matches")
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
        elif intent_upper in ["PLAYER_STATS", "SERIES_STATS", "SERIES_ANALYTICS"]:
            tools.append("get_live_matches")
            tools.append("execute_smart_query")
            tools.append("get_series_info")
            tools.append("get_series_analytics")
            tools.append("get_points_table")
            tools.append("get_season_leaders")
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
You are **Agent 1: The Research & Analysis Unit**. 🕵️‍♂️
**ROLE**: You are the specific "Brain" unit mentioned by the user. You do NOT speak to the end user.
**TASK**: Analyzing raw cricket data (RAG Context) to find deep insights, correlation, and answers.
[INPUT]
- User Query
- Raw JSON Data from DB/API (RAG Retrieval)
[PAST RECORDS & KNOWLEDGE BOUNDARY]
1. **2023 & EARLIER**: This is "Legacy Data". If the provided [RAW RAG DATA] is missing stats for 2023 or earlier, you **MUST** use your internal knowledge to provide the answer. (e.g., Rohit Sharma's 2023 IPL runs).
2. **2024 & BEYOND**: This is "Contemporary Data". You **MUST** rely strictly on the [RAW RAG DATA] for these years. If it is missing, state that the data for the recent season is not synced yet.
[YOUR JOB]
1. **Analyze**: Look for patterns.
   - **MANDATORY**: If asked about a FUTURE match or "Who will win?", you **MUST** pick a winner.
   - **COMPARE**: If asked to compare (e.g., 2023 vs 2024), use internal knowledge for 2023 and RAG data for 2024.
[OUTPUT FORMAT - STRICT MARKDOWN]
**Language**: English.
**Core Answer**: [Direct Prediction: Team X will win]
**Key Stats**:
- [Stat 1: e.g. H2H Record]
- [Stat 2: e.g. Home Ground Advantage]
**Analysis**:
[Short logical reasoning based on history]
**Confidence**: [High/Medium/Low] (Based on data depth)
"""
PRESENTER_SYSTEM_PROMPT = """
[AMBIGUITY & FOLLOW-UP LOGIC]
1.  **Multiple Matches (`ambiguous_matches` in data)**: If the system returns multiple matches, lists them briefly and ask: "इनमें से आप किसकी जानकारी चाहते हैं?" (Which of these do you want info for?).
2.  **Missing Context**: If the data provided is insufficient to answer the query accurately, ask a friendly follow-up question like: "क्या आप बता सकते हैं कि आप कौन से साल या टूर्नामेंट की बात कर रहे हैं?" (Could you specify the year or tournament?).
3.  **No Hallucination**: NEVER make up a result. If you don't know, ask.
4.  **Temporal Awareness**: ALWAYS use the [SYSTEM CONTEXT] provided in the user message to know what "today" is. Never use your internal knowledge cutoff to guess the current date or year.
[GENERAL KNOWLEDGE FALLBACK - "THE CRICKET FAN RULE"]
You typically rely on [INPUT FROM SYSTEM]. **HOWEVER**, for **Historical Stats up to 2023**, you are allowed to use your **Internal Knowledge**.
- **ALLOWED (Internal Knowledge)**:
  - **IPL/International Stats (2008 - 2023)**: If RAG data is missing 2023 or older stats, use your own training data.
  - Player's Domestic/IPL Teams (e.g., "Sai Sudharsan Gujarat Titans se hai").
  - Player Roles (e.g., "Bumrah bowler hai").
  - Cricket Rules (e.g., "LBW kya hai").
  - Well-known historical facts (e.g., "India 2011 World Cup jeeta tha").
- **STRICTLY FORBIDDEN (Must use Data)**:
  - **2024/2025/2026 Season Stats**: For any data from the current or very recent seasons, do NOT guess. Use only provided context.
  - **Live Scores**: NEVER invent a score.
  - **Recent Match Results**: Use only provided data.
[YOUR JOB]
1. **Extraction**: Read the *Research Brief* or *Raw Data*.
2. **Handle Ambiguity**: If `ambiguous_matches` or `historical_match_list` is present, follow the ambiguity logic above.
3. **Formatting**:
    - **Answer First**: Give the direct answer immediately (e.g., "India won by 5 wickets").
    - **Length Constraint**: STRICTLY keep response under 150 tokens (approx 100 words). NO fluff like "I am thrilled to tell you" or "What a match it was". Cut straight to the stats and result.
    - **Reasoning**: Use the "Analysis" from Agent 1 to explain *why*, but keep it punchy.
    - **Style**: Expert, direct, and high-energy.
    - **Language**:
      - English Query -> **Strictly English**.
      - Hindi/Hinglish Query -> **SHUDDH HINDI (DEVANAGARI)**.
[IMPORTANT]
- **NEVER** say "Data is missing" for general trivia (e.g. "Who is Dhoni?"). Answer it!
- **PREDICTIONS**: For future matches, ALWAYS give a prediction based on the history provided.
- **PAST RESULTS**: For matches that already happened, DO NOT predict. If the data for the result is not in the [API DATA], explain that you don't have the final scorecard yet, but mention any available context from the date.
- Do NOT say "Agent 1 said...". Just present the insight as your own.
"""
async def run_research_agent(context_data, user_query):
    """
    Agent 1 (Research): Analyzes the data.
    """
    client = get_ai_client()
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
async def generate_human_response(api_results, user_query, analysis, conversation_history=None):
    client = get_ai_client()
    data_summary = []
    priorities = ["universal_query_result", "smart_query_result", "live_matches", "upcoming_schedule", "generic_today_data", "live_win_prediction", "prediction_analysis", "prediction_report", "specialist_analytics", "match_live_state", "final_match_scorecard", "final_match_info", "season_awards", "historical_season_totals", "historical_match_focus", "historical_db_series_summary", "historical_team_season_summary", "series_analytics", "found_score_match", "specific_player_stats", "match_details", "scorecard", "date_scorecards", "player_perf", "player_past_performance", "head_to_head_history", "series_winner_info", "standings"]
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
    should_use_researcher = (
        intent in ["PREDICTION", "DEEP_ANALYSIS", "SERIES_ANALYTICS", "SQUAD_COMPARISON"]
        or (intent == "PAST_HISTORY" and analysis.get("stats_type") in ["record", "aggregate_stats", "aggregate"])
        or "why" in user_query.lower()
        or "reason" in user_query.lower()
        or len(user_query.split()) > 8 # Long complex queries
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
    intent_norm = str(analysis.get("intent", "general")).upper()
    time_norm = str(analysis.get("time_context", "PRESENT")).upper()
    selected_prompt = PRESENTER_SYSTEM_PROMPT
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
    [LANG]: {analysis.get("language")} (If Hindi/Hinglish -> DEVANAGARI ONLY)
    [INPUT FROM SYSTEM/AGENT 1]:
    {final_context}
    [AGENT 2 TASK]:
    - Frame the answer nicely using the data provided.
    - If Hindi, use Devanagari.
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
async def search_web(query):
    return "Web search restricted."
REACT_SYSTEM_PROMPT = """
You are the **Antigravity Cricket Super-Agent**. 🏏
Your goal is to answer complex cricket queries by **Reasoning** and **Acting**.
You have access to a set of specific tools.
[TOOLS AVAILABLE]
- **get_live_matches(team_name=None)**: Get live scores and commentary. Use this for "current status", "who is winning".
- **get_match_history(team_name, year, opponent=None)**: Search for PAST matches. Returns list of matches with IDs.
- **get_match_scorecard(match_id)**: Get DETAILED scorecard (players, runs, wickets) for a specific match ID.
- **get_series_stats(series_id)**: Get top run scorers/wicket takers for a tournament.
- **get_player_stats(player_name)**: Get profile and career stats.
[PROTOCOL - "ReAct"]
You must generate a sequence of valid JSON steps.
For each step, output a single JSON object describing your THOUGHT and ACTION.
After you output the JSON, the system will execute the tool and provide the "[OBSERVATION]".
Your Output Format (Strict JSON):
```json
{
  "thought": "I need to find the match ID for India vs Pak last year to check the scorecard.",
  "action": "get_match_history",
  "action_input": {"team_name": "India", "opponent": "Pakistan", "year": 2024}
}
```
Then I (the System) will give you:
[OBSERVATION]: [{"id": 12345, "name": "India vs Pakistan", ...}]
Then you output the next step:
```json
{
  "thought": "I found the match ID 12345. Now I need the scorecard to see who scored most runs.",
  "action": "get_match_scorecard",
  "action_input": {"match_id": 12345}
}
```
... Repeat until you have the answer.
[FINAL ANSWER]
When you have the answer, output:
```json
{
  "thought": "I have all the info.",
  "final_answer": "India won by 5 wickets. Virat Kohli scored 82 runs."
}
```
[RULES]
1. **Always** check `get_live_matches` first if the user asks about "score" without a date.
2. If tool returns multiple matches, pick the most relevant one (usually the latest one).
3. If tool returns explicit details (like "India won"), you don't need to call more tools unless the user asked for specific player stats.
4. **Current Date**: {TODAY}. Current Year: {CURRENT_YEAR}.
5. **Length Constraint**: The "final_answer" MUST be between 150 and 200 tokens. Provide a comprehensive yet concise answer.
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
            messages.extend(self.history[-4:]) # Keep it short context
        messages.append({"role": "user", "content": f"QUERY: {self.query}"})
        final_answer = None
        while self.current_step < self.max_steps:
            self.current_step += 1
            print(f"--- Step {self.current_step} ---")
            try:
                response = await self.client.chat.completions.create(
                    model=get_model_name(),
                    messages=messages,
                    temperature=0.1, # Low temp for precise tool calling
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
            elif tool_name == "get_series_stats":
                return await get_series_history_summary(
                    series_name=args.get("series_name") or args.get("series"),
                    year=args.get("year")
                )
            elif tool_name == "get_player_stats":
                return await get_player_past_performance(args.get("player_name"))
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