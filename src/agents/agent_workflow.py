import asyncio
import json
import streamlit as st
from datetime import datetime
from src.utils.utils_core import get_logger
from src.agents.ai_core import analyze_intent, run_reasoning_agent, generate_human_response, predict, predict_live_match
from src.utils.match_utils import _normalize, _is_team_match
from src.environment.backend_core import (
    get_upcoming_matches, get_todays_matches, get_live_matches,
    get_match_scorecard, get_series_standings, cricket_api
)
from src.environment.history_service import (
    get_player_past_performance, get_head_to_head_history,
    past_db_get_standings, execute_smart_query, sync_specific_match
)
from src.core.search_service import (
    find_match_id, find_series_smart, find_match_by_score
)
from src.core.analytics_service import (
    get_series_analytics, extract_series_winner,
    get_player_recent_performance, get_series_matches_by_id,
    get_series_top_performers, handle_tournament_specialist_logic, get_head_to_head_statistics
)
from src.environment.live_match_service import (
    get_live_match_details, fetch_match_context_bundle, calculate_match_odds
)

from src.core.rag_orchestrator import execute_rag_pipeline
def update_context(series=None, year=None, team=None, player=None, opponent=None):
    if series: st.session_state.chat_context["last_series"] = series
    if year: st.session_state.chat_context["last_year"] = year
    if team: st.session_state.chat_context["last_team"] = team
    if opponent: st.session_state.chat_context["last_opponent"] = opponent
    if player: st.session_state.chat_context["last_player"] = player
async def process_user_message(user_query, conversation_history=None):
    matches_task = asyncio.create_task(get_todays_matches(use_cache=False, ttl=5))
    analysis_task = asyncio.create_task(analyze_intent(user_query, conversation_history))
    analysis = await analysis_task
    analysis["intent"] = str(analysis.get("intent", "general")).upper()
    intent = analysis["intent"]
    analysis["stats_type"] = str(analysis.get("stats_type", "none")).lower()
    stats_type = analysis["stats_type"]
    analysis["time_context"] = str(analysis.get("time_context", "present")).upper()
    time_context = analysis["time_context"]
    analysis["past_subtype"] = str(analysis.get("past_subtype", "unknown")).upper()
    past_subtype = analysis["past_subtype"]
    entities = analysis.get("entities") or {}
    target_date = entities.get("target_date")
    is_archived = (past_subtype == "ARCHIVED")
    intent_map = {
        "LIVE_MATCH": "LIVE_FEED.log",
        "UPCOMING": "UPCOMING_SCHEDULE.log",
        "PAST_HISTORY": "PAST_HISTORY.log",
        "PLAYER_STATS": "ANALYTICS.log",
        "SERIES_STATS": "ANALYTICS.log",
        "SERIES_ANALYTICS": "ANALYTICS.log",
        "PREDICTION": "PREDICTION.log", # New Prediction Log
        "DEEP_REASONING": "AGENT_REASONING.log",
        "GENERAL": "GENERAL_QUERIES.log"
    }
    log_file = intent_map.get(intent, "GENERAL_QUERIES.log")
    logger_name = log_file.replace(".log", "").lower() + "_logger"
    ctx_logger = get_logger(logger_name, log_file)
    ctx_logger.info(f"=== NEW QUERY START [{datetime.now().strftime('%H:%M:%S')}] ===")
    ctx_logger.info(f"QUERY: {user_query}")
    ctx_logger.info(f"ANALYSIS: Intent={intent} | Entities={entities}")
    try:
        if hasattr(st, "session_state") and "chat_context" in st.session_state:
             ctx_logger.info(f"CONTEXT: Last_Series={st.session_state.chat_context.get('last_series')}")
    except: pass
    logger = ctx_logger
    if intent == "DEEP_REASONING":
        ctx_logger.info("🧠 Triggering ReAct Agent for Deep Reasoning...")
        try:
            agent_response = await run_reasoning_agent(user_query, conversation_history)
            ctx_logger.info(f"ReAct Agent Result: {agent_response}")
            return agent_response
        except Exception as e:
            ctx_logger.error(f"ReAct Agent Failed: {e}")
            ctx_logger.warning("Falling back to standard router...")
    required_tools = analysis.get("required_tools", [])
    entities = analysis.get("entities") or {}
    is_new_topic = analysis.get("is_new_topic", False)
    new_s = entities.get("series")
    t_name = entities.get("team")
    o_name = entities.get("opponent")
    p_name = entities.get("player")
    last_s = st.session_state.chat_context.get("last_series")
    if is_new_topic or (intent == "LIVE_MATCH"):
        st.session_state.chat_context["last_series"] = new_s
        st.session_state.chat_context["last_team"] = t_name
        st.session_state.chat_context["last_opponent"] = o_name
        st.session_state.chat_context["last_player"] = p_name
        if intent == "LIVE_MATCH":
             st.session_state.chat_context["last_year"] = datetime.now().year
    if new_s and _normalize(new_s) != _normalize(last_s):
         st.session_state.chat_context["last_series"] = new_s
         st.session_state.chat_context["last_team"] = None
         st.session_state.chat_context["last_opponent"] = None
         st.session_state.chat_context["last_player"] = None
    s_name = entities.get("series") or st.session_state.chat_context.get("last_series")
    year = entities.get("year") or st.session_state.chat_context.get("last_year") or datetime.now().year
    update_context(s_name, entities.get("year"), t_name, p_name, o_name)
    
    # Context Fallback: Ensure tools use persistent context if entities are missing
    t_name = t_name or st.session_state.chat_context.get("last_team")
    o_name = o_name or st.session_state.chat_context.get("last_opponent")
    p_name = p_name or st.session_state.chat_context.get("last_player")
    needs_clarification = analysis.get("needs_clarification", None)
    if needs_clarification:
        return needs_clarification
    api_results = {}
    ctx_logger.info("Checking Tournament Specialist Logic...")
    specialist_res = await handle_tournament_specialist_logic(analysis, user_query, s_name, year)
    if specialist_res and "error" not in specialist_res:
         ctx_logger.info(f"Specialist returned data for {s_name}")
         api_results["specialist_analytics"] = specialist_res
    else:
         ctx_logger.info("No specialist data found.")
    if analysis.get("retrieve_chat_history") and conversation_history:
        api_results["personal_conversation_history"] = conversation_history
    # --- YEAR EXTRACTION & ROUTING LOGIC (CASE A, B, C) ---
    q_years = entities.get("years") or []
    if not q_years and entities.get("year"):
        q_years = [entities.get("year")]
    
    # Simple regex fallback if extraction failed
    if not q_years:
        import re
        q_years = re.findall(r'\b(19|20)\d{2}\b', user_query)
    
    # Ensure all are clean integers
    clean_years = []
    for y in q_years:
        try:
            clean_years.append(int(str(y)))
        except: continue
    q_years = clean_years

    # If NO years were found at all, but intent is PAST, assume search is needed or career check
    old_years = [y for y in q_years if y <= 2023]
    new_years = [y for y in q_years if y >= 2024]

    # Decide Source:
    is_pure_historical = (len(q_years) > 0 and len(new_years) == 0)
    is_pure_database = (len(q_years) > 0 and len(old_years) == 0)
    is_mixed = (len(old_years) > 0 and len(new_years) > 0)
    no_year_detected = (len(q_years) == 0)
    
    # Special Fix: If user says "2023", ensure it's PURE HISTORICAL
    if 2023 in q_years and len(new_years) == 0:
        is_pure_historical = True
        is_pure_database = False

    ctx_logger.info(f"Year Routing -> Years: {q_years} | Hist: {is_pure_historical} | DB: {is_pure_database} | Mixed: {is_mixed}")

    # --- DATA SOURCING LOGIC (Finished = DB, Live/Upcoming = API) ---
    q_lower = user_query.lower()
    
    # Logic: Detect if query is about TODAY
    # This is critical because even if a match is "PAST" (finished), if it was TODAY,
    # the Database might not have it yet, so we MUST check the Live API.
    today_iso = datetime.now().strftime("%Y-%m-%d")
    is_about_today = (target_date == today_iso) or (time_context == "PRESENT")
    
    is_past_intent = (
        (intent in ["PAST_HISTORY", "RECORDS", "DEEP_REASONING"]) or
        (intent in ["PLAYER_STATS", "SERIES_STATS", "HEAD_TO_HEAD"] and time_context == "PAST") or
        (time_context == "PAST") or
        (target_date and target_date < today_iso) or
        is_pure_historical or is_mixed
    )
    
    # EXCEPTION: If it is about TODAY, we don't treat it as "Pure Past" for DB-only sourcing.
    # We still allow RAG, but we will ensure Live API data is fetched later.
    if is_about_today:
        ctx_logger.info("📍 Query identifies as TODAY -> Ensuring Live API check regardless of 'Past' status.")
        ctx_logger.info(f"LOGIC: Historical={is_pure_historical} | DB={is_pure_database} | Mixed={is_mixed} | NoYear={no_year_detected}")
        
        # Execute RAG Pipeline for Database years (>= 2024)
        rag_result = {"status": "skipped", "data_count": 0}
        if is_pure_database or is_mixed or (no_year_detected and intent != "GENERAL"):
             ctx_logger.info("📡 Executing RAG Pipeline for Database context...")
             rag_result = await execute_rag_pipeline(user_query, analysis)
        
        # Configure Internal Knowledge permission based on routing
        if is_pure_historical or is_mixed or no_year_detected:
             ctx_logger.info("📚 Enabling Internal Knowledge Fallback.")
             api_results["internal_knowledge_allowed"] = True
             
             instructions = []
             if is_mixed:
                 instructions.append(f"[MIXED QUERY DETECTED]: Years {old_years} are Historical. Years {new_years} are in Database.")
                 instructions.append("1. Answer data for years <= 2023 using your internal knowledge.")
                 instructions.append("2. Answer data for years >= 2024 using the provided database results.")
             elif is_pure_historical:
                 instructions.append(f"[HISTORICAL QUERY]: For years {old_years}, use ONLY internal knowledge.")
             elif no_year_detected:
                 instructions.append("[CAREER/GENERAL QUERY]: No specific year detected. Use internal knowledge for career stats and database for recent (2024-25) matches.")

             if "rag_evidence" not in api_results: api_results["rag_evidence"] = ""
             api_results["rag_evidence"] += "\n" + "\n".join(instructions)
             
             # If it was pure historical and RAG was skipped/failed, force success to proceed to Presenter
             if is_pure_historical:
                 rag_result["status"] = "success"

        if rag_result.get("status") == "success":
            ctx_logger.info(f"✅ RAG Success: Retrieved {rag_result.get('data_type')} data ({rag_result.get('data_count')} records)")
            
            # Store the main context evidence for the LLM
            if "rag_evidence" not in api_results:
                 api_results["rag_evidence"] = rag_result.get("context")
            
            # Use raw data to populate specific fields if needed
            if rag_result.get("data_type") == "universal":
                api_results["universal_query_result"] = {"data": rag_result.get("raw_data")}
            elif rag_result.get("data_type") == "match":
                # If specifically match data, populate historical_match_focus for deep dives
                if rag_result.get("raw_data") and len(rag_result.get("raw_data")) > 0:
                     api_results["historical_match_focus"] = {
                         "match_info": rag_result.get("raw_data")[0],
                         "rag_sourced": True
                     }
            
            # If RAG found data, we can often skip legacy tools to avoid redundancy/errors
            if rag_result.get("data_count", 0) > 0:
                 ctx_logger.info("RAG provided sufficient data. Optimizing tool usage...")
                 # Filter out some legacy tools if RAG covered them
                 tools_to_remove = ["execute_smart_query", "search_historical_matches"]
                 required_tools = [t for t in required_tools if t not in tools_to_remove]
                 
        else:
             ctx_logger.error(f"❌ RAG Pipeline Failed: {rag_result.get('error')}")
             ctx_logger.info("Falling back to legacy tools...")
             # Fallback to Universal Engine directly if RAG failed (though RAG tries it internally)
             # But if RAG error was catastrophic, we might try one last direct shot? 
             # Actually RAG Orchestrator already does fallback. So we just rely on legacy tools in 'required_tools'.

    for tool in required_tools:
        try:
            logger.info(f"Executing Tool: {tool}...")
            if tool == "execute_smart_query":
                schema_input = analysis # Pass full analysis including structured_schema
                logger.info("Executing Schema-Aware Smart Query...")
                query_res = await execute_smart_query(schema_input)
                api_results["smart_query_result"] = query_res
                logger.info(f"Smart Query Result: {len(query_res)} keys")
            elif tool == "get_head_to_head_statistics":
                 stats = await get_head_to_head_statistics(
                      team_a=t_name,
                      team_b=o_name,
                      player_name=p_name
                 )
                 api_results["head_to_head_stats"] = stats
                 logger.info(f"Head to head Stats: {stats}")
            elif tool == "get_head_to_head_history":
                h2h = await get_head_to_head_history(t_name, o_name)
                api_results["head_to_head"] = h2h
                logger.info(f"Fetched {len(h2h)} H2H matches")
                if p_name:
                    logger.info(f"Adding player performance context for {p_name} in H2H")
                    api_results["player_perf_context"] = await get_player_recent_performance(p_name)
                api_results["head_to_head_stats"] = await get_head_to_head_statistics(
                    team_a=t_name,
                    team_b=o_name,
                    player_name=p_name
                )
            elif tool in ["player_perf", "get_player_performance", "player_stats"]:
                logger.info(f"Handling Player Performance Tool: {tool} for {p_name}")
                series_scope_id = None
                if s_name:
                    series_scope_id = await find_series_smart(s_name, year)
                perf_data = await get_player_recent_performance(p_name, series_id=series_scope_id)
                if perf_data:
                    api_results["player_perf"] = perf_data
                else:
                    api_results["api_error"] = f"No recent performance data found for {p_name}"
            elif tool == "deep_analysis" or intent == "LIVE_ANALYSIS":
                m_id = await find_match_id(t_name, team2=o_name, target_date=target_date, series_name=s_name, year=year)
                if m_id:
                    logger.info(f"Triggering Deep Analysis for match_id: {m_id}")
                    api_results["deep_match_bundle"] = await fetch_match_context_bundle(m_id)
                else:
                    s_id = await find_series_smart(s_name, year)
                    if isinstance(s_id, str) and s_id.startswith("ERROR:"):
                        api_results["api_error"] = s_id
                        s_id = None
                    if s_id:
                        res = await get_series_matches_by_id(s_id)
                        api_results["series_potential_matches"] = res.get("data", [])[:10]
            elif tool == "get_series_analytics" or tool == "get_point_table" or intent == "SERIES_STATS":
                s_id = await find_series_smart(s_name or "IPL", year)
                if isinstance(s_id, str) and s_id.startswith("ERROR:"):
                    api_results["api_error"] = s_id
                    s_id = None
                if s_id:
                    logger.info(f"Fetching Series Analytics for {s_id} (Intent: {intent})")
                    an_res = await get_series_analytics(s_id, deep_scan=True)
                    winner_info = await extract_series_winner(s_id)
                    api_results["series_winner_info"] = winner_info
                    if isinstance(an_res, dict):
                        if not t_name and not o_name:
                             an_res.pop("team_match_sequence", None)
                        an_res.pop("completed_matches", None)
                    api_results["series_analytics"] = an_res
                    sq = (analysis.get("entities") or {}).get("score_mentioned")
                    sd = (analysis.get("entities") or {}).get("score_details")
                    val = sd.get("value") if isinstance(sd, dict) else None
                    if sq and val is not None and str(val).strip() != "":
                        logger.info(f"Series Stats + Score Search for: {sq} (Val: {val})")
                        sc_res = await find_match_by_score(None, sq, year=year, series_name=s_name, score_details=sd)
                        if sc_res: api_results["found_score_match"] = sc_res
            elif tool == "get_series_info" and s_name:
                s_id = await find_series_smart(s_name, year)
                if isinstance(s_id, str) and s_id.startswith("ERROR:"):
                    api_results["api_error"] = s_id
                    s_id = None
                if s_id:
                    res = await get_series_matches_by_id(s_id)
                    if not res.get("ok"):
                        api_results["api_error"] = res.get("error")
                    else:
                        all_matches = res.get("data", [])
                        logger.info(f"Loaded {len(all_matches)} matches for series {s_id}. Sample: {all_matches[:2]}")
                        api_results["series_full_schedule"] = all_matches
                        if stats_type in ["aggregate", "historical", "standings", "aggregate_stats", "scorecard", "winner", "final"]:
                            is_analytical_intent = True
                            if is_analytical_intent:
                                api_results["series_analytics"] = await get_series_analytics(s_id, deep_scan=True)
                                logger.info(f"Proactive Series Analytics added for {s_id} (Stats Type: {stats_type})")
                            important_matches = []
                            q_words = _normalize(user_query)
                            candidates = []
                            team_seq = (api_results.get("series_analytics") or {}).get("team_match_sequence", {})
                            m_order = entities.get("match_order")
                            for m in all_matches:
                                m_name = _normalize(m.get("name", ""))
                                m_date = m.get("date", "0000-00-00")
                                m["_sort_date"] = m_date
                                status_lower = str(m.get("status", "")).lower()
                                penalty = 0
                                if "no scorecard" in status_lower or "abandoned" in status_lower:
                                    penalty = -100
                                score = 0
                                if t_name and t_name in team_seq:
                                    seq = team_seq[t_name]
                                    if m_order and seq:
                                        try:
                                            order_idx = int(m_order)
                                            if order_idx == 1 and m.get("id") == seq[0].get("id"): score = 150
                                            elif order_idx == -1 and m.get("id") == seq[-1].get("id"): score = 150
                                            elif 1 < order_idx <= len(seq) and m.get("id") == seq[order_idx-1].get("id"): score = 150
                                        except: pass
                                elif not t_name and m_order:
                                    try:
                                        order_idx = int(m_order)
                                        if order_idx == 1 and m.get("id") == all_matches[0].get("id"): score = 150
                                        elif order_idx == -1 and m.get("id") == all_matches[-1].get("id"): score = 150
                                        elif 1 < order_idx <= len(all_matches) and m.get("id") == all_matches[order_idx-1].get("id"): score = 150
                                    except: pass
                                if score < 150:
                                    if t_name and o_name and _is_team_match(t_name, m_name) and _is_team_match(o_name, m_name):
                                        score = 60
                                    elif t_name and p_name and _is_team_match(t_name, m_name):
                                        score = 55
                                    elif m_order == -1 and ("final" in m_name and "semi" not in m_name):
                                        score = 500
                                    elif m_order == 1 and ("1st match" in m_name or "opener" in m_name):
                                        score = 14
                                    elif t_name and _is_team_match(t_name, m_name):
                                        score = 10
                                    elif p_name:
                                        score = 5
                                    elif m_order == -1:
                                        score = 1
                                if score > 0 or penalty < 0:
                                    candidates.append((score + penalty, m))
                            if candidates:
                                candidates.sort(key=lambda x: (x[0], x[1]["_sort_date"]), reverse=True)
                                target_match = candidates[0][1]
                            elif m_order is not None:
                                sorted_schedule = sorted(all_matches, key=lambda x: (x.get("date", "9999-12-31"), x.get("dateTimeGMT", "00:00")))
                                logger.info(f"Sorted Schedule Top 3: {[m.get('name') + ' (' + m.get('date', '') + ')' for m in sorted_schedule[:3]]}")
                                if sorted_schedule:
                                    try:
                                        m_idx = int(float(m_order))
                                        logger.info(f"Fallback selection for order {m_idx} from {len(sorted_schedule)} matches")
                                        if m_idx == 1: target_match = sorted_schedule[0]
                                        elif m_idx == -1: target_match = sorted_schedule[-1]
                                        elif 1 < m_idx <= len(sorted_schedule): target_match = sorted_schedule[m_idx-1]
                                        else: target_match = None
                                    except Exception as e:
                                        logger.error(f"Error parsing order: {e}")
                                        target_match = None
                                else: target_match = None
                            else:
                                target_match = None
                            if target_match:
                                logger.info(f"Targeting match (Series Sequence): {target_match['name']} ({target_match['id']})")
                                m_info, m_score = await asyncio.gather(
                                    get_live_match_details(target_match["id"]),
                                    get_match_scorecard(target_match["id"])
                                )
                                if m_info and isinstance(m_info, dict) and "data" in m_info: m_info = m_info["data"]
                                if m_score and isinstance(m_score, dict) and "data" in m_score: m_score = m_score["data"]
                                actual_scorecard = m_score.get("scorecard") if isinstance(m_score, dict) else m_score
                                api_results["historical_match_focus"] = {
                                    "match_info": m_info,
                                    "full_scorecard": actual_scorecard
                                }
                                if actual_scorecard:
                                    for inn in actual_scorecard:
                                        t = inn.get("totals", {})
                                        r_val = int(t.get("R") or t.get("r") or 0)
                                        if r_val == 0:
                                            calc_r = sum(int(b.get("r") or 0) for b in inn.get("batting", []))
                                            extras = inn.get("extras", {})
                                            if isinstance(extras, dict):
                                                calc_r += int(extras.get("r") or extras.get("total") or 0)
                                            calc_w = sum(int(bow.get("w") or 0) for bow in inn.get("bowling", []))
                                            if calc_w == 0:
                                                calc_w = len([b for b in inn.get("batting", []) if b.get("dismissal")])
                                            inn["totals"] = {"R": calc_r, "W": calc_w, "O": t.get("O", "20.0")}
                            else:
                                mf = None
                                if "wicket" in user_query.lower(): mf = "wickets"
                                elif "run" in user_query.lower(): mf = "runs"
                                m_id_deep = await find_match_id(t_name, team2=o_name, year=year, match_type_filter=mf)
                                if m_id_deep:
                                    api_results["historical_match_deep_discovery"] = await get_live_match_details(m_id_deep)
                        if stats_type in ["winner", "series-winner"]:
                            winner_name = await extract_series_winner(s_id)
                            if winner_name: api_results["series_winner_calculated"] = winner_name
                        if stats_type == "aggregate_stats":
                            api_results["series_analytics"] = await get_series_analytics(s_id, deep_scan=True)
                        if target_date:
                            date_matches = [m for m in all_matches if m.get("date") == target_date]
                            api_results["filtered_matches_on_date"] = date_matches
                            if 0 < len(date_matches) <= 2:
                                 logger.info(f"Proactively fetching scorecards for {len(date_matches)} matches on {target_date}")
                                 for dm in date_matches:
                                     sc_info = await get_live_match_details(dm["id"])
                                     if "date_scorecards" not in api_results: api_results["date_scorecards"] = []
                                     api_results["date_scorecards"].append(sc_info)
            elif tool == "get_series_standings" or intent == "SERIES_STATS":
                s_id = await find_series_smart(s_name, year)
                if isinstance(s_id, str) and s_id.startswith("ERROR:"):
                    api_results["api_error"] = s_id
                    s_id = None
                if s_id:
                    standings_res = await get_series_standings(s_id)
                    if standings_res.get("ok"):
                        api_results["standings"] = standings_res
                    else:
                        api_results["api_error"] = standings_res.get("error")
                    if stats_type == "aggregate":
                        logger.info(f"Triggering aggregate analytics based on stats_type for {s_id}")
                        api_results["series_analytics"] = await get_series_analytics(s_id, deep_scan=True)
                    else:
                        api_results["top_performers"] = await get_series_top_performers(s_id)
            elif tool == "get_match_squad":
                m_id = await find_match_id(t_name, team2=o_name, series_id=s_id if 's_id' in locals() else None, target_date=target_date) if t_name else None
                if m_id: api_results["squad"] = await get_match_squad(m_id)
            elif tool == "get_series_analytics":
                 logger.info(f"Series analytics requested for {s_name} ({year})")
                 segment = entities.get("segment", "")
                 phase = entities.get("phase", "")
                 limit = entities.get("match_limit") or entities.get("limit")
                 analytics = await cricket_api("series_analytics", series=s_name, year=year, segment=segment, phase=phase, limit=limit, query=user_query)
                 api_results["series_analytics"] = analytics
            elif tool == "get_points_table":
                 s_id = await find_series_smart(s_name, year)
                 if not s_id and year and int(year) < datetime.now().year:
                     api_results["standings"] = await past_db_get_standings(year, s_name)
                 elif s_id:
                     api_results["standings"] = await get_series_standings(s_id)
            elif tool == "extract_series_winner":
                 s_id = await find_series_smart(s_name, year)
                 if s_id:
                     logger.info(f"Extracting winner info for series: {s_id}")
                     winner_info = await extract_series_winner(s_id)
                     if winner_info:
                         api_results["series_winner_info"] = winner_info
                         if "points_table" in winner_info:
                             api_results["standings"] = winner_info["points_table"]
                 else:
                     logger.warning(f"Could not find series ID for winner extraction: {s_name} {year}")
            elif tool == "get_match_details":
                current_s_id = locals().get('s_id', None)
                m_id = None
                if t_name or (target_date and s_name):
                    match_filter_type = None
                    if "wicket" in user_query.lower(): match_filter_type = "wickets"
                    elif "run" in user_query.lower(): match_filter_type = "runs"
                    m_id = await find_match_id(t_name, team2=o_name, series_id=current_s_id, target_date=target_date, series_name=s_name, year=year, match_type_filter=match_filter_type)
                if not m_id and not t_name:
                    running_matches = api_results.get("today_action_summary", {}).get("currently_running", [])
                    if len(running_matches) == 1:
                        m_id = running_matches[0]["id"]
                        logger.info(f"Contextless query -> Auto-selecting single live match: {running_matches[0]['name']}")
                    elif len(running_matches) > 1:
                        m_id = running_matches[0]["id"]
                        logger.info(f"Contextless query -> Auto-selecting first of multiple live matches: {running_matches[0]['name']}")
                if m_id:
                    details = await get_live_match_details(m_id, use_cache=False)
                    api_results["match_details"] = details
                    if stats_type == "odds":
                        logger.info("Calculating odds for match...")
                        api_results["match_odds"] = calculate_match_odds(details)
            elif tool in ["get_upcoming_matches", "get_upcoming_match"]:
                 if "upcoming_schedule" not in api_results:
                     force_date = entities.get("target_date")
                     upcoming_res = await get_upcoming_matches(check_date=force_date)
                     api_results["upcoming_schedule"] = upcoming_res.get("data", [])[:15]
            elif tool == "get_player_performance" and p_name:
                target_series_id = None
                current_year = datetime.now().year
                try:
                    query_year = int(str(entities.get("year") or year))
                except:
                    query_year = None
                is_historical = is_archived or (query_year and query_year < current_year)
                ctx_logger.info(f"🎯 Player Performance: {p_name} | Year={query_year} | Historical={is_historical}")
                if is_historical or query_year is None:
                    ctx_logger.info(f"📚 Fetching {p_name} stats from LOCAL DATABASE")
                    past_perf = await get_player_past_performance(p_name, s_name, query_year)
                    if past_perf:
                        api_results["player_past_performance"] = past_perf
                        ctx_logger.info(f"✅ Found {len(past_perf)} records in DB for {p_name}")
                    if query_year is None:
                         api_results["player_recent_perf"] = await get_player_recent_performance(p_name)
                else:
                    target_series_id = await find_series_smart(s_name or "IPL", query_year)
                    api_results["player_perf"] = await get_player_recent_performance(p_name, series_id=target_series_id)
                    ctx_logger.info(f"🌐 Fetching {p_name} stats from API (current year {query_year})")
                    if (analysis.get("time_intent") == "past" or analysis.get("time_context") == "PAST") and (s_name or query_year):
                        target_series_id = await find_series_smart(s_name or "IPL", query_year)
                    api_results["player_perf"] = await get_player_recent_performance(p_name, series_id=target_series_id)
            elif tool == "get_player_performance" and not p_name and s_name:
                logger.info("Redirecting ambiguous player query to Series Top Performers")
                s_id = await find_series_smart(s_name, year)
                if s_id:
                     api_results["top_performers"] = await get_series_top_performers(s_id)
            elif tool == "deep_analysis":
                current_s_id = locals().get('s_id', None)
                m_id = await find_match_id(t_name, team2=o_name, series_id=current_s_id, series_name=s_name, year=year)
                if m_id:
                    api_results["deep_match_bundle"] = await fetch_match_context_bundle(m_id)
                    logger.info(f"Deep Analysis Bundle added for match: {m_id}")
            elif tool == "compare_squads" or tool == "get_squad_comparison":
                ctx_logger.warning("Squad comparison feature is currently unavailable.")
            elif tool == "predict_match_analysis":
                ctx_logger.info(f"🔮 Prediction Analysis for {t_name} vs {o_name}")
                if t_name and o_name:
                    p_res = await predict(
                        prediction_type="match_analysis",
                        team_a=t_name,
                        team_b=o_name,
                        date=target_date,
                        venue=entities.get("venue")
                    )
                    api_results["prediction_report"] = p_res
                else:
                    api_results["api_error"] = "To predict a winner, I need two team names. Please specify which teams."
            elif tool == "predict_live_match":
                ctx_logger.info("🔴 Live Match Prediction Requested")
                # Ensure we have live data
                if "live_matches" not in api_results:
                     l_data = await get_live_matches()
                     if l_data: api_results["live_matches"] = l_data
                
                live_source = api_results.get("live_matches")
                target_live = None
                if live_source and isinstance(live_source, dict):
                    all_live = live_source.get("data", [])
                    # Find match matching teams
                    for m in all_live:
                        m_n = _normalize(m.get("name", ""))
                        if (t_name and _is_team_match(t_name, m_n)) or (o_name and _is_team_match(o_name, m_n)):
                            target_live = m
                            break
                    # Fallback: if only one live match, assume that
                    if not target_live and len(all_live) == 1:
                        target_live = all_live[0]
                        
                if target_live:
                     pred_l = await predict_live_match(target_live)
                     api_results["live_win_prediction"] = pred_l
                else:
                     api_results["api_error"] = "No relevant live match found to predict."
        except Exception as e:
            ctx_logger.error(f"Tool {tool} execution failed: {e}")
    # FINAL CHECK: If it's about TODAY, we MUST fetch from Live API even if intent is PAST.
    if is_about_today or intent == "LIVE_MATCH" or any(tool in ["get_live_matches"] for tool in required_tools):
        live_data = await get_live_matches()
        if live_data and live_data:
             api_results["live_matches"] = live_data
        if matches_task is not None:
            m_data = await matches_task
        else:
            m_data = await get_todays_matches()
        today_iso = datetime.now().strftime("%Y-%m-%d")
        active_statuses = ["live", "stumps", "break", "tea", "lunch", "dinner", "innings", "delayed", "interrupted"]
        api_results["generic_today_data"] = [
            m for m in m_data.get("data", [])
            if m.get("date") == today_iso or any(s in str(m.get("status", "")).lower() for s in active_statuses)
        ]
        if not api_results.get("live_matches") and not api_results.get("generic_today_data"):
             api_results["upcoming_broad_schedule"] = m_data.get("data", [])[:10]
        
        # PROACTIVE DISCOVERY: If teams are mentioned and we are looking at TODAY, find the match!
        if (t_name or o_name) and api_results.get("generic_today_data"):
            found_today_match = None
            norm_q = _normalize(user_query)
            for m in api_results["generic_today_data"]:
                m_name = _normalize(m.get("name", ""))
                if (t_name and _is_team_match(t_name, m_name)) or (o_name and _is_team_match(o_name, m_name)):
                    found_today_match = m
                    break
            
            if found_today_match:
                ctx_logger.info(f"🎯 Discovered relevant TODAY match: {found_today_match.get('name')}")
                api_results["today_match_discovery"] = found_today_match
                # If finished, we want the result
                if "finish" in str(found_today_match.get("status", "")).lower():
                    api_results["today_finished_match_result"] = {
                        "match": found_today_match.get("name"),
                        "result": found_today_match.get("status"),
                        "scorecard_hint": "Match finished today. Use the Live API data provided."
                    }
    
    # LOGIC: If it's a GENERAL query, we enable internal knowledge by default
    if intent == "GENERAL":
        api_results["internal_knowledge_allowed"] = True
        if "rag_evidence" not in api_results: api_results["rag_evidence"] = ""
        api_results["rag_evidence"] += "\n[SYSTEM]: This is a general query. Use your internal knowledge to answer."

    elif not api_results and intent not in ["GENERAL", "general_qa"]:
        api_results["search_failed"] = True
        if s_name: api_results["missing_entity"] = f"Series: {s_name} ({year})"
        if p_name: api_results["missing_entity"] = f"Player: {p_name}"
    
    # --- RESULT CLEANUP FOR HISTORICAL FALLBACK ---
    if api_results.get("internal_knowledge_allowed"):
        # If we allowed internal knowledge, we shouldn't show "search failed" or "missing entity"
        # as these flags trigger 'sorry' messages in the AI.
        api_results.pop("search_failed", None)
        api_results.pop("missing_entity", None)
        
        if is_pure_historical:
             # For years <= 2023, the API/DB errors are expected and should be hidden
             # to prevent the AI from apologizing for missing DB data.
             api_results.pop("api_error", None)
             if api_results.get("smart_query_result") == []:
                 api_results.pop("smart_query_result", None)

    # Layer 3: Generate response and verify
    final_response = await generate_human_response(api_results, user_query, analysis, conversation_history)
    
    # LAYER 3: VERIFICATION (ChatGPT-Level Accuracy)
    if api_results and intent not in ["GENERAL", "UPCOMING"]:
        from src.agents.ai_core import verify_response
        ctx_logger.info("🔍 LAYER 3: Verifying response accuracy...")
        detected_lang = analysis.get("language", "english")
        verification_result = await verify_response(user_query, api_results, final_response, detected_lang=detected_lang)
        
        if verification_result.startswith("FAIL"):
            ctx_logger.warning(f"❌ Verification Failed: {verification_result}")
            ctx_logger.info("🔄 Regenerating response with stricter prompt...")
            
            # Regenerate with explicit instruction to stick to data
            final_response = await generate_human_response(
                api_results, 
                user_query, 
                analysis, 
                conversation_history,
                strict_mode=True
            )
            ctx_logger.info("✅ Response regenerated with strict mode")
        else:
            ctx_logger.info(f"✅ Verification Passed: {verification_result}")
    
    ctx_logger.info("--------------------------------------------------")
    ctx_logger.info("              SEARCH/REASONING SUMMARY")
    ctx_logger.info("--------------------------------------------------")
    ctx_logger.info(f"1. INTENT: {intent}")
    ctx_logger.info(f"2. YEAR CONTEXT: {q_years if q_years else year}")
    ctx_logger.info(f"3. TOOLS EXECUTED: {list(api_results.keys())}")
    
    datasource = "LIVE API"
    if is_past_intent:
        if is_pure_historical: datasource = "INTERNAL KNOWLEDGE (GPT Memory)"
        elif is_mixed: datasource = "HYBRID (DB + GPT)"
        else: datasource = "DATABASE (Universal Engine)"
        
    ctx_logger.info(f"4. DATA SOURCE: {datasource}")
    ctx_logger.info("--------------------------------------------------")
    ctx_logger.info("              FINAL AI RESPONSE")
    ctx_logger.info("--------------------------------------------------")
    ctx_logger.info(f"\n{final_response}\n")
    ctx_logger.info("==================================================")
    ctx_logger.info("=== QUERY END ===")
    return final_response