
import asyncio
import json
import streamlit as st
from datetime import datetime

from app.utils_core import get_logger
from app.ai_core import analyze_intent, run_reasoning_agent, generate_human_response
from app.match_utils import _normalize, _is_team_match, _smart_ctx_match

# Service Imports
from app.backend_core import (
    getSeries, get_upcoming_matches, get_todays_matches, get_live_matches,
    get_match_scorecard, get_all_series, get_series_info, get_series_standings,
    fetch_last_finished_match, cricket_api
)
from app.history_service import (
    get_player_past_performance, get_head_to_head_history,
    get_series_history_summary, search_historical_matches,
    get_historical_match_details, get_season_leaders, get_season_records,
    get_season_match_stats, get_team_season_summary, get_all_historical_matches,
    past_db_get_standings, execute_smart_query
)
from app.search_service import (
    find_match_id, find_series_smart, find_match_by_score
)
from app.analytics_service import (
    get_series_analytics, extract_series_winner,
    get_series_final_info, get_player_recent_performance, get_series_matches_by_id,
    get_series_top_performers, handle_tournament_specialist_logic, get_head_to_head_statistics
)
from app.live_match_service import (
    get_live_match_details, fetch_match_context_bundle, calculate_match_odds, extract_live_state
)
from app.squad_service import get_team_squad, compare_team_squads

# Define logger if not available via get_logger redirection inside function
# We will use function-local loggers as in original code

def update_context(series=None, year=None, team=None, player=None, opponent=None):
    if series: st.session_state.chat_context["last_series"] = series
    if year: st.session_state.chat_context["last_year"] = year
    if team: st.session_state.chat_context["last_team"] = team
    if opponent: st.session_state.chat_context["last_opponent"] = opponent
    if player: st.session_state.chat_context["last_player"] = player


async def process_user_message(user_query, conversation_history=None):

    base_logger = get_logger()

    # Pre-fetch tasks
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

    entities = analysis.get("entities", {})

    current_s = entities.get("series") or entities.get("league") or entities.get("tournament")

    if current_s:
        safe_name = "".join(x for x in str(current_s) if x.isalnum() or x in " -_").strip().upper().replace(" ", "_")
        log_name = safe_name
    elif st.session_state.chat_context.get("last_series") and not analysis.get("is_new_topic", False):
        s_name = st.session_state.chat_context.get("last_series")
        safe_name = "".join(x for x in str(s_name) if x.isalnum() or x in " -_").strip().upper().replace(" ", "_")
        log_name = safe_name
    else:
        log_name = "GENERAL"
        
    # Intent to Log File Mapping
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

    # Determine Log File
    log_file = intent_map.get(intent, "GENERAL_QUERIES.log")
    logger_name = log_file.replace(".log", "").lower() + "_logger"

    # Get specific logger for this intent
    ctx_logger = get_logger(logger_name, log_file)

    ctx_logger.info(f"=== NEW QUERY START [{datetime.now().strftime('%H:%M:%S')}] ===")
    ctx_logger.info(f"QUERY: {user_query}")
    ctx_logger.info(f"ANALYSIS: Intent={intent} | Entities={entities}")
    try:
        if hasattr(st, "session_state") and "chat_context" in st.session_state:
             ctx_logger.info(f"CONTEXT: Last_Series={st.session_state.chat_context.get('last_series')}")
    except: pass

    logger = ctx_logger

    # 🧠 NEW: DEEP REASONING AGENT INTERCEPTION
    if intent == "DEEP_REASONING":
        ctx_logger.info("🧠 Triggering ReAct Agent for Deep Reasoning...")
        try:
            agent_response = await run_reasoning_agent(user_query, conversation_history)
            ctx_logger.info(f"ReAct Agent Result: {agent_response}")
            return agent_response
        except Exception as e:
            ctx_logger.error(f"ReAct Agent Failed: {e}")
            # Fallback to standard flow if agent fails
            ctx_logger.warning("Falling back to standard router...")

    required_tools = analysis.get("required_tools", [])
    entities = analysis.get("entities", {})
    language = analysis.get("language", "english")
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

    if intent == "PAST_HISTORY":
        display_series = entities.get("series") or entities.get("league") or entities.get("tournament")
        has_tournament = bool(display_series)
        match_order = entities.get("match_order")

        if not has_tournament and (match_order == -1 or "last" in user_query.lower()):
            match_type = entities.get("match_type")
            ctx_logger.info(f"Searching for Global Last Match (Type: {match_type})")
            last_match = await fetch_last_finished_match(match_type=match_type)
            if last_match:
                 api_results["score_match_details"] = last_match
                 api_results["match_found_via_score"] = True
                 ctx_logger.info(f"Found generic last match: {last_match.get('name')}")

    if intent == "PAST_HISTORY" and stats_type in ["winner", "scorecard", "venue", "final", "record", "aggregate_stats", "standings", "comparison", "none", "aggregate", "general"]:
        ctx_logger.info(f"🔎 Entering PAST_HISTORY History Block. Series: {entities.get('series')}, Year: {entities.get('year')}")
        
        display_series = entities.get("series") or entities.get("league") or entities.get("tournament")
        has_tournament = bool(display_series)
        has_teams = bool(entities.get("team") or entities.get("opponent"))
        has_year = entities.get("year") is not None
        asks_location = entities.get("venue") is not None or intent == "VENUE_QUERY"
        asks_winner = stats_type in ["winner", "scorecard", "final", "standings", "comparison"] or entities.get("match_order") == -1

        # Check for general "fetch all" or broad history requests if no specific entities are found
        is_general_history = "all" in user_query.lower() or "database" in user_query.lower() or "sabhi" in user_query.lower()
        
        if is_general_history and not (has_tournament or has_teams):
            ctx_logger.info("Explict request for ALL/GENERAL historical matches from DB")
            all_hist_matches = await get_all_historical_matches(limit=20)
            if all_hist_matches:
                 api_results["historical_match_focus"] = all_hist_matches
                 api_results["match_found_via_score"] = True
                 ctx_logger.info(f"Retrieved {len(all_hist_matches)} general historical matches.")

        if (has_tournament or has_teams):
            query_year = entities.get("year") or year
            current_year = datetime.now().year
            series_name = entities.get("series") or s_name or ""
            target_sid = None

            past_subtype = str(analysis.get("past_subtype", "UNKNOWN")).upper()
            stats_type = str(analysis.get("stats_type", "NONE")).upper()
            
            is_archived = past_subtype == "ARCHIVED" or (query_year is None and intent == "PAST_HISTORY")
            if not is_archived and query_year:
                try:
                    if int(query_year) < current_year:
                        is_archived = True
                except: pass

            is_recent_past = past_subtype == "RECENT_PAST" or (query_year and str(query_year) == str(current_year))
            
            if query_year is None:
                 ctx_logger.info("🔍 ALL-TIME Query Context identified.")
            
            ctx_logger.info(f"🔍 Data Strategy: Subtype={past_subtype}, Archived={is_archived}, Recent={is_recent_past}")

            is_specific_match_query = stats_type in ["scorecard", "winner", "final"]

            if is_archived:
                ctx_logger.info(f"📚 ARCHIVED YEAR DETECTED ({query_year}) → Fetching from LOCAL DATABASE")

                if is_specific_match_query:
                    search_term = series_name or user_query
                    clean_term = search_term.replace("match", "").replace("score", "").replace("result", "").strip()
                    if "final" in user_query.lower() and "final" not in clean_term.lower():
                        clean_term += " Final"

                    # 1. Try Specific Search First (Exact Match Name)
                    match_details = await get_historical_match_details(clean_term, year=query_year)

                    if match_details:
                         api_results["historical_match_focus"] = match_details
                         ctx_logger.info(f"✅ Found Specific Match Detail in DB: {match_details.get('name')}")
                    else:
                        # 2. flexible search using all available entities
                        target_d = entities.get("target_date")
                        found_matches = await search_historical_matches(
                            query=clean_term if len(clean_term) > 3 else None,
                            team=entities.get("team") or entities.get("opponent"),
                            year=query_year,
                            date=target_d,
                            series=series_name,
                            limit=5
                        )
                        if found_matches:
                            final_match = next((m for m in found_matches if "final" in m.get("name", "").lower()), None)
                            selected_match = final_match if final_match else found_matches[0]
                            full_m = await get_historical_match_details(selected_match["name"], year=query_year or selected_match.get("year"))
                            api_results["historical_match_focus"] = full_m or selected_match
                            ctx_logger.info(f"✅ Found Match via Flexible Search: {selected_match.get('name')}")
                            if len(found_matches) > 1 and not final_match:
                                 api_results["ambiguous_matches"] = found_matches
                                 ctx_logger.info(f"⚠️ Found {len(found_matches)} matches, selected top 1 but flagged ambiguity")
                        else:
                             ctx_logger.warning("No matches found via flexible search.")

                uq = user_query.lower()
                leader_category = None
                if ("orange" in uq or "run" in uq) and "high" not in uq: leader_category = "runs"
                elif "purple" in uq or "wicket" in uq: leader_category = "wickets"
                elif "six" in uq: leader_category = "sixes"
                elif "four" in uq: leader_category = "fours"
                elif "high" in uq and "score" in uq: leader_category = "highest_score"
                elif "best" in uq and "bowl" in uq: leader_category = "best_bowling"
                elif "mvp" in uq or "valuable" in uq or "point" in uq: leader_category = "points"

                if leader_category:
                     leaders = await get_season_leaders(query_year, category=leader_category, series_name=series_name)
                     if leaders:
                         api_results["historical_season_leaders"] = leaders
                         ctx_logger.info(f"✅ Found Season Leaders ({leader_category}) in DB")

                if "high" in uq and "total" in uq:
                    api_results["historical_season_totals"] = await get_season_match_stats(query_year, type="highest", series_name=series_name)
                    ctx_logger.info(f"✅ Found Season Match Totals (highest) in DB for {series_name}")
                elif "low" in uq and "total" in uq:
                    api_results["historical_season_totals"] = await get_season_match_stats(query_year, type="lowest", series_name=series_name)
                    ctx_logger.info(f"✅ Found Season Match Totals (lowest) in DB for {series_name}")

                team_in_query = entities.get("team") or entities.get("opponent")
                if team_in_query and query_year and not is_specific_match_query:
                    team_sum = await get_team_season_summary(team_in_query, query_year)
                    if team_sum:
                        api_results["historical_team_season_summary"] = team_sum
                        ctx_logger.info(f"✅ Found Team Season Summary for {team_in_query}")

                if stats_type == "record":
                    record_data = await get_season_records(query_year, user_query)
                    if record_data:
                        api_results["historical_record"] = record_data
                        ctx_logger.info(f"✅ Found Historical Record ({record_data['category']}) in DB (Logic Driven)")

                series_summary = await get_series_history_summary(series_name or display_series, year=query_year)
                if series_summary:
                    api_results["historical_db_series_summary"] = series_summary
                    ctx_logger.info(f"✅ Found historical series summary in DB for {series_name} {query_year}")
                    
                    is_comp = any(w in user_query.lower() for w in ["compare", "comparison", "vs", "tulna", "muqabla"])
                    if is_comp:
                        standings = series_summary.get("standings", [])
                        t_name_query = entities.get("team")
                        o_name_query = entities.get("opponent")
                        comp_data = {}
                        if len(standings) >= 2:
                            if t_name_query and o_name_query:
                                comp_data["team1"] = next((s for s in standings if t_name_query.lower() in s['team'].lower()), standings[0])
                                comp_data["team2"] = next((s for s in standings if o_name_query.lower() in s['team'].lower()), standings[1])
                            else:
                                comp_data["team1"] = standings[0]
                                comp_data["team2"] = standings[1]
                                comp_data["is_top_two"] = True
                            api_results["historical_comparison_data"] = comp_data
                            ctx_logger.info(f"📊 Prepared historical comparison data for {comp_data['team1']['team']} vs {comp_data['team2']['team']}")
                else:
                    ctx_logger.warning(f"⚠️ No DB data for {series_name} {query_year}, falling back to API")
                    if series_name:
                        target_sid = await find_series_smart(series_name, year=query_year)
            else:
                ctx_logger.info(f"🌐 RECENT PAST / CURRENT YEAR ({query_year}) → Fetching from API for latest data")
                try:
                    target_date_str = entities.get("target_date")
                    parsed_dt = None
                    if target_date_str:
                         try: parsed_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
                         except: pass

                    if not parsed_dt:
                        parsed_dt = datetime.now() 
                    
                    target_date = str(parsed_dt).split(" ")[0]

                except: target_date = None

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
                
                # Fetch statistics too
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
                    if not t_name and not o_name:
                         an_res.pop("team_match_sequence", None)
                    an_res.pop("completed_matches", None)
                    api_results["series_analytics"] = an_res
                    sq = analysis.get("entities", {}).get("score_mentioned")
                    sd = analysis.get("entities", {}).get("score_details")
                    val = sd.get("value") if sd else None
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
                        if stats_type in ["aggregate", "historical", "standings", "aggregate_stats"]:
                            is_analytical_intent = True
                            if is_analytical_intent:
                                api_results["series_analytics"] = await get_series_analytics(s_id, deep_scan=True)
                                logger.info(f"Proactive Series Analytics added for {s_id} (Stats Type: {stats_type})")
                            important_matches = []
                            q_words = _normalize(user_query)
                            candidates = []
                            team_seq = api_results.get("series_analytics", {}).get("team_match_sequence", {})
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
                     api_results["standings"] = past_db_get_standings(year, s_name)
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
                     # Only pass a specific date if the user explicitly mentioned it (found in entities)
                     # Otherwise, let the service default to the next 14 days.
                     force_date = entities.get("target_date")
                     upcoming_res = await get_upcoming_matches(check_date=force_date)
                     api_results["upcoming_schedule"] = upcoming_res.get("data", [])[:15]

            elif tool == "get_player_performance" and p_name:
                target_series_id = None
                current_year = datetime.now().year
                query_year = entities.get("year") or year
                is_past_year = query_year and int(query_year) < current_year
                is_historical = is_archived or (query_year and int(query_year) < current_year)
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
                active_t1 = t_name
                active_t2 = o_name
                if not active_t1 or not active_t2:
                    winner_info = api_results.get("series_winner_info")
                    if winner_info:
                        pt = winner_info.get("points_table", [])
                        is_top_request = any(kw in user_query.lower() for kw in ["top 2", "top two", "standings", "points table"])
                        if is_top_request and len(pt) >= 2:
                            if not active_t1: active_t1 = pt[0]["team"]
                            if not active_t2: active_t2 = pt[1]["team"]
                            ctx_logger.info(f"Identified TOP 2 from Points Table: {active_t1} vs {active_t2}")
                        else:
                            if not active_t1: active_t1 = winner_info.get("winner")
                            if not active_t2: active_t2 = winner_info.get("runner_up")
                            ctx_logger.info(f"Identified teams from winner/runner-up info: {active_t1} vs {active_t2}")
                if active_t1 and active_t2:
                    ctx_logger.info(f"Comparing squads: {active_t1} vs {active_t2}")
                    comparison = await compare_team_squads(active_t1, active_t2, s_name, year)
                    api_results["squad_comparison"] = comparison
                    api_results["comparison_teams"] = {"team1": active_t1, "team2": active_t2}
                elif active_t1:
                    ctx_logger.info(f"Fetching squad for: {active_t1}")
                    squad = await get_team_squad(active_t1, s_name, year)
                    api_results["team_squad"] = squad
                else:
                    ctx_logger.warning("Squad comparison requested but no teams specified")

                if tool not in ["get_live_matches", "get_upcoming_matches", "upcoming_schedule", "extract_series_winner", "get_match_scorecard", "compare_squads", "get_points_table"]:
                     ctx_logger.warning(f"Unmapped tool call: {tool}")

        except Exception as e:
            ctx_logger.error(f"Tool {tool} execution failed: {e}")

    if intent == "LIVE_MATCH" or any(tool in ["get_live_matches"] for tool in required_tools):
        # 1. Try Fetching Strictly Live Matches first
        live_data = await get_live_matches()
        
        # 2. If live matches exist, prioritize them
        if live_data and live_data:
             api_results["live_matches"] = live_data
        
        # 3. Always fetch today's schedule as context/fallback
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
        
        if not api_results.get("live_matches") and not api_results["generic_today_data"]:
             api_results["upcoming_broad_schedule"] = m_data.get("data", [])[:10]

    elif not api_results and intent != "general_qa":
        api_results["search_failed"] = True
        if s_name: api_results["missing_entity"] = f"Series: {s_name} ({year})"
        if p_name: api_results["missing_entity"] = f"Player: {p_name}"

    ctx_logger.info(f"Reasoning complete. Tools: {list(api_results.keys())}")
    ctx_logger.info(f"FULL API DATA BUNDLE: {json.dumps(api_results, default=str, ensure_ascii=False)[:32000]}")
    ctx_logger.info("--- GENERATING FINAL RESPONSE ---")

    final_response = await generate_human_response(api_results, user_query, analysis, conversation_history)
    ctx_logger.info(f"Process Finished.")
    ctx_logger.info(f"Final AI Response: {final_response[:200]}...")
    ctx_logger.info("=== QUERY END ===")

    return final_response
