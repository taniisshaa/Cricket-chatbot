import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
load_dotenv(override=True)
import streamlit as st
import asyncio
import json
import random
import os
from datetime import datetime
from app.ai_core import (
    analyze_intent,
    generate_human_response,
    predict,
    predict_player_performance,
    predict_live_match
)

from app.live_match_service import fetch_realtime_matches
from app.history_service import (
    get_player_past_performance,
    get_head_to_head_history,
    get_series_history_summary,
    search_historical_matches,
    get_historical_match_details
)

from app.backend_core import (
    getSeries, get_upcoming_matches, get_todays_matches, get_live_matches,
    get_match_scorecard, get_all_series, get_series_info, get_series_standings
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

from app.utils_core import (

    get_logger,

    load_chat,

    save_chat,

    generate_chat_pdf,

    Config

)

logger = get_logger("app_main", "general_app.log")

try:

    SMART_ROUTER_ENABLED = True

except ImportError:

    SMART_ROUTER_ENABLED = False

Config.ensure_dirs()

if "messages" not in st.session_state: st.session_state.messages = load_chat()

if "processing" not in st.session_state: st.session_state.processing = False

if "chat_context" not in st.session_state:

    st.session_state.chat_context = {

        "last_series": None,

        "last_year": None,

        "last_team": None,

        "last_opponent": None,

        "last_player": None

    }

st.markdown("""

    <style>

    /* Sidebar styling */

    [data-testid="stSidebar"] {

        background-color: #f8f9fa;

        border-right: 1px solid #dee2e6;

    }

    [data-testid="stSidebar"] .stMarkdown h1 {

        color: #212529 !important;

    }

    /* Global Background (Optional, but makes it consistent) */

    .stApp {

        background-color: #ffffff;

    }

    /* Button styling */

    .stButton>button {

        width: 100%;

        border-radius: 8px;

        height: 3.2em;

        background-color: #ffffff;

        color: #212529;

        border: 1px solid #dee2e6;

        font-weight: 500;

        box-shadow: 0 1px 2px rgba(0,0,0,0.05);

        transition: all 0.2s ease;

    }

    .stButton>button:hover {

        background-color: #f1f3f5;

        border-color: #ced4da;

        color: #000000;

        transform: translateY(-1px);

    }

    /* Special styling for New Chat button */

    .new-chat-btn>button {

        background-color: #2ea043 !important;

        color: white !important;

        border: none !important;

    }

    .new-chat-btn>button:hover {

        background-color: #2c974b !important;

        box-shadow: 0 4px 12px rgba(46, 160, 67, 0.2) !important;

    }

    /* Chat Text colors for light mode */

    .stMarkdown {

        color: #212529;

    }

    </style>

""", unsafe_allow_html=True)

with st.sidebar:

    st.markdown('<div class="new-chat-btn">', unsafe_allow_html=True)

    if st.button("New Chat"):

        st.session_state.messages = []

        st.session_state.chat_context = {

            "last_series": None, "last_year": None, "last_team": None, "last_player": None

        }

        save_chat([])

        st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)

    st.write("---")

    if st.button("Clear History"):

        st.session_state.messages = []

        st.session_state.chat_context = {

            "last_series": None, "last_year": None, "last_team": None, "last_player": None

        }

        save_chat([])

        st.success("History Cleared!")

        st.rerun()

    st.write("---")

    if st.session_state.get("messages"):

        try:

            pdf_bytes = generate_chat_pdf(st.session_state.messages)

            if pdf_bytes:

                st.download_button(

                    label="📥 Download PDF",

                    data=pdf_bytes,

                    file_name=f"cricket_chat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",

                    mime="application/pdf"

                )

            else:

                st.error("Could not generate PDF history.")

        except Exception as e:

            st.error(f"Error generating PDF: {e}")

def update_context(series=None, year=None, team=None, player=None, opponent=None):

    if series: st.session_state.chat_context["last_series"] = series

    if year: st.session_state.chat_context["last_year"] = year

    if team: st.session_state.chat_context["last_team"] = team

    if opponent: st.session_state.chat_context["last_opponent"] = opponent

    if player: st.session_state.chat_context["last_player"] = player

def _normalize(s):

    return (s or "").strip().lower()

def _is_initials_match(query, name):

    """

    Checks if 'query' is an acronym/initials for 'name'.

    e.g. "MI" -> "Mumbai Indians" (True)

         "CSK" -> "Chennai Super Kings" (True)

    """

    if not query or not name: return False

    q = _normalize(query).replace(" ", "")

    n_parts = _normalize(name).split()

    initials = "".join([p[0] for p in n_parts if p])

    if q == initials: return True

    if len(q) >= 2 and q in initials: return True

    return False

def _smart_ctx_match(m, scope):

    """

    Checks if match 'm' is relevant to 'scope' (team/series), handling aliases dynamically.

    """

    if not scope: return False

    scope_norm = _normalize(scope)

    name_norm = _normalize(m.get("name"))

    if scope_norm in name_norm: return True

    tag = _normalize(m.get("_matched_entity") or "")

    if tag and (scope_norm in tag or tag in scope_norm): return True

    if _is_initials_match(scope_norm, name_norm): return True

    return False

def _is_team_match(team, match_name):

    """

    Robustly checks if a team name matches a match name using logic rather than hardcoding.

    Handles acronyms (RCB, CSK), partial names (Royal Challengers), and variations.

    """

    if not team or not match_name: return False

    t_norm = _normalize(team)

    m_norm = _normalize(match_name)

    if t_norm in m_norm: return True

    if _is_initials_match(team, match_name) or _is_initials_match(match_name, team):

        return True

    t_tokens = [w for w in t_norm.split() if len(w) > 2]

    if not t_tokens: t_tokens = t_norm.split()

    matches = [w for w in t_tokens if w in m_norm]

    if t_tokens and len(matches) / len(t_tokens) >= 0.5:

        return True

    if len(t_norm) <= 5 and not t_norm.isnumeric():

        m_tokens = [w for w in m_norm.split() if len(w) > 2]

        m_initials = "".join([w[0] for w in m_tokens if w])

        if t_norm in m_initials: return True

    return False

def _is_finished(m):

    """

    Determines if a match is finished using a hierarchy of checks:

    1. API Flags (matchEnded, matchWinner)

    2. Cricket Rules Logic (Scores, Overs, Wickets)

    3. Failure Safety (Status text)

    """

    if m.get("matchEnded"): return True

    if m.get("matchWinner") or m.get("winner"): return True

    try:

        score = m.get("score")

        if score and isinstance(score, list):

            m_type = str(m.get("matchType", "")).lower()

            max_overs = 20 if "t20" in m_type else 50 if "odi" in m_type else 100

            if "test" in m_type: max_overs = 0

            inn1 = next((s for s in score if "Inning 1" in s.get("inning", "")), None)

            inn2 = next((s for s in score if "Inning 2" in s.get("inning", "")), None)

            if inn1 and inn2:

                r1 = int(inn1.get("r", 0))

                r2 = int(inn2.get("r", 0))

                w2 = int(inn2.get("w", 0))

                o2 = float(inn2.get("o", 0))

                if r2 > r1: return True

                if w2 >= 10: return True

                if max_overs > 0 and o2 >= max_overs: return True

    except Exception:

        pass

    status = str(m.get("status", "")).lower()

    if "won by" in status: return True

    return False

async def process_user_message(user_query, conversation_history=None):

    base_logger = get_logger()

    from app.backend_core import get_todays_matches, get_all_series

    from app.live_match_service import fetch_realtime_matches

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
    ctx_logger.info(f"CONTEXT: Last_Series={st.session_state.chat_context.get('last_series')}")

    # Use this logger for the rest of the function if needed, or rely on service-level logging
    # logic below uses 'logger' variable, let's update it or just let services handle their own
    logger = ctx_logger

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
        return needs_clarification # Return directly to st.chat_message without API calls

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
        from app.backend_core import fetch_last_finished_match
        from app.history_service import get_series_history_summary, get_historical_match_details, get_season_leaders, get_season_records, get_season_match_stats, get_team_season_summary

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

    if intent == "PAST_HISTORY" and stats_type in ["winner", "scorecard", "venue", "final", "record", "aggregate_stats", "standings", "comparison"]:

        display_series = entities.get("series") or entities.get("league") or entities.get("tournament")

        has_tournament = bool(display_series)
        has_teams = bool(entities.get("team") or entities.get("opponent"))

        has_year = entities.get("year") is not None

        asks_location = entities.get("venue") is not None or intent == "VENUE_QUERY"

        asks_winner = stats_type in ["winner", "scorecard", "final", "standings", "comparison"] or entities.get("match_order") == -1

        # Check for general "fetch all" or broad history requests if no specific entities are found
        is_general_history = "all" in user_query.lower() or "database" in user_query.lower() or "sabhi" in user_query.lower()
        
        if is_general_history and not (has_tournament or has_teams):
            from app.history_service import get_all_historical_matches
            
            ctx_logger.info("Explict request for ALL/GENERAL historical matches from DB")
            all_hist_matches = await get_all_historical_matches(limit=20)
            
            if all_hist_matches:
                 api_results["historical_match_focus"] = all_hist_matches
                 api_results["match_found_via_score"] = True
                 ctx_logger.info(f"Retrieved {len(all_hist_matches)} general historical matches.")

        if (has_tournament or has_teams) and (has_year or asks_location or asks_winner):

            query_year = entities.get("year") or year
            current_year = datetime.now().year

            series_name = entities.get("series") or s_name or ""

            target_sid = None


            past_subtype = str(analysis.get("past_subtype", "UNKNOWN")).upper()
            stats_type = str(analysis.get("stats_type", "NONE")).upper()
            
            # Treat None (All-time) year as ARCHIVED for stats/history queries
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

                    # Generic flexible search
                    from app.history_service import search_historical_matches

                    search_term = series_name or user_query
                    # Clean search term to avoid noise
                    clean_term = search_term.replace("match", "").replace("score", "").replace("result", "").strip()

                    # If "Final" is requested, append it to refine search but don't hardcode it for just "Final" string
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
                            # Heuristic: If one selected_match is clearly the "Final", pick it
                            final_match = next((m for m in found_matches if "final" in m.get("name", "").lower()), None)
                            
                            selected_match = final_match if final_match else found_matches[0]
                            
                            # Fetch full details
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

                # --- 4. Season Match Stats (Highest/Lowest Totals) ---
                if "high" in uq and "total" in uq:
                    api_results["historical_season_totals"] = await get_season_match_stats(query_year, type="highest", series_name=series_name)
                    ctx_logger.info(f"✅ Found Season Match Totals (highest) in DB for {series_name}")
                elif "low" in uq and "total" in uq:
                    api_results["historical_season_totals"] = await get_season_match_stats(query_year, type="lowest", series_name=series_name)
                    ctx_logger.info(f"✅ Found Season Match Totals (lowest) in DB for {series_name}")

                # --- 5. Team Season Summary ---
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
                    
                    # --- Strategic Comparison Logic ---
                    is_comp = any(w in user_query.lower() for w in ["compare", "comparison", "vs", "tulna", "muqabla"])
                    if is_comp:
                        standings = series_summary.get("standings", [])
                        t_name_query = entities.get("team")
                        o_name_query = entities.get("opponent")
                        
                        comp_data = {}
                        if len(standings) >= 2:
                            if t_name_query and o_name_query:
                                # Compare specific teams
                                comp_data["team1"] = next((s for s in standings if t_name_query.lower() in s['team'].lower()), standings[0])
                                comp_data["team2"] = next((s for s in standings if o_name_query.lower() in s['team'].lower()), standings[1])
                            else:
                                # Compare top 2
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
                    from dateparser.search import search_dates
                    found_dates = search_dates(user_query, settings={'PREFER_DATES_FROM': 'past'})
                    parsed_dt = found_dates[0][1] if found_dates else None

                    if parsed_dt and (parsed_dt.date() < datetime.now().date()):
                        fmt_date = parsed_dt.strftime("%Y-%m-%d")
                        ctx_logger.info(f"📅 Identified specific past date in query: {fmt_date}")

                        t_name = entities.get("team")
                        o_name = entities.get("opponent")
                        recent_res = await get_live_matches(date=fmt_date, team=t_name, opponent=o_name)
                        recent_matches = recent_res.get("data", []) if isinstance(recent_res, dict) else []

                        if recent_matches:
                            api_results["historical_match_focus"] = recent_matches if len(recent_matches) > 1 else recent_matches[0]
                            api_results["match_found_via_score"] = True
                            ctx_logger.info(f"✅ Found {len(recent_matches)} Recent Matches via API Date Search.")
                        else:
                            # Sub-fallback 1: Search local DB for this date (in case it was synced)
                            db_res = await search_historical_matches(date=fmt_date, team=t_name or o_name)
                            if db_res:
                                api_results["historical_match_focus"] = db_res[0]
                                api_results["ambiguous_matches"] = db_res if len(db_res) > 1 else None
                                api_results["match_found_via_score"] = True
                                ctx_logger.info(f"✅ Found {len(db_res)} matches in local DB for the date {fmt_date}")
                            else:
                                # Sub-fallback 2: Try Last Year same date? (Optional but "proper" for trivia)
                                pass
                                
                            # Sub-fallback 3: Last completed match for these teams
                            if (t_name or o_name) and is_specific_match_query:
                                last_m = await fetch_last_finished_match(team_name=t_name, opponent_name=o_name)
                                if last_m:
                                    api_results["historical_match_focus"] = last_m
                                    api_results["match_found_via_score"] = True
                                    ctx_logger.info(f"✅ Found Last Completed Match for {t_name} vs {o_name}: {last_m.get('name')}")
                    else:
                        # No date found, but maybe team stats?
                        t_name = entities.get("team")
                        o_name = entities.get("opponent")
                        if (t_name or o_name) and is_specific_match_query:
                            last_m = await fetch_last_finished_match(team_name=t_name, opponent_name=o_name)
                            if last_m:
                                api_results["historical_match_focus"] = last_m
                                api_results["match_found_via_score"] = True
                                ctx_logger.info(f"✅ Found Last Completed Match for {t_name} vs {o_name}: {last_m.get('name')}")
                except Exception as e:
                    ctx_logger.error(f"Error in recent date/team match search: {e}")

            if series_name and not is_archived:

                 ctx_logger.info(f"Searching for Series ID: {series_name} ({query_year})")

                 target_sid = await find_series_smart(series_name, year=query_year)

                 ctx_logger.info(f"Series ID found: {target_sid}")

            if not target_sid and not is_archived:

                 ctx_logger.info("Specific series name not found, trying dynamic full-query search...")

                 target_sid = await find_series_smart(user_query, year=query_year)

            final_match = None

            if target_sid:

                 ctx_logger.info(f"Fetching Series Final for SID: {target_sid}")

                 final_match = await get_series_final_info(series_id=target_sid)

            else:

                 if has_year and not is_archived:

                     logger.info(f"Fallback: Searching for any final match in year {query_year}")

                     final_match = None

            if final_match and final_match.get("data"):

                api_results["final_match_info"] = final_match["data"]

            if target_sid:
                 winner_info = await extract_series_winner(target_sid)
                 if winner_info:
                     api_results["series_winner_info"] = winner_info

    if stats_type in ["aggregate_stats", "winner"] or intent == "SERIES_STATS":

        query_year = entities.get("year") or year

        series_name = entities.get("series")

        target_sid = None

        if series_name:

             target_sid = await find_series_smart(series_name, year=query_year)

        if not target_sid:

            ctx_logger.info("Specific series name not found for stats, trying dynamic full-query search...")

            target_sid = await find_series_smart(user_query, year=query_year)

        if not target_sid and query_year:

             if not target_sid and query_year:

                  ctx_logger.info(f"Last resort: API search for series in {query_year}")

                  try:

                       y_res = await getSeries(str(query_year))

                       if y_res.get("data"):

                            target_sid = y_res["data"][0]["id"]

                  except: pass

        if target_sid:

            query_lower = user_query.lower()

            analytics_res = await get_series_analytics(target_sid)

            if analytics_res:

                 api_results["series_stats"] = analytics_res

        else:

             ctx_logger.info("Could not resolve series ID for stats query.")

             pass

    for tool in required_tools:

        ctx_logger.info(f"Executing Tool: {tool}")

        try:

            target_date = analysis.get("entities", {}).get("target_date")

            score_query = analysis.get("entities", {}).get("score_mentioned")

            score_info = analysis.get("entities", {}).get("score_details")

            if score_query or (score_info and score_info.get("value") is not None):

                pass

                score_match = await find_match_by_score(t_name, score_query, year=year, series_name=s_name, score_details=score_info)

                if score_match:

                    api_results["score_match_details"] = score_match

                    api_results["match_found_via_score"] = True

            if tool == "get_live_matches" or (intent in ["LIVE_MATCH", "MATCH_ON_DATE", "VENUE_QUERY"] and not api_results):

                v_q = analysis.get("entities", {}).get("venue")

                if not target_date and intent in ["LIVE_MATCH", "GENERAL"]:

                    real_data = await fetch_realtime_matches()
                    live_res = {"ok": True, "status": "Realtime Service", "data": real_data}
                    daily_res = await (matches_task if matches_task else get_todays_matches(use_cache=False))
                else:
                    live_res, daily_res = await asyncio.gather(
                        get_live_matches(date=target_date, venue=v_q, use_cache=False),
                        matches_task if matches_task else get_todays_matches(use_cache=False)
                    )

                ctx_logger.info(f"Live Matches Fetched: {len(live_res.get('data', []))}")

                ctx_logger.info(f"Daily Matches Fetched: {len(daily_res.get('data', []))}")

                real_live = []

                today_completed = []

                today_upcoming = []

                combined_pool = (live_res.get("data", []) + daily_res.get("data", []))

                seen_ids = set()
                unique_matches = []


                filter_series = entities.get("series") or entities.get("league") or entities.get("tournament")
                filter_type = entities.get("match_type")


                def _norm(x): return str(x or "").lower().strip()

                for m in combined_pool:
                    if m["id"] in seen_ids: continue
                    seen_ids.add(m["id"])


                    if filter_series:
                        fs = _norm(filter_series)
                        m_s = _norm(m.get("serie", {}).get("name"))
                        m_l = _norm(m.get("league", {}).get("name"))
                        m_n = _norm(m.get("name"))

                        if fs not in m_s and fs not in m_l and fs not in m_n:


                            continue

                    if filter_type:
                        ft = _norm(filter_type)
                        m_type = _norm(m.get("matchType"))
                        m_l = _norm(m.get("league", {}).get("name"))
                        if ft not in m_type and ft not in m_l:
                            continue

                    unique_matches.append(m)

                for m in unique_matches:

                    st_text = str(m.get("status", "")).upper()

                    if st_text in ["LIVE", "INNINGS BREAK", "TEA BREAK", "LUNCH", "DINNER", "DRINKS",

                                   "1ST INNING", "2ND INNING", "3RD INNING", "4TH INNING",

                                   "STUMPS", "INTERRUPTED", "DELAYED"]:

                         real_live.append(m)

                    elif st_text in ["FINISHED", "COMPLETED", "MATCH ENDED", "ABANDONED", "NO RESULT", "CANCELLED", "POSTPONED"] or "WON BY" in st_text:

                         today_completed.append(m)

                    else:

                         today_upcoming.append(m)

                api_results["live_matches"] = real_live
                api_results["today_completed"] = today_completed
                api_results["today_upcoming"] = today_upcoming

                ctx_logger.info(f"Categorized: Live={len(real_live)}, Completed={len(today_completed)}, Upcoming={len(today_upcoming)}")

                if real_live:
                    api_results["has_real_live_matches"] = True
                else:
                    api_results["no_live_matches_found_message"] = "No matches are currently live."

                if live_res.get("status") and not live_res.get("data"):
                    api_results["live_matches_status"] = live_res.get("status")

            # Removed fetch_historical_series_data call as it is redundant
            elif tool == "search_historical_matches":
                from app.history_service import search_historical_matches
                q = analysis.get("query") if analysis.get("query") else user_query
                t = entities.get("team") or entities.get("opponent")
                y = entities.get("year")
                d = entities.get("target_date")
                s = entities.get("series")
                
                db_matches = await search_historical_matches(query=q, team=t, year=y, date=d, series=s)
                if db_matches:
                    api_results["historical_matches_list"] = db_matches
                    ctx_logger.info(f"✅ Found {len(db_matches)} matches in local DB")
                
            elif tool == "get_series_info" and s_name:
                pass # Already handled or will be fetched

            target_iso = target_date or datetime.now().strftime("%Y-%m-%d")

            deep_discovered = []

            target_entities = []

            ent_s = entities.get("series")

            ent_t = entities.get("team")

            if ent_s: target_entities.append(ent_s)

            elif ent_t: target_entities.append(ent_t)

            elif s_name and intent != "LIVE_MATCH":

                target_entities.append(s_name)

            if (target_date or intent == "LIVE_MATCH") and not target_entities:

                fast_scan_res = await get_todays_matches(offset=0)

                if fast_scan_res.get("data"):

                    fast_matches = fast_scan_res.get("data", [])

                    matches_on_dt = [m for m in fast_matches if m.get("date") == target_iso]

                    if matches_on_dt:

                        ctx_logger.info(f"Fast Scan found {len(matches_on_dt)} matches on date {target_iso}")

                        deep_discovered.extend(matches_on_dt)

                    else:

                        fast_scan_res_2 = await get_todays_matches(offset=50)

                        fast_matches_2 = fast_scan_res_2.get("data", [])

                        matches_on_dt_2 = [m for m in fast_matches_2 if m.get("date") == target_iso]

                        if matches_on_dt_2:

                            deep_discovered.extend(matches_on_dt_2)

                    if not deep_discovered and not matches_on_dt:

                        series_data = await get_all_series()

                        active_series = series_data.get("data", [])[:10]

                        force_flag = str(datetime.now().year) in target_iso

                        series_tasks = [get_series_matches_by_id(s["id"], force_api=force_flag) for s in active_series]

                        series_responses = await asyncio.gather(*series_tasks)

                        for s_data in series_responses:

                            deep_discovered.extend([m for m in s_data.get("data", []) if m.get("date") == target_iso])

                if target_entities:

                    for entity in target_entities:

                        combined_recent = (live_res.get("data", []) or []) + (daily_res.get("data", []) or [])

                        fallback_matches = []

                        norm_entity = _normalize(entity)

                        for m in combined_recent:

                            m_name = _normalize(m.get("name"))

                            if norm_entity in m_name:

                                status_norm = _normalize(m.get("status"))

                                if m.get("date") == target_iso or m.get("matchStarted") or m.get("matchEnded") or "won" in status_norm:

                                    fallback_matches.append(m)

                        if fallback_matches:

                            deep_discovered.extend(fallback_matches)

                            for m in fallback_matches:

                                m["_matched_entity"] = _normalize(entity)

                            continue

                        s_id_search = await find_series_smart(entity, year)

                        if s_id_search and "ERROR:" not in str(s_id_search):

                            force_flag = str(datetime.now().year) in target_iso

                            s_data = await get_series_matches_by_id(s_id_search, force_api=force_flag)

                            matches_on_dt = [m for m in s_data.get("data", []) if m.get("date") == target_iso]

                            if matches_on_dt:

                                deep_discovered.extend(matches_on_dt)

                                for m in matches_on_dt:

                                    m["_matched_entity"] = _normalize(entity)

                                    if entity.lower() not in m["name"].lower():

                                        m["name"] = f"[{entity}] {m['name']}"

                if not deep_discovered and target_date:

                    pass

                all_matches = live_res.get("data", []) + daily_res.get("data", []) + deep_discovered

                unique_pool = {m["id"]: m for m in all_matches}.values()

                current_series = entities.get("series")

                current_team = entities.get("team")

                if current_series or current_team:

                    scope_filter = _normalize(current_series or current_team)

                    running = [m for m in unique_pool if m.get("matchStarted") and not _is_finished(m) and _smart_ctx_match(m, scope_filter)]

                    finished = [m for m in unique_pool if _is_finished(m) and _smart_ctx_match(m, scope_filter)]

                    upcoming = [m for m in unique_pool if not m.get("matchStarted") and not _is_finished(m) and _smart_ctx_match(m, scope_filter)]

                    api_results["matches_in_scope"] = {"running": running, "finished": finished, "upcoming": upcoming}

                else:

                    api_results["matches_in_scope"] = {

                        "running": [m for m in unique_pool if m.get("matchStarted") and not _is_finished(m)],

                        "finished": [m for m in unique_pool if _is_finished(m)]

                    }

                if current_series or current_team:

                    s_filter = _normalize(current_series or current_team)

                    def _is_match(m):

                        return _smart_ctx_match(m, s_filter)

                    api_results["today_action_summary"] = {

                        "currently_running": [m for m in unique_pool if m.get("matchStarted") and not _is_finished(m) and _is_match(m)],

                        "just_finished_today": [m for m in unique_pool if _is_finished(m) and m.get("date") == target_iso and _is_match(m)],

                        "full_daily_schedule": [m for m in unique_pool if m.get("date") == target_iso and _is_match(m)]

                    }

                    if not any(api_results["today_action_summary"].values()):

                         api_results["scope_missing_for_today"] = f"No matches found for {current_series or current_team} on {target_iso}"

                else:

                    api_results["today_action_summary"] = {

                        "currently_running": [m for m in unique_pool if m.get("matchStarted") and not _is_finished(m)],

                        "just_finished_today": [m for m in unique_pool if _is_finished(m) and m.get("date") == target_iso],

                        "full_daily_schedule": [m for m in unique_pool if m.get("date") == target_iso][:15]

                    }

                    if not api_results["today_action_summary"]["full_daily_schedule"]:

                         api_results["upcoming_broad_schedule"] = daily_res.get("data", [])[:5]

                if target_date:

                    api_results["RELEVANT_MATCHES_FOUND"] = deep_discovered

                    api_results["RELEVANT_MATCHES_FOUND"] = deep_discovered

                    api_results["RELEVANT_MATCHES_FOUND"] = deep_discovered

                target_m_candidates = []

                if "matches_in_scope" in api_results:

                     target_m_candidates.extend(api_results["matches_in_scope"].get("finished", []))

                     target_m_candidates.extend(api_results["matches_in_scope"].get("running", []))

                if deep_discovered:

                     ids_in_scope = {m["id"] for m in target_m_candidates}

                     for dm in deep_discovered:

                         if dm["id"] not in ids_in_scope:

                             target_m_candidates.append(dm)

                chosen_match = None

                if target_m_candidates:

                    chosen_match = target_m_candidates[0]

                    if target_date:

                        date_matches = [m for m in target_m_candidates if m.get("date") == target_date]

                        if date_matches:

                            chosen_match = date_matches[0]

                            chosen_match = date_matches[0]

                if chosen_match and not p_name:

                     tid = chosen_match.get("id")

                     if tid:

                          force_flag = str(datetime.now().year) in str(chosen_match.get("date", ""))

                          temp_sc = await get_match_scorecard(tid, force_api=force_flag)

                          if temp_sc and temp_sc.get("data"):

                               full_sc_temp = temp_sc["data"].get("scorecard", [])

                               user_q_tokens = set(_normalize(user_query).split())

                               found_dynamic_p = None

                               if temp_sc and temp_sc.get("data"):

                                   full_sc_temp = temp_sc["data"].get("scorecard", [])

                                   user_q_tokens = set(_normalize(user_query).split())

                                   debug_names = []

                                   for inn in full_sc_temp:

                                       if found_dynamic_p: break

                                       for bat in inn.get("batting", []):

                                           bn = bat.get("batsman", {}).get("name", "")

                                           debug_names.append(bn)

                                           name_parts = _normalize(bn).split()

                                           for part in name_parts:

                                               if (len(part) >= 3 and part in user_q_tokens):

                                                    found_dynamic_p = bn

                                                    break

                                       if not found_dynamic_p:

                                            for bowl in inn.get("bowling", []):

                                                bn = bowl.get("bowler", {}).get("name", "")

                                                if bn not in debug_names: debug_names.append(bn)

                                                name_parts = _normalize(bn).split()

                                                for part in name_parts:

                                                    if (len(part) >= 3 and part in user_q_tokens):

                                                        found_dynamic_p = bn

                                                        break

                                   logger.info(f"Scorecard Players Scanned: {debug_names[:5]}... (Total {len(debug_names)})")

                               if found_dynamic_p:

                                    p_name = found_dynamic_p

                                    logger.info(f"Dynamic Logic: Discovered player '{p_name}' in query from match data!")

                               else:

                                    logger.warning(f"Dynamic Logic: Could not find any player from scorecard in user query. Tokens: {list(user_q_tokens)[:5]}...")

                if target_m_candidates:

                    query_lower = _normalize(user_query)

                    scored_candidates = []

                    for m in target_m_candidates:

                        score = 0

                        m_str = _normalize(m.get("name", "")) + " " + _normalize(m.get("venue", "")) + " " + _normalize(str(m.get("series_id", "")))

                        if target_date and m.get("date") == target_date:

                            score += 50

                        m_tokens = set(m_str.split())

                        q_tokens = set(query_lower.split())

                        common = m_tokens.intersection(q_tokens)

                        score += (len(common) * 10)

                        entities = api_results.get("intent_analysis", {}).get("entities", {})

                        matches_count = 0

                        for qt in q_tokens:

                            if len(qt) > 3 and qt in m_str:

                                matches_count += 1

                        score += (matches_count * 15)

                        m_name_norm = _normalize(m.get("name", ""))

                        if t_name and o_name:

                             t_norm = _normalize(t_name)

                             o_norm = _normalize(o_name)

                             if t_norm in m_name_norm and o_norm in m_name_norm:

                                  score += 150

                                  logger.info(f"Dynamic Boost: Match '{m.get('name')}' aligns with requested entities {t_name} vs {o_name}")

                        elif t_name:

                             if _normalize(t_name) in m_name_norm:

                                  score += 30

                        pass

                        if str(m.get("id")).isdigit() or m.get("original_data_source") == "sportmonks" or m.get("_source") == "sportmonks":

                             score += 200

                             logger.info(f"Boosting SportMonks match: {m.get('name')} (Score += 200)")

                        scored_candidates.append((score, m))

                    scored_candidates.sort(key=lambda x: x[0], reverse=True)

                    top_scores = [f"{s[1]['name']} ({s[0]})" for s in scored_candidates[:3]]

                    logger.info(f"Candidate Match Scores: {top_scores}")

                    best_score = scored_candidates[0][0] if scored_candidates else 0

                    search_teams = []

                    if t_name: search_teams.append(t_name)

                    if o_name: search_teams.append(o_name)

                    if best_score < 40 and search_teams:

                        logger.warning(f"Low match score ({best_score}) for candidates. Triggering TARGETED SEARCH for: {search_teams}")

                        for stm in search_teams:

                            try:

                                search_res = await cric("/matches")

                                if search_res.get("ok") and search_res.get("data"):

                                    all_matches = search_res["data"]

                                    for m in all_matches:

                                        m_name = _normalize(m.get("name", ""))

                                        if stm in m_name:

                                            if target_date and m.get("date") != target_date:

                                                 continue

                                            m["source"] = "targeted_search"

                                            s = 50

                                            if target_date and m.get("date") == target_date: s += 50

                                            scored_candidates.append((s, m))

                            except Exception as e:

                                logger.error(f"Targeted Search Failed: {e}")

                        scored_candidates.sort(key=lambda x: x[0], reverse=True)

                        if scored_candidates:

                            top_scores = [f"{s[1]['name']} ({s[0]})" for s in scored_candidates[:3]]

                            logger.info(f"Refined Match Scores: {top_scores}")

                    chosen_match = scored_candidates[0][1]

                    target_m_id = chosen_match.get("id")

                    if target_m_id:

                        m_date = str(chosen_match.get("date", ""))

                        current_year = str(datetime.now().year)

                        is_current_year = current_year in m_date

                        logger.info(f"Fetching full scorecard for match {target_m_id} (Date: {m_date}, ForceAPI: {is_current_year})")

                        sc_data = await get_match_scorecard(target_m_id, force_api=is_current_year)

                        if not (sc_data and sc_data.get("data")) and chosen_match.get("matchStarted"):

                             logger.info(f"Scorecard null for started match {target_m_id}. Fetching live details fallback.")

                             api_results["live_match_details_fallback"] = await get_live_match_details(target_m_id, use_cache=False)

                        if sc_data and sc_data.get("data"):

                             data_node = sc_data["data"]

                             api_results["final_match_scorecard"] = data_node

                             live_state = extract_live_state(data_node, points_data=data_node.get("points_data"))

                             api_results["match_live_state"] = live_state

                             logger.info(f"Generated Live State: {json.dumps(live_state)}")

                             match_info_context = []

                             toss_text = data_node.get('toss') or data_node.get('tossText')

                             if isinstance(toss_text, dict):

                                 toss_text = f"{toss_text.get('winner', 'Unknown')} chose to {toss_text.get('elected', 'play')}"

                             if toss_text:

                                 match_info_context.append(f"Toss: {toss_text}")

                                 api_results['toss_info'] = toss_text

                             mom = data_node.get('manOfTheMatch') or data_node.get('player_of_match')

                             if mom:

                                 mom_name = mom.get('name') if isinstance(mom, dict) else str(mom)

                                 match_info_context.append(f"Player of the Match: {mom_name}")

                                 api_results['man_of_the_match'] = mom_name

                             m_status = data_node.get('status') or chosen_match.get('status')

                             if m_status:

                                 match_info_context.append(f"Result: {m_status}")

                                 pp_lines = []

                                 for inn in data_node.get('scorecard', []):

                                     inn_name = inn.get('inning', 'Inn')

                                     pp_data = inn.get('powerplay') or inn.get('pp_data')

                                     if pp_data:

                                         pp_lines.append(f"{inn_name} PP: {pp_data}")

                                 if pp_lines:

                                     match_info_context.append(" | ".join(pp_lines))

                             if match_info_context:

                                 api_results['match_key_info'] = ' | '.join(match_info_context)

                             if not data_node.get("scorecard") and data_node.get("status"):

                                  fallback_msg = data_node.get("_fallback_message", "Scorecard data pending.")

                                  fallback_status = data_node.get("status", "")

                                  api_results["match_score_summary"] = f"Match Status: {fallback_status} ({fallback_msg})"

                                  logger.info(f"Using Fallback Status Summary: {api_results['match_score_summary']}")

                             if p_name:

                                 p_stats_found = []

                                 full_sc = sc_data["data"].get("scorecard", [])

                                 if not full_sc and sc_data["data"].get("fantasy_points_dump"):

                                      logger.info("Using Fantasy Dump for Player Stats extraction")

                                      full_sc = sc_data["data"]["fantasy_points_dump"]

                                 q_tokens = set(_normalize(p_name).split())

                        if not api_results.get("match_score_summary") and chosen_match.get("score"):

                             try:

                                 summary_scores = []

                                 for s in chosen_match["score"]:

                                     inn = s.get("inning", "").replace(" Inning 1", "").replace(" Inning 2", "")

                                     r = s.get("r")

                                     w = s.get("w")

                                     o = s.get("o")

                                     summary_scores.append(f"{inn}: {r}/{w} ({o} ov)")

                                 if summary_scores:

                                     fallback_txt = " vs ".join(summary_scores)

                                     api_results["match_score_summary"] = f"Match Summary (Live List): {fallback_txt}"

                                     logger.info(f"Using Match List Score Summary: {api_results['match_score_summary']}")

                             except Exception as e:

                                 logger.error(f"Match List Summary construction failed: {e}")

                                 for inn in full_sc:

                                     for bat in inn.get("batting", []):

                                         b_name = bat.get("batsman", {}).get("name", "")

                                         b_tokens = set(_normalize(b_name).split())

                                         match = False

                                         if _normalize(p_name) in _normalize(b_name) or _normalize(b_name) in _normalize(p_name):

                                             match = True

                                         else:

                                             common = q_tokens.intersection(b_tokens)

                                             if any(len(t) > 2 for t in common):

                                                 match = True

                                         if match:

                                             r = bat.get("r", 0)

                                             b = bat.get("b", 0)

                                             s4 = bat.get("4s", 0)

                                             s6 = bat.get("6s", 0)

                                             sr = bat.get("sr") or bat.get("strikeRate") or "-"

                                             p_stats_found.append(f"Batting ({inn.get('inning')}): {b_name} {r} runs off {b} balls ({s4}x4, {s6}x6) SR: {sr}")

                                     for bowl in inn.get("bowling", []):

                                         b_name = bowl.get("bowler", {}).get("name", "")

                                         b_tokens = set(_normalize(b_name).split())

                                         match = False

                                         if _normalize(p_name) in _normalize(b_name) or _normalize(b_name) in _normalize(p_name):

                                             match = True

                                         else:

                                             common = q_tokens.intersection(b_tokens)

                                             if any(len(t) > 2 for t in common):

                                                 match = True

                                         if match:

                                             w = bowl.get("w", 0)

                                             o = bowl.get("o", 0)

                                             r = bowl.get("r", 0)

                                             eco = bowl.get("eco") or bowl.get("economy") or "-"

                                             p_stats_found.append(f"Bowling ({inn.get('inning')}): {b_name} {w} wickets for {r} runs in {o} overs (Eco: {eco})")

                                 if p_stats_found:

                                     api_results["specific_player_stats"] = " | ".join(p_stats_found)

                                     logger.info(f"Extracted Player Stats for {p_name}: {api_results['specific_player_stats']}")

                                 else:

                                     logger.warning(f"Could not find stats for '{p_name}' in scorecard.")

                             try:

                                 current_status_lines = []

                                 full_sc = sc_data["data"].get("scorecard", []) or sc_data["data"].get("fantasy_points_dump", [])

                                 if full_sc and isinstance(full_sc, list):

                                     active_inn = full_sc[-1]

                                     if isinstance(active_inn, list):

                                         logger.warning(f"active_inn is a list! {active_inn}")

                                         continue

                                     inn_name = active_inn.get("inning", "Current Inning")

                                     active_batters = []

                                     for bat in active_inn.get("batting", []):

                                         if not isinstance(bat, dict):

                                             continue

                                         if not bat.get("dismissal") or bat.get("dismissal") == "not out":

                                             b_name = bat.get("batsman", {}).get("name", "Unknown")

                                             r = bat.get("r", 0)

                                             b = bat.get("b", 0)

                                             active_batters.append(f"{b_name} ({r} off {b})")

                                     if active_batters:

                                         current_status_lines.append(f"🏏 Batting Now in {inn_name}: {', '.join(active_batters)}")

                                 if current_status_lines:

                                     api_results["current_match_situation"] = " | ".join(current_status_lines)

                                     logger.info(f"Extracted Current Situation: {api_results['current_match_situation']}")

                             except Exception as e:

                                 logger.error(f"Error extracting current situation: {e}")

                             api_results["match_identity"] = {

                                 "name": chosen_match.get("name"),

                                 "venue": chosen_match.get("venue"),

                                 "status": chosen_match.get("status")

                             }

                             if chosen_match.get("matchStarted") and not _is_finished(chosen_match):

                                 api_results["full_scorecard_context"] = sc_data["data"]

                if t_name:

                    match_filter_type = entities.get("metric")

                    if not match_filter_type:

                         if "wicket" in user_query.lower(): match_filter_type = "wickets"

                         elif "run" in user_query.lower(): match_filter_type = "runs"

                    m_id = await find_match_id(t_name, team2=o_name, series_id=s_id if 's_id' in locals() else None, target_date=target_date, match_type_filter=match_filter_type)

                    if m_id: api_results["target_match_details"] = await get_live_match_details(m_id, use_cache=False)

            elif tool in ["get_upcoming_matches", "get_upcoming_match", "upcoming_schedule"]:

                upcoming_data = await get_upcoming_matches(check_date=target_date, use_cache=False)

                matches = upcoming_data.get("data", [])

                api_results["upcoming_schedule"] = matches[:15]

            elif tool == "predict_live_match":
                candidates = api_results.get("RELEVANT_MATCHES_FOUND", [])
                if not candidates and api_results.get("live_matches"):
                     candidates = api_results["live_matches"]


                live_candidates = [m for m in candidates if str(m.get("status", "")).upper() in ["LIVE", "INNINGS BREAK"]]
                target_m = live_candidates[0] if live_candidates else (candidates[0] if candidates else None)

                if target_m:
                     ctx_logger.info(f"Predicting match: {target_m.get('name')}")
                     p_res = await predict_live_match(target_m)
                     api_results["live_win_prediction"] = p_res
                else:
                     api_results["live_win_prediction"] = {"note": "No active match found to predict."}

            elif tool == "extract_series_winner" and s_name:

                s_id = await find_series_smart(s_name, year)

                if s_id and "ERROR:" not in str(s_id):

                    winner_info = await extract_series_winner(s_id)

                    api_results["series_winner_info"] = winner_info

                    if winner_info and winner_info.get("match_id"):

                        force_flag = str(datetime.now().year) in str(winner_info.get("date", ""))

                        final_scorecard = await get_match_scorecard(winner_info["match_id"], force_api=force_flag)

                        if final_scorecard and final_scorecard.get("data"):

                            scorecard_data = final_scorecard["data"]

                            api_results["final_match_scorecard"] = scorecard_data

                            try:

                                summary_scores = []

                                for sc_inn in scorecard_data.get("scorecard", []):

                                    inn_name = sc_inn.get("inning", "Inning").replace(" Inning 1", "").replace(" Inning 2", "")

                                    tots = sc_inn.get("totals", {})

                                    r = tots.get("R") or tots.get("runs") or "?"

                                    w = tots.get("W") or tots.get("wickets") or "?"

                                    o = tots.get("O") or tots.get("overs") or "?"

                                    summary_scores.append(f"{inn_name}: {r}/{w} ({o} ov)")

                                if summary_scores:

                                    api_results["match_score_summary"] = " vs ".join(summary_scores)

                                    logger.info(f"Generated Score Summary: {api_results['match_score_summary']}")

                            except Exception as e:

                                logger.error(f"Score Summary Failed: {e}")

                            if not scorecard_data.get("manOfTheMatch"):

                                logger.info("manOfTheMatch is None, extracting top performer from stats")

                                top_batsman = None

                                top_bowler = None

                                max_runs = 0

                                max_wickets = 0

                                for inning in scorecard_data.get("scorecard", []):

                                    for bat in inning.get("batting", []):

                                        runs = int(bat.get("r") or bat.get("runs") or 0)

                                        if runs > max_runs:

                                            max_runs = runs

                                            bat_obj = bat.get("batsman") or bat.get("batter")

                                            top_batsman = bat_obj.get("name") if isinstance(bat_obj, dict) else str(bat_obj)

                                    for bowl in inning.get("bowling", []):

                                        wickets = int(bowl.get("w") or bowl.get("wickets") or 0)

                                        if wickets > max_wickets:

                                            max_wickets = wickets

                                            bowl_obj = bowl.get("bowler")

                                            top_bowler = bowl_obj.get("name") if isinstance(bowl_obj, dict) else str(bowl_obj)

                                api_results["top_performers"] = {

                                    "top_batsman": {"name": top_batsman, "runs": max_runs},

                                    "top_bowler": {"name": top_bowler, "wickets": max_wickets}

                                }

                                if top_batsman:

                                    scorecard_data["manOfTheMatch"] = top_batsman

                                    scorecard_data["manOfTheSeries"] = top_batsman

                                    logger.info(f"Patched manOfTheMatch with: {top_batsman}")

                                logger.info(f"Top performers: {top_batsman} ({max_runs}r), {top_bowler} ({max_wickets}w)")

                            logger.info(f"Fetched final match scorecard for Player of the Match data")

                else:

                     api_results["api_error"] = f"Series not found: {s_name} {year or ''}"

            elif tool == "get_head_to_head_statistics":
                current_year = datetime.now().year
                query_year = entities.get("year") or year
                is_past_year = query_year and int(query_year) < current_year

                ctx_logger.info(f"⚔️ H2H: {t_name} vs {o_name} | Year={query_year} | UsePastDB={is_past_year}")

                if is_past_year or analysis.get("time_context") == "PAST" or analysis.get("intent") == "past_history":

                    ctx_logger.info(f"📚 Fetching H2H from LOCAL DATABASE")
                    h2h_history = await get_head_to_head_history(t_name, o_name)
                    if h2h_history:
                        api_results["head_to_head_history"] = h2h_history
                        ctx_logger.info(f"✅ Found {len(h2h_history)} historical matches in DB")


                ctx_logger.info(f"🌐 Fetching H2H from API for latest data")
                api_results["head_to_head_data"] = await get_head_to_head_statistics(t_name, o_name)

            elif tool in ["predict_winner", "predict_live_match", "predict_match_analysis"]:

                pred_type = "live_match" if tool == "predict_live_match" else "winner"


                if tool == "predict_match_analysis":
                    pred_type = "match_analysis"

                f_id = None
                if api_results.get("target_match_details"):
                    f_id = api_results["target_match_details"].get("id")


                p_t1 = t_name
                p_t2 = o_name


                if not p_t1 or not p_t2:
                     up_s = api_results.get("upcoming_schedule", [])
                     if up_s:

                         p_t1 = up_s[0].get("localteam", {}).get("name")
                         p_t2 = up_s[0].get("visitorteam", {}).get("name")
                         if not target_date: target_date = up_s[0].get("date")

                api_results["prediction_analysis"] = await predict(
                    team_a=p_t1,
                    team_b=p_t2,
                    prediction_type=pred_type,
                    fixture_id=f_id,
                    date=target_date
                )

            elif tool == "predict_player_performance":

                f_id = None

                if api_results.get("target_match_details"):

                    f_id = api_results["target_match_details"].get("id")

                api_results["player_prediction"] = await predict_player_performance(

                    fixture_id=f_id,

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

                     from app.backend_core import past_db_get_standings

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

                     upcoming_res = await get_upcoming_matches(check_date=target_date)

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
                         # For all-time, also try API as fallback/complement for recent stats
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

    if not api_results and (intent == "LIVE_MATCH" or any(tool in ["get_live_matches"] for tool in required_tools)):

        if matches_task is not None:

            m_data = await matches_task

        else:

            m_data = await get_todays_matches()

        today_iso = datetime.now().strftime("%Y-%m-%d")

        api_results["generic_today_data"] = [m for m in m_data.get("data", []) if m.get("date") == today_iso]

        if not api_results["generic_today_data"]:

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

for msg in st.session_state.messages:

    with st.chat_message(msg["role"]): st.markdown(msg["content"])

user_input = st.chat_input("Ask me anything...", disabled=st.session_state.processing)

if user_input:

    st.session_state.messages.append({"role": "user", "content": user_input})

    save_chat(st.session_state.messages)

    with st.chat_message("user"): st.markdown(user_input)

    with st.chat_message("assistant"):

        try:

            conv_history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1]]

            async def run_chat_flow():

                response = await process_user_message(user_input, conv_history)

                if isinstance(response, str):

                    st.markdown(response)

                    return response

                elif hasattr(response, "__aiter__"):

                    full_response = ""

                    placeholder = st.empty()

                    async for chunk in response:

                        full_response += chunk

                        if chunk:

                            placeholder.markdown(full_response + "▌")

                    placeholder.markdown(full_response)

                    return full_response

                else:

                    st.markdown(str(response))

                    return str(response)

            final_text = asyncio.run(run_chat_flow())

            st.session_state.messages.append({"role": "assistant", "content": final_text})

            save_chat(st.session_state.messages)

            st.rerun()

        except Exception as e:

            st.error(f"System Error: {e}")
