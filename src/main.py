import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
load_dotenv(override=True)
import streamlit as st
import asyncio
from datetime import datetime
from src.utils.ui_components import setup_streamlit_ui
from src.agents.agent_workflow import process_user_message
from src.utils.utils_core import save_chat, load_chat, Config, get_logger
Config.ensure_dirs()
logger = get_logger("app_main", "general_app.log")
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
setup_streamlit_ui()
try:
    SMART_ROUTER_ENABLED = True
except ImportError:
    SMART_ROUTER_ENABLED = False
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
            logger.error(f"Main Loop Error: {e}")