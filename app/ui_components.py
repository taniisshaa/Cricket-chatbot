
import streamlit as st
from datetime import datetime
from app.utils_core import save_chat, generate_chat_pdf

def setup_streamlit_ui():
    """
    Sets up the Streamlit UI, including CSS and Sidebar logic.
    """
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
