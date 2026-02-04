
# 🏏 Cricket Chatbot - Project Architecture & Flow

Welcome to the architectural overview of your Advanced AI Cricket Chatbot. This document explains how the entire system triggers, processes, and responds to user queries.

---

## 1. High-Level Flow (The "Lifecycle" of a Query)

1.  **User Input**: User types a query in the Streamlit UI (`main.py`).
2.  **Orchestration**: The request is sent to `agent_workflow.py`.
3.  **Intent Analysis**: `ai_core.py` uses an LLM to decide: "Is this about a *Live Match*, *History*, *Stats*, or *Schedule*?"
4.  **Tool Execution**: Based on intent, `agent_workflow.py` calls the right service (e.g., `history_service.py` or `live_match_service.py`).
5.  **Data Fetching**: The service fetches data from either:
    *   **Local DB** (`legacy_ipl_wpl.db`) for past matches.
    *   **Live API** (`backend_core.py`) for real-time scores.
6.  **Response Generation**: The raw data + user query is sent back to `ai_core.py`, which generates a natural, fan-friendly response.
7.  **Display**: The response is shown in the UI.

---

## 2. File-by-File Breakdown

### 🧠 The Core (The Brain)
*   **`app/main.py`**: The entry point. It launches the Streamlit interface, handles user session state, and captures input.
*   **`app/agent_workflow.py`**: The **Master Controller**.
    *   It contains the `process_user_message` function.
    *   It coordinates everything: calls the analyzer, selects tools (History vs Live), manages context (what team are we talking about?), and logs the "Thought Process".
*   **`app/ai_core.py`**: The **Intelligence Layer**.
    *   It talks to the LLM (Gemini/OpenAI).
    *   `analyze_intent()`: Decides what the user wants.
    *   `generate_human_response()`: Turns boring JSON data into exciting cricket commentary.

### 🔌 The Data Layer (The Backbone)
*   **`app/backend_core.py`**: The **API Gateway**.
    *   Direct link to the SportMonks Cricket API.
    *   Functions like `getMatchScorecard`, `get_live_matches` reside here.
    *   It handles caching (so we don't hit API limits).
*   **`app/history_service.py`**: The **Time Machine**.
    *   Manages the **Local SQLite Database**.
    *   Handles queries like "Most runs in 2024" or "India vs Pak H2H".
    *   It has "Smart Logic" to construct complex SQL queries dynamically.

### ⚡ Feature Services (The Skills)
*   **`app/live_match_service.py`**: Handles real-time matches. Fetches scores, run rates, and "Who is winning" probabilities.
*   **`app/analytics_service.py`**: The **Data Scientist**. Calculates complex stats like "Player Impact," "Head-to-Head," and summaries.
*   **`app/prediction_service.py`**: The **Oracle**. Uses past data + current form to predict match winners (Win % Calculator).
*   **`app/commentary_service.py`**: Fetches ball-by-ball commentary lines to make the chat feel like a live broadcast.
*   **`app/upcoming_service.py`**: Handles schedule queries ("When is the next match?").
*   **`app/squad_service.py`**: Fetches and compares team playing XIs.

### 🛠️ Utilities & UI
*   **`app/ui_components.py`**: Custom UI widgets for Streamlit (e.g., the Scorecard Dataframe, Charts).
*   **`app/utils_core.py`**: Logging setup (`get_logger`), PDF generation, and configuration loaders.
*   **`app/match_utils.py`**: Helper functions to normalize names (e.g., "MI" -> "Mumbai Indians") and format dates.

---

## 3. Example Workflows

### Scenario A: "Who is winning right now?"
1.  **`main.py`** receives text.
2.  **`agent_workflow.py`** calls `ai_core.py` -> Intent identified as `LIVE_MATCH`.
3.  **`agent_workflow.py`** calls `get_live_matches()` from `backend_core.py`.
4.  Data is passed to **`live_match_service.py`** to extract the score.
5.  **`ai_core.py`** writes: "India is dominating! 150/2 in 15 overs."

### Scenario B: "Who won IPL 2024?"
1.  **`main.py`** receives text.
2.  **`agent_workflow.py`** calls `ai_core.py` -> Intent identified as `PAST_HISTORY`.
3.  **`agent_workflow.py`** triggers `find_series_smart("IPL", 2024)`.
4.  **`history_service.py`** queries the **SQLite DB**.
5.  It finds the final match and winner.
6.  **`ai_core.py`** formats the answer.
