# Cricket Chatbot 🏏🤖

**An Advanced AI-Powered Cricket Assistant built with Streamlit & Python.**

This chatbot provides **Live Scores**, **Upcoming Schedules**, **Historical Stats**, and **Win Predictions** by intelligently switching between a **Real-Time API** and a **Local Historical Database**.

---

## 🏗️ Architecture & How It Works

The project uses a **Hybrid Data Routing System** to ensure 100% data availability:

### 1. 🔄 Data Flow Protocol
| Data Type | Source | Logic |
| :--- | :--- | :--- |
| **Live / Today / Upcoming** | 📡 **API (SportMonks)** | Fetches real-time data for freshness. |
| **Past Matches (Any Year)** | 💾 **Database (SQLite)** | Queries local `data/full_raw_history.db` for instant results. |
| **Predictions** | 🧠 **SQL Engine** | Calculates Win Probability using H2H, Venue, & Form from the DB. |

### 2. ⚡ Automatic Archiving
- **Auto-Archiver**: Runs in the background when checking live scores. Detecting a "Finished" match instantly archives it to the database.
- **Sync System**: Ensures no finished match is ever lost.

### 3. 📂 Project Structure
- **`src/main.py`**: The Entry Point. Runs the Streamlit Interface.
- **`src/agents/agent_workflow.py`**: The "Brain". Decides whether to route query to API or Database.
- **`src/core/rag_orchestrator.py`**: The Master RAG Controller that ensures zero hallucination by retrieving verified data.
- **`src/core/universal_cricket_engine.py`**: A Text-to-SQL engine that turns natural language queries into database commands.
- **`src/environment/`**: Services for fetching Live/API data.
- **`data/full_raw_history.db`**: The central brain storage for all historical stats.

---

## 🛠️ Setup & Installation Guide

### Prerequisites
- Python 3.10 or higher
- Git

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/taniisshaa/Cricket-chatbot.git
cd Cricket-chatbot
```

### 2️⃣ Create Virtual Environment (Recommended)
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Mac/Linux
source .venv/bin/activate
```

### 3️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```

### 4️⃣ Configure API Keys
Create a `.env` file in the root directory and add:
```ini
OPENAI_API_KEY=your_openai_key_here
SPORTMONKS_API_KEY=your_sportmonks_key_here
```

### 5️⃣ Run the Application
```bash
streamlit run src/main.py
```
The app will open in your browser at `http://localhost:8501`.

---

## 🚀 Features

- **Live Commentary & Scores**: Instant updates.
- **Deep Historical Stats**: ask "Who scored most runs in 2016?"
- **Win Probability**: Data-driven predictions based on venue & form.
- **Player Profiles**: Career stats and recent performance.

---

## 🔄 Updating Data

If you need to manually sync missed matches (e.g., from yesterday):
```bash
# Run the catch-up script
python catchup_2026_sync.py
```

---


