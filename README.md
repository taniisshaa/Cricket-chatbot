# Cricket Chatbot 🏏🤖

**An Advanced AI-Powered Cricket Assistant built with Streamlit, Python & PostgreSQL.**

This chatbot provides **Live Scores**, **Upcoming Schedules**, **Historical Stats**, **Win Predictions**, and **Deep Analytics** by intelligently switching between a **Real-Time API** and a **Robust PostgreSQL Database**. It features a state-of-the-art **RAG (Retrieval-Augmented Generation) Pipeline** for accurate, context-aware responses.

---

## 🏗️ Architecture & How It Works

The project uses a **Hybrid Data Routing System** to ensure 100% data availability and accuracy:

### 1. 🔄 Data Flow Protocol
| Data Type | Source | Logic |
| :--- | :--- | :--- |
| **Live / Today / Upcoming** | 📡 **API (SportMonks)** | Fetches real-time data for freshness. |
| **Past Matches (Historical)** | 🐘 **Database (PostgreSQL)** | Queries the local PostgreSQL database for instant, deep historical analysis. |
| **Predictions & Analytics** | 🧠 **Analytics Engine** | Calculates Win Probability, Venn Diagrams, and Form Indices using complex SQL queries. |
| **Contextual Answers** | 📚 **RAG Pipeline** | Retrieves relevant documents/stats to ground the AI's responses and prevent hallucinations. |

### 2. ⚡ Automatic & Background Sync
- **Auto-Archiver**: Runs in the background to sync finished matches from the API to the PostgreSQL database.
- **Data Consistency**: Ensures the historical database is always up-to-date with the latest match results.

### 3. 📂 Key Project Components
- **`src/main.py`**: The Entry Point. Runs the Streamlit Interface.
- **`src/agents/agent_workflow.py`**: The "Brain". Orchestrates the flow between user input, RAG, and tools.
- **`src/core/rag_pipeline.py` & `rag_orchestrator.py`**: The core Retrieval-Augmented Generation system.
- **`src/core/analytics_service.py`**: Advanced analytics engine for generating insights like Win Probability and Team Comparison.
- **`src/core/universal_cricket_engine.py`**: Text-to-SQL engine for natural language database queries.
- **`src/environment/`**: Services for fetching Live/API data.
- **`data/`**: (Now migrated to PostgreSQL) Contains SQL scripts or schemas if applicable.

---

## 🛠️ Setup & Installation Guide

### Prerequisites
- Python 3.10 or higher
- PostgreSQL (Installed and Running)
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

### 4️⃣ Configure Environment & Database
Create a `.env` file in the root directory and add your API keys and Database credentials:
```ini
OPENAI_API_KEY=your_openai_key_here
SPORTMONKS_API_KEY=your_sportmonks_key_here

# PostgreSQL Database Configuration
DB_HOST=localhost
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_PORT=5432
```

### 5️⃣ Run the Application
```bash
streamlit run src/main.py
```
The app will open in your browser at `http://localhost:8501`.

---

## 🚀 Features

- **RAG-Powered Conversations**: accurate, context-aware answers to complex cricket queries.
- **Live Commentary & Scores**: Instant updates for ongoing matches.
- **Deep Historical Stats**: Query vast amounts of historical data (e.g., "Who has the highest strike rate in 2023?").
- **Win Probability Models**: Data-driven predictions based on venue stats, head-to-head records, and team form.
- **Visual Analytics**: Interactive charts and comparisons generated on the fly.

---

## 🔄 Updating Data

The system is designed to auto-sync, but you can force a sync if needed:
```bash
# Run the background sync manually (if script provided)
# python src/utils/background_scheduler.py
```

---
