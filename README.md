# 🏏 Cricket Chatbot & Analytics Tool

A powerful, AI-driven cricket chatbot that provides real-time scores, historical statistics, match predictions, and deep analytics. Built with Python, Streamlit, and Sportmonks API.

## 🚀 Features

- **Updates**: Live match scores and commentary without refreshing.
- **Agentic AI**:
  - **Agent 1 (Research)**: Analyzes raw data for deep insights.
  - **Agent 2 (History)**: Retrieves past match records and trivia (e.g., "Fastest 50").
- **Smart Search**: Find matches effectively by date, team, or specific events (e.g., "25 Jan matches").
- **Predictive Models**: Win probability and player performance forecasting.
- **Analytics**: Series stats, points tables, and head-to-head comparisons.
- **Multi-Language Support**: Supports English and Hinglish queries.

## 🛠️ Tech Stack

- **Backend**: Python, SQLite (Historical DB)
- **Frontend**: Streamlit
- **AI/LLM**: OpenAI GPT-4o
- **Data Source**: Sportmonks Cricket API v2.0 & Local Legacy Database

## 📦 Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd chatbot-cricket
   ```

2. **Create a Virtual Environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # source .venv/bin/activate  # Mac/Linux
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Setup**
   - Create a `.env` file in the root directory.
   - Add your API keys:
     ```env
     SPORTMONKS_API_KEY=your_key_here
     OPENAI_API_KEY=your_key_here
     ```

## ▶️ Running the App

Start the Streamlit application:
```bash
streamlit run app/main.py
```

## 📂 Project Structure

- `app/`: Core application logic (AI, Backend, Services).
- `data/`: Local databases for archival data.
- `logs/`: Application logs (excluded from git).

---
*Built with ❤️ for Cricket Fans*
