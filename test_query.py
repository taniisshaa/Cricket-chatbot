import asyncio
from src.core.universal_cricket_engine import handle_universal_cricket_query

async def test_winner_query():
    user_query = "ipl 2025 winner??"
    result = await handle_universal_cricket_query(user_query)
    print("--- QUERY RESULT ---")
    print(result)

if __name__ == "__main__":
    asyncio.run(test_winner_query())
