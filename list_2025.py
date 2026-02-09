import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def list_2025_seasons():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT name, year FROM seasons WHERE year = '2025';")
        results = cur.fetchall()
        print("--- 2025 SEASONS ---")
        for r in results:
            print(r)

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_2025_seasons()
