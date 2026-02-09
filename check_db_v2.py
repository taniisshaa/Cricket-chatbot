import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def check_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("--- SEASONS 2024-2025 ---")
        cur.execute("SELECT id, name, year FROM seasons WHERE year IN (2024, 2025);")
        for s in cur.fetchall():
            print(f"Season: {s['name']} (ID: {s['id']}, Year: {s['year']})")
            
        print("\n--- TEAMS (Sample) ---")
        cur.execute("SELECT id, name FROM teams LIMIT 5;")
        for t in cur.fetchall():
            print(f"Team: {t['name']} (ID: {t['id']})")

        print("\n--- SEASON CHAMPIONS ---")
        cur.execute("""
            SELECT s.year, s.name as season_name, t.name as winner_name 
            FROM season_champions sc
            JOIN seasons s ON sc.season_id = s.id
            JOIN teams t ON sc.winner_team_id = t.id
            WHERE s.year IN (2024, 2025);
        """)
        champs = cur.fetchall()
        if not champs:
            print("No champions found for 2024-2025 in season_champions table.")
        for c in champs:
            print(c)

        print("\n--- RECENT FIXTURES (IPL 2025) ---")
        cur.execute("""
            SELECT f.id, f.name, f.status, f.winner_team_id 
            FROM fixtures f
            JOIN seasons s ON f.season_id = s.id
            WHERE s.year = 2025 AND s.name ILIKE '%IPL%'
            LIMIT 5;
        """)
        for f in cur.fetchall():
            print(f)

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_db()
