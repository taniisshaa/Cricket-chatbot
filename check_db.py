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
        
        # Check seasons
        print("--- SEASONS ---")
        cur.execute("SELECT * FROM seasons ORDER BY year DESC LIMIT 10;")
        seasons = cur.fetchall()
        for s in seasons:
            print(s)
            
        # Check season_champions
        print("\n--- SEASON CHAMPIONS ---")
        cur.execute("""
            SELECT s.year, s.name as season_name, t.name as winner_name 
            FROM season_champions sc
            JOIN seasons s ON sc.season_id = s.id
            JOIN teams t ON sc.winner_team_id = t.id
            ORDER BY s.year DESC;
        """)
        champions = cur.fetchall()
        for c in champions:
            print(c)
            
        # Check fixtures for IPL 2025 if any
        print("\n--- IPL 2025 FIXTURES COUNT ---")
        cur.execute("""
            SELECT count(*) 
            FROM fixtures f
            JOIN seasons s ON f.season_id = s.id
            WHERE s.year = 2025 AND s.name ILIKE '%IPL%';
        """)
        print(cur.fetchone())

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_db()
