import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def find_ipl_winner():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT s.id, s.name, s.year, t.name as winner
            FROM seasons s
            LEFT JOIN season_champions sc ON s.id = sc.season_id
            LEFT JOIN teams t ON sc.winner_team_id = t.id
            WHERE s.name ILIKE '%IPL%' AND s.year = '2025';
        """)
        results = cur.fetchall()
        print("--- IPL 2025 DATA ---")
        for r in results:
            print(r)

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    find_ipl_winner()
