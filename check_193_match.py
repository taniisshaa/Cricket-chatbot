import psycopg2
import json

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def check_193_match():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT f.id, f.name, f.starting_at, l.name, s.name, f.raw_json 
            FROM fixtures f
            JOIN seasons s ON f.season_id = s.id
            JOIN leagues l ON s.league_id = l.id
            WHERE f.name = 'Royal Challengers Bengaluru vs Punjab Kings' 
            AND f.starting_at::date = '2025-04-18'
        """)
        
        row = cur.fetchone()
        if row:
            print(f"Match ID: {row[0]}")
            print(f"Name: {row[1]}")
            print(f"League: {row[3]}")
            print(f"Season: {row[4]}")
            raw = row[5]
            if isinstance(raw, str): raw = json.loads(raw)
            print(f"Scoreboards: {json.dumps(raw.get('scoreboards'), indent=2)}")
        else:
            print("Match not found.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_193_match()
