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
        print(f"Total seasons in 2025: {len(results)}")
        for r in results:
            if 'IPL' in r['name'] or 'Indian' in r['name'] or 'Premiere' in r['name'] or 'Premier' in r['name']:
                print(f"MATCH: {r}")
            else:
                pass # Skip irrelevant for now

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_2025_seasons()
