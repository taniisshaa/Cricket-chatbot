import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def check_schema():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        tables = ['seasons', 'season_champions', 'teams', 'fixtures']
        for table in tables:
            print(f"\n--- SCHEMA FOR {table} ---")
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table}';
            """)
            for col in cur.fetchall():
                print(f"{col['column_name']}: {col['data_type']}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
