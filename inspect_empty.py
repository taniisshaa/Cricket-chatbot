import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def inspect_remaining_empty():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, name, status, starting_at, (raw_json->>'note') as note
            FROM fixtures 
            WHERE status = 'Finished' AND (
                (raw_json->'batting') IS NULL OR jsonb_array_length(raw_json->'batting') = 0
            );
        """)
        rows = cur.fetchall()
        print(f"--- REMAINING EMPTY MATCHES: {len(rows)} ---")
        for r in rows:
            print(f"ID: {r['id']} | Name: {r['name']} | Status: {r['status']} | Note: {r['note']}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_remaining_empty()
