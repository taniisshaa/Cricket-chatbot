import psycopg2
import json

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def check_rcb_pbks():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, name, starting_at, status, raw_json->>'note' as note
            FROM fixtures 
            WHERE season_id = 1689 
            AND (name ILIKE '%Royal Challengers Bengaluru%' OR name ILIKE '%Punjab Kings%')
            ORDER BY starting_at
        """)
        
        rows = cur.fetchall()
        for r in rows:
            print(f"ID: {r[0]}, Name: {r[1]}, Date: {r[2]}, Status: {r[3]}, Note: {r[4]}")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_rcb_pbks()
