
import os
import sys
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.core.universal_cricket_engine import DB_CONFIG

def check_nulls():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Count total fixtures
        cur.execute("SELECT COUNT(*) FROM fixtures;")
        total = cur.fetchone()['count']
        
        # Count null winner_team_id
        cur.execute("SELECT COUNT(*) FROM fixtures WHERE winner_team_id IS NULL AND status = 'Finished';")
        null_winner = cur.fetchone()['count']
        
        # Count null manofmatch (assuming it's stored in raw_json or a column, usually raw_json based on previous file view)
        # In history_service.py, manofmatch is extracted from raw_json in the sync function, but stored in raw_json
        # Let's check if there is a specific column for it, or if we need to check the JSON.
        # Looking at history_service.py insert statement:
        # INSERT INTO fixtures (id, season_id, name, starting_at, status, venue_id, winner_team_id, raw_json)
        # So manofmatch is likely inside raw_json.
        
        cur.execute("SELECT COUNT(*) FROM fixtures WHERE raw_json->>'manofmatch' IS NULL OR raw_json->'manofmatch' = 'null'::jsonb OR raw_json->'manofmatch' = '{}'::jsonb AND status = 'Finished';")
        null_mom = cur.fetchone()['count']

        print(f"Total Fixtures: {total}")
        print(f"Finished Matches with NULL winner_team_id: {null_winner}")
        print(f"Finished Matches with Empty/Null Man of Match in raw_json: {null_mom}")
        
        # Get a sample of IDs to fix
        cur.execute("SELECT id, name, status, starting_at FROM fixtures WHERE (winner_team_id IS NULL OR raw_json->>'manofmatch' IS NULL OR raw_json->'manofmatch' = '{}'::jsonb) AND status = 'Finished' LIMIT 10;")
        rows = cur.fetchall()
        print("\nSample matches to fix:")
        for r in rows:
            print(f"ID: {r['id']} | Name: {r['name']} | Date: {r['starting_at']}")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_nulls()
