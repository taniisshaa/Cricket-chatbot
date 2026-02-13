import psycopg2
import json

DB_CONFIG = {
    "dbname": "cricket_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

def analyze_low_scores():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, name, starting_at, raw_json 
            FROM fixtures 
            WHERE season_id = 1689 
            AND status = 'Finished'
        """)
        
        matches = cur.fetchall()
        innings_list = []
        aggregates_list = []
        
        for mid, name, start, raw in matches:
            if not raw: continue
            if isinstance(raw, str): raw = json.loads(raw)
            
            scoreboards = raw.get('scoreboards', [])
            if not scoreboards: continue
            
            totals = []
            for sb in scoreboards:
                if sb.get('type') == 'total':
                    val = sb.get('total', 0)
                    team_id = sb.get('team_id')
                    totals.append((team_id, val))
            
            if not totals: continue
            
            # Record individual innings
            for tid, val in totals:
                # Find team name
                team_name = "Unknown"
                if raw.get('localteam', {}).get('id') == tid:
                    team_name = raw['localteam'].get('name')
                elif raw.get('visitorteam', {}).get('id') == tid:
                    team_name = raw['visitorteam'].get('name')
                
                innings_list.append({
                    "match": name,
                    "date": start,
                    "team": team_name,
                    "score": val
                })
            
            # Record match aggregate
            if len(totals) >= 2:
                aggregate = sum(t[1] for t in totals)
                aggregates_list.append({
                    "match": name,
                    "date": start,
                    "aggregate": aggregate,
                    "scores": [t[1] for t in totals]
                })

        # Individual Lowest
        innings_list.sort(key=lambda x: x['score'])
        print("--- TOP 10 LOWEST TEAM TOTALS ---")
        for i in innings_list[:10]:
            print(f"{i['score']} - {i['team']} in {i['match']} ({i['date']})")

        # Aggregate Lowest
        aggregates_list.sort(key=lambda x: x['aggregate'])
        print("\n--- TOP 10 LOWEST MATCH AGGREGATES ---")
        for a in aggregates_list[:10]:
            print(f"{a['aggregate']} - {a['match']} ({a['date']}) Scores: {a['scores']}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_low_scores()
