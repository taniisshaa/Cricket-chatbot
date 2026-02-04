"""
Enhanced Live Commentary Service
Provides detailed ball-by-ball commentary and match events
"""

import asyncio 
from datetime import datetime 
from app.backend_core import sportmonks_cric 
from app.utils_core import get_logger 

logger = get_logger("commentary", "LIVE_COMMENTARY.log")async def get_ball_by_ball_commentary(match_id): """
 Fetch detailed ball-by-ball commentary for a match
 """
 if not match_id: return {"error": "No match ID provided"}

 logger.info(f"Fetching ball-by-ball commentary for match {match_id }")try: result = await sportmonks_cric(f"/fixtures/{match_id }", {"include": "balls, runs, batting.batsman, bowling.bowler, localteam, visitorteam"}, use_cache = False, ttl = 5)if not result.get("ok"): logger.warning(f"API call failed for match {match_id }: {result.get('error')}")return {"message": "Ball-by-ball commentary is not available for this match.This feature works best with live or recently completed matches."}

 match_data = result.get("data", {})if not match_data: return {"message": "No match data found"}

 balls = match_data.get("balls", [])if not balls or len(balls) == 0: logger.info(f"No ball data available for match {match_id }")return {"message": "Ball-by-ball commentary is not available for this match.This feature works best with live or recently completed matches."}


 commentary = process_commentary(balls, match_data)return {
 "match_id": match_id, "match_name": f"{match_data.get('localteam', {}).get('name', 'Team 1')} vs {match_data.get('visitorteam', {}).get('name', 'Team 2')}", "commentary": commentary, "total_balls": len(balls), "last_updated": datetime.now().isoformat()}

 except Exception as e: logger.error(f"Error fetching commentary for match {match_id }: {e }")return {"message": f"Unable to fetch commentary at this time.This feature works best with live matches."}

def process_commentary(balls, match_data): """
 Process raw ball data into structured commentary
 """
 commentary_data = {
 "recent_overs": [], "last_over": [], "key_events": [], "current_over": None 
 }


 sorted_balls = sorted(balls, key = lambda x: (x.get("over", 0), x.get("ball", 0)), reverse = True)current_over_num = None 
 over_data = []over_count = 0 

 for ball in sorted_balls: over_num = ball.get("over")ball_num = ball.get("ball")runs = ball.get("score", {}).get("runs", 0)is_wicket = ball.get("score", {}).get("is_wicket", False)batsman = ball.get("batsman", {}).get("name", "Unknown")bowler = ball.get("bowler", {}).get("name", "Unknown")ball_desc = f"{over_num }.{ball_num }: {batsman } - "

 if is_wicket: ball_desc + = f"WICKET ! {runs } run(s)"
 commentary_data["key_events"].append({
 "type": "wicket", "over": over_num, "ball": ball_num, "batsman": batsman, "bowler": bowler, "description": ball_desc 
 })elif runs >= 4: event_type = "six"if runs == 6 else "four"
 ball_desc + = f"{runs } runs({event_type.upper()} ! )"
 commentary_data["key_events"].append({
 "type": event_type, "over": over_num, "ball": ball_num, "batsman": batsman, "runs": runs, "description": ball_desc 
 })else: ball_desc + = f"{runs } run(s)"


 if current_over_num is None: current_over_num = over_num 
 commentary_data["current_over"] = over_num 

 if over_num == current_over_num: over_data.append({
 "ball": ball_num, "runs": runs, "is_wicket": is_wicket, "batsman": batsman, "bowler": bowler, "description": ball_desc 
 })else: if over_count < 5: commentary_data["recent_overs"].append({
 "over_number": current_over_num, "balls": over_data[: ], "total_runs": sum(b["runs"]for b in over_data)})over_count + = 1 


 current_over_num = over_num 
 over_data = [{
 "ball": ball_num, "runs": runs, "is_wicket": is_wicket, "batsman": batsman, "bowler": bowler, "description": ball_desc 
 }]if over_count >= 5: break 


 if over_data and over_count < 5: commentary_data["last_over"] = over_data 
 commentary_data["recent_overs"].insert(0, {
 "over_number": current_over_num, "balls": over_data, "total_runs": sum(b["runs"]for b in over_data)})return commentary_data 

async def get_last_over_summary(match_id): """
 Get a quick summary of the last over
 """
 commentary = await get_ball_by_ball_commentary(match_id)if "error"in commentary or "message"in commentary: return commentary 

 last_over = commentary.get("commentary", {}).get("last_over", [])if not last_over: return {"message": "No recent over data available"}


 total_runs = sum(ball["runs"]for ball in last_over)wickets = sum(1 for ball in last_over if ball["is_wicket"])summary = {
 "over_number": commentary["commentary"]["current_over"], "total_runs": total_runs, "wickets": wickets, "balls": [f"{b['ball']}: {b['runs']}{'W'if b['is_wicket']else ''}"for b in last_over], "description": f"Over {commentary['commentary']['current_over']}: {total_runs } runs, {wickets } wicket(s)"
 }

 return summary 

async def get_recent_events(match_id, event_type = None): """
 Get recent key events(wickets, fours, sixes)event_type: 'wicket', 'four', 'six', or None for all
 """
 commentary = await get_ball_by_ball_commentary(match_id)if "error"in commentary or "message"in commentary: return commentary 

 events = commentary.get("commentary", {}).get("key_events", [])if event_type: events = [e for e in events if e["type"] == event_type]return {
 "match_id": match_id, "event_type": event_type or "all", "events": events[: 10], "total_events": len(events)}


async def format_commentary_for_user(match_id, query_type = "last_over"): """
 Format commentary data for natural language response
 query_type: 'last_over', 'recent_events', 'full_commentary'
 """
 if query_type == "last_over": data = await get_last_over_summary(match_id)if "error"in data or "message"in data: return data 

 response = f"**Last Over Summary(Over {data['over_number']})**\n\n"
 response + = f"Total Runs: {data['total_runs']}\n"
 response + = f"Wickets: {data['wickets']}\n\n"
 response + = "Ball-by-ball: "+", ".join(data['balls'])return {"formatted_text": response, "raw_data": data }

 elif query_type == "recent_events": data = await get_recent_events(match_id)if "error"in data or "message"in data: return data 

 response = "**Recent Key Events**\n\n"
 for event in data["events"][: 5]: response + = f"• {event['description']}\n"

 return {"formatted_text": response, "raw_data": data }

 else: data = await get_ball_by_ball_commentary(match_id)if "error"in data or "message"in data: return data 

 commentary = data["commentary"]response = f"**Ball-by-Ball Commentary - {data['match_name']}**\n\n"

 for over in commentary["recent_overs"][: 3]: response + = f"\n**Over {over['over_number']}({over['total_runs']} runs)**\n"
 for ball in over["balls"]: response + = f" {ball['description']}\n"

 return {"formatted_text": response, "raw_data": data }
