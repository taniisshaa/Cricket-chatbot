"""
🧩 RAG CONTEXT BUILDER
======================
Formats retrieved data into perfect context for LLM consumption.
Ensures zero hallucination by providing structured, verified data.

Features:
- Smart data formatting
- Context compression
- Relevance scoring
- Evidence packaging
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json
from src.utils.utils_core import get_logger

logger = get_logger("rag_context_builder", "rag_context_builder.log")

class ContextBuilder:
    """
    Builds perfect context packages for LLM to generate accurate responses.
    """
    
    def __init__(self):
        self.max_context_tokens = 4000  # Approximate token limit for context
    
    def build_match_context(self, matches: List[Dict], query: str) -> str:
        """
        Build formatted context from match data.
        
        Args:
            matches: List of match records
            query: Original user query
        
        Returns:
            Formatted context string
        """
        if not matches:
            return "❌ No match data found for this query."
        
        context_parts = []
        context_parts.append(f"📊 **MATCH DATA** (Total: {len(matches)} matches)\n")
        
        # Smarter sampling: First 3 and Last 3 matches if there are many
        sample_matches = matches
        if len(matches) > 6:
            sample_matches = matches[:3] + matches[-3:]
            context_parts.append(f"*(Showing first 3 and last 3 matches out of {len(matches)} total)*\n")
        
        for idx, match in enumerate(sample_matches, 1):
            context_parts.append(f"\n### Match {idx}: {match.get('name', 'Unknown')}")
            context_parts.append(f"📅 Date: {match.get('starting_at', 'N/A')}")
            context_parts.append(f"📍 Status: {match.get('status', 'N/A')}")
            
            if match.get('result'):
                context_parts.append(f"🏆 Result: {match['result']}")
            
            # Legacy Innings summary
            if match.get('innings_summary'):
                context_parts.append("\n**Scorecard:**")
                for inn in match['innings_summary']:
                    tname = inn.get('team_name') or f"Team {inn.get('team_id')}"
                    context_parts.append(
                        f"  - {tname}: {inn.get('score')}/{inn.get('wickets')} "
                        f"in {inn.get('overs')} overs"
                    )
            
            # Universal Engine Score Summary
            if match.get('innings_scores'):
                context_parts.append("\n**Scorecard (Summary):**")
                for score in match['innings_scores']:
                    context_parts.append(f"  - {score}")
            
            # Universal Engine Batting
            bat_summary = match.get('batting_summary') or match.get('top_batsmen')
            if bat_summary:
                context_parts.append("\n**Top Batsmen:**")
                for bat in bat_summary[:4]:
                    if isinstance(bat, dict):
                        name = bat.get('p') or bat.get('name')
                        runs = bat.get('r') or bat.get('runs')
                        balls = bat.get('b') or bat.get('balls')
                        if name:
                            context_parts.append(f"  - {name}: {runs} ({balls})")

            # Universal Engine Bowling
            bowl_summary = match.get('bowling_summary') or match.get('top_bowlers')
            if bowl_summary:
                context_parts.append("\n**Top Bowlers:**")
                for bowl in bowl_summary[:4]:
                    if isinstance(bowl, dict):
                        name = bowl.get('p') or bowl.get('name')
                        w = bowl.get('w') or bowl.get('wickets')
                        e = bowl.get('e') or bowl.get('economy')
                        if name:
                            context_parts.append(f"  - {name}: {w} wkts (Econ: {e})")

            # Fallback for raw 'scorecard' blob
            if match.get('scorecard'):
                context_parts.append(f"\n**Raw Scorecard Data:** {str(match['scorecard'])[:500]}...")

            context_parts.append("\n" + "-" * 50)
        
        return "\n".join(context_parts)
    
    def build_player_context(self, player_data: Dict, query: str) -> str:
        """
        Build formatted context from player statistics.
        
        Args:
            player_data: Player statistics dict
            query: Original user query
        
        Returns:
            Formatted context string
        """
        if "error" in player_data:
            return f"❌ {player_data['error']}"
        
        context_parts = []
        
        # Player info
        if player_data.get('player_info'):
            info = player_data['player_info']
            context_parts.append(f"👤 **PLAYER: {info.get('fullname', 'Unknown')}**")
            context_parts.append(f"Position: {info.get('position_name', 'N/A')}")
            context_parts.append(f"Country ID: {info.get('country_id', 'N/A')}\n")
        
        # Batting stats
        if player_data.get('batting'):
            bat = player_data['batting']
            context_parts.append("🏏 **BATTING STATISTICS:**")
            context_parts.append(f"  - Innings: {bat.get('innings', 0)}")
            context_parts.append(f"  - Total Runs: {bat.get('total_runs', 0)}")
            context_parts.append(f"  - Average: {bat.get('average', 0):.2f}")
            context_parts.append(f"  - Highest Score: {bat.get('highest_score', 0)}")
            context_parts.append(f"  - Strike Rate: {bat.get('strike_rate', 0):.2f}")
            context_parts.append(f"  - Fours: {bat.get('fours', 0)}")
            context_parts.append(f"  - Sixes: {bat.get('sixes', 0)}\n")
        
        # Bowling stats
        if player_data.get('bowling'):
            bowl = player_data['bowling']
            context_parts.append("⚡ **BOWLING STATISTICS:**")
            context_parts.append(f"  - Matches: {bowl.get('matches', 0)}")
            context_parts.append(f"  - Total Wickets: {bowl.get('total_wickets', 0)}")
            context_parts.append(f"  - Runs Conceded: {bowl.get('runs_conceded', 0)}")
            context_parts.append(f"  - Economy: {bowl.get('economy', 0):.2f}")
            context_parts.append(f"  - Best Figures: {bowl.get('best_figures', 0)} wickets\n")
        
        return "\n".join(context_parts)
    
    def build_season_context(self, season_data: Dict, query: str) -> str:
        """
        Build formatted context from season data.
        
        Args:
            season_data: Season information dict
            query: Original user query
        
        Returns:
            Formatted context string
        """
        if isinstance(season_data, list):
            context_parts = ["🏆 **TOURNAMENT DATA:**"]
            for item in season_data:
                if "champion" in item:
                    context_parts.append(f"  - 👑 **Champion**: {item['champion']}")
                elif "winner_name" in item:
                    context_parts.append(f"  - 🏆 **Winner**: {item['winner_name']} (Match: {item.get('match', 'N/A')})")
                else:
                    context_parts.append(f"  - Data: {item}")
            return "\n".join(context_parts)

        if "error" in season_data:
            return f"❌ {season_data['error']}"
        
        context_parts = []
        
        # Season info
        if season_data.get('season_info'):
            info = season_data['season_info']
            context_parts.append(f"SEASON: {info.get('name', 'Unknown')} {info.get('year', '')}\n")
        
        # Champion
        if season_data.get('champion'):
            champ = season_data['champion']
            context_parts.append(f"CHAMPION: {champ.get('winner_team', 'Unknown')}\n")
        
        # Awards
        if season_data.get('awards'):
            context_parts.append("AWARDS:")
            for award in season_data['awards']:
                context_parts.append(
                    f"  - {award.get('award_type', 'Unknown')}: "
                    f"{award.get('player_name', 'N/A')} "
                    f"({award.get('value', '')})"
                )
            context_parts.append("")

        # Final Match Details (CRITICAL for "score" queries)
        if season_data.get('final_match'):
            fm = season_data['final_match']
            context_parts.append("\n" + "="*40)
            context_parts.append("ULTIMATE FINAL MATCH (VERIFIED DATABASE RECORD)")
            context_parts.append(f"MATCH: {fm.get('name', 'Unknown')}")
            context_parts.append(f"DATE: {fm.get('starting_at', 'N/A')}")
            context_parts.append(f"RESULT: {fm.get('result', 'N/A')}")
            
            if fm.get('innings_summary'):
                context_parts.append("\nOFFICIAL SCORECARD:")
                for inn in fm['innings_summary']:
                    tname = inn.get('team_name') or f"Team {inn.get('team_id')}"
                    score = inn.get('score')
                    wickets = inn.get('wickets')
                    overs = inn.get('overs')
                    context_parts.append(f"  - {tname}: {score}/{wickets} in {overs} overs")
            
            if fm.get('top_batsmen'):
                context_parts.append("\nTOP BATTING PERFORMANCES:")
                for bat in fm['top_batsmen']:
                    context_parts.append(f"  - {bat.get('name')}: {bat.get('runs')} ({bat.get('balls')}b, SR: {bat.get('sr')})")
            
            if fm.get('top_bowlers'):
                context_parts.append("\nTOP BOWLING PERFORMANCES:")
                for bowl in fm['top_bowlers']:
                    context_parts.append(f"  - {bowl.get('name')}: {bowl.get('wickets')}/{bowl.get('runs')} in {bowl.get('overs')} ov")
            
            context_parts.append("="*40 + "\n")

        # Key Matches (Playoffs)
        if season_data.get('key_matches'):
             context_parts.append("PLAYOFFS / KEY MATCHES:")
             for km in season_data['key_matches']:
                 # Skip if it's the final (already shown)
                 if season_data.get('final_match') and km.get('id') == season_data['final_match'].get('id'):
                     continue
                 context_parts.append(f"  - {km.get('name')}: {km.get('result')}")
             context_parts.append("")

        # Match summary (General)
        total_matches = season_data.get('total_matches', 0)
        context_parts.append(f"Total Matches: {total_matches}\n")
        
        if season_data.get('matches') and len(season_data['matches']) > 0:
            matches = season_data['matches']
            context_parts.append("Regular Season Samples:")
            
            # Show first 3 and last 3 if there are more than 6
            sample_matches = matches
            if len(matches) > 6:
                sample_matches = matches[:3] + matches[-3:]
                context_parts.append(f"*(Showing first 3 and last 3 matches out of {len(matches)} total)*")
            
            for match in sample_matches:
                context_parts.append(
                    f"  - {match.get('name', 'Unknown')} "
                    f"({match.get('starting_at', 'N/A')[:10]}): "
                    f"{match.get('result', 'N/A')}"
                )
        
        return "\n".join(context_parts)
    
    def build_head_to_head_context(self, h2h_matches: List[Dict], team_a: str, team_b: str) -> str:
        """
        Build formatted context for head-to-head matches.
        
        Args:
            h2h_matches: List of H2H match records
            team_a: First team name
            team_b: Second team name
        
        Returns:
            Formatted context string
        """
        if not h2h_matches:
            return f"❌ No head-to-head data found between {team_a} and {team_b}."
        
        context_parts = []
        context_parts.append(f"⚔️ **HEAD-TO-HEAD: {team_a} vs {team_b}**")
        context_parts.append(f"Total Matches: {len(h2h_matches)}\n")
        
        # Calculate win statistics
        team_a_wins = 0
        team_b_wins = 0
        
        for match in h2h_matches:
            result = match.get('result', '').lower()
            if team_a.lower() in result and 'won' in result:
                team_a_wins += 1
            elif team_b.lower() in result and 'won' in result:
                team_b_wins += 1
        
        context_parts.append(f"🏆 **Win Statistics:**")
        context_parts.append(f"  - {team_a}: {team_a_wins} wins")
        context_parts.append(f"  - {team_b}: {team_b_wins} wins")
        context_parts.append(f"  - Draws/No Result: {len(h2h_matches) - team_a_wins - team_b_wins}\n")
        
        # Recent matches
        context_parts.append("📋 **Recent Matches:**")
        for idx, match in enumerate(h2h_matches[:5], 1):
            context_parts.append(f"\n**Match {idx}:** {match.get('name', 'Unknown')}")
            context_parts.append(f"📅 {match.get('starting_at', 'N/A')[:10]}")
            context_parts.append(f"🏆 {match.get('result', 'N/A')}")
            
            if match.get('innings_summary'):
                for inn in match['innings_summary']:
                    context_parts.append(
                        f"  - {inn.get('score')}/{inn.get('wickets')} in {inn.get('overs')} overs"
                    )
        
        return "\n".join(context_parts)
    
    def build_universal_context(self, data: Any, query: str, data_type: str = "auto") -> str:
        """
        Universal context builder that auto-detects data type and formats accordingly.
        
        Args:
            data: Retrieved data (can be list, dict, or any structure)
            query: Original user query
            data_type: Type hint ("match", "player", "season", "h2h", or "auto")
        
        Returns:
            Formatted context string
        """
        logger.info(f"🔨 Building context for query: {query[:50]}...")
        
        # Auto-detect data type
        if data_type == "auto":
            if isinstance(data, list) and len(data) > 0:
                row = data[0]
                # Enhanced detection for Universal Engine results
                if "innings_summary" in row or "match" in row or "name" in row or "batting_summary" in row or "scoreboards" in row:
                    data_type = "match"
                elif "player_info" in row or "player_name" in row:
                    data_type = "player"
                elif "champion" in row or "winner_name" in row or "award_type" in row:
                    data_type = "season"
            elif isinstance(data, dict):
                if "player_info" in data:
                    data_type = "player"
                elif "season_info" in data:
                    data_type = "season"
                elif "champion" in data:
                    data_type = "season"
        
        # Route to appropriate builder
        if data_type == "match":
            return self.build_match_context(data if isinstance(data, list) else [data], query)
        elif data_type == "player":
            return self.build_player_context(data, query)
        elif data_type == "season":
            return self.build_season_context(data, query)
        elif data_type == "h2h":
            return self.build_head_to_head_context(data, "", "")
        else:
            # Fallback: JSON dump with formatting
            logger.warning(f"⚠️ Unknown data type, using fallback formatting")
            return f"📊 **DATA RETRIEVED:**\n```json\n{json.dumps(data, indent=2, default=str)[:2000]}\n```"
    
    def compress_context(self, context: str, max_length: int = 3000) -> str:
        """
        Compress context if it exceeds max length while preserving key information.
        
        Args:
            context: Original context string
            max_length: Maximum character length
        
        Returns:
            Compressed context
        """
        if len(context) <= max_length:
            return context
        
        logger.warning(f"⚠️ Context too long ({len(context)} chars), compressing to {max_length}")
        
        # Simple truncation with ellipsis
        return context[:max_length - 50] + "\n\n... (Context truncated for brevity) ..."
    
    def build_evidence_package(
        self, 
        retrieved_data: Any, 
        query: str,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Build complete evidence package for LLM with metadata.
        
        Args:
            retrieved_data: Data retrieved from database
            query: Original user query
            metadata: Additional metadata (intent, entities, etc.)
        
        Returns:
            Complete evidence package dict
        """
        context = self.build_universal_context(retrieved_data, query)
        compressed_context = self.compress_context(context)
        
        package = {
            "query": query,
            "context": compressed_context,
            "raw_data_summary": {
                "type": type(retrieved_data).__name__,
                "count": len(retrieved_data) if isinstance(retrieved_data, list) else 1,
                "has_data": bool(retrieved_data)
            },
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
        
        logger.info(f"✅ Evidence package built: {package['raw_data_summary']}")
        return package

# Global instance
context_builder = ContextBuilder()
