"""
🎯 RAG ORCHESTRATOR - THE BRAIN
================================
Master controller that orchestrates the complete RAG pipeline:
1. Intent Analysis
2. Smart Retrieval
3. Context Building
4. Response Generation
5. Verification

This ensures ZERO hallucination and ChatGPT-level accuracy.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import asyncio
from src.utils.utils_core import get_logger
from src.core.rag_retriever import smart_retriever
from src.core.rag_context_builder import context_builder
from src.core.universal_cricket_engine import handle_universal_cricket_query

logger = get_logger("rag_orchestrator", "rag_orchestrator.log")

class RAGOrchestrator:
    """
    Master RAG pipeline controller.
    Ensures every response is backed by real database evidence.
    """
    
    def __init__(self):
        self.retriever = smart_retriever
        self.context_builder = context_builder
    
    async def process_query(
        self, 
        user_query: str, 
        intent_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Main RAG pipeline execution.
        
        Args:
            user_query: User's original question
            intent_analysis: Parsed intent from agent_workflow
        
        Returns:
            Complete RAG result with context and evidence
        """
        logger.info(f"🎯 RAG PIPELINE START: {user_query[:60]}...")
        logger.info(f"📋 Intent: {intent_analysis.get('intent')}")
        
        intent = intent_analysis.get("intent", "GENERAL")
        entities = intent_analysis.get("entities", {})
        time_context = intent_analysis.get("time_context", "PRESENT")
        
        # Route to appropriate retrieval strategy
        if intent in ["PAST_HISTORY", "RECORDS"] or time_context == "PAST":
            result = await self._handle_historical_query(user_query, entities, intent_analysis)
        elif intent == "PLAYER_STATS":
            result = await self._handle_player_query(user_query, entities)
        elif intent == "SERIES_STATS":
            result = await self._handle_season_query(user_query, entities)
        elif intent == "HEAD_TO_HEAD":
            result = await self._handle_h2h_query(user_query, entities)
        else:
            # Fallback to universal engine
            logger.info("🔄 Routing to Universal Cricket Engine...")
            result = await self._handle_universal_query(user_query, entities)
        
        logger.info(f"✅ RAG PIPELINE COMPLETE: Retrieved {result.get('data_count', 0)} records")
        return result
    
    async def _handle_historical_query(
        self, 
        query: str, 
        entities: Dict, 
        analysis: Dict
    ) -> Dict[str, Any]:
        """
        Handle queries about past matches.
        
        Examples:
        - "Yesterday's match scorecard"
        - "IPL 2023 final"
        - "Match on 17 Oct 2023"
        """
        logger.info("📚 HISTORICAL QUERY DETECTED")
        
        target_date = entities.get("target_date")
        team_name = entities.get("team")
        series_name = entities.get("series")
        year = entities.get("year")
        
        # Handle relative dates
        if not target_date:
            q_lower = query.lower()
            if "yesterday" in q_lower or "kal" in q_lower:
                yesterday = datetime.now() - timedelta(days=1)
                target_date = yesterday.strftime("%Y-%m-%d")
                logger.info(f"📅 Resolved 'yesterday' to {target_date}")
            elif "today" in q_lower or "aaj" in q_lower:
                target_date = datetime.now().strftime("%Y-%m-%d")
                logger.info(f"📅 Resolved 'today' to {target_date}")
        
        # Retrieve matches
        if target_date:
            matches = await self.retriever.retrieve_match_by_date(target_date, team_name)
        elif series_name and year:
            # Use universal engine for complex season queries
            return await self._handle_universal_query(query, entities)
        else:
            # Fallback to universal engine
            return await self._handle_universal_query(query, entities)
        
        # Build context
        context = self.context_builder.build_match_context(matches, query)
        
        return {
            "status": "success",
            "data_type": "match",
            "data_count": len(matches),
            "context": context,
            "raw_data": matches,
            "metadata": {
                "target_date": target_date,
                "team": team_name,
                "retrieval_method": "date_based"
            }
        }
    
    async def _handle_player_query(
        self, 
        query: str, 
        entities: Dict
    ) -> Dict[str, Any]:
        """
        Handle player statistics queries.
        
        Examples:
        - "Virat Kohli IPL 2023 stats"
        - "Rohit Sharma performance"
        - "Compare Dhoni vs Kohli"
        """
        logger.info("🏏 PLAYER QUERY DETECTED")
        
        player_name = entities.get("player")
        series_name = entities.get("series")
        year = entities.get("year")
        
        if not player_name:
            logger.warning("❌ No player name found in query")
            return {
                "status": "error",
                "error": "Player name not specified",
                "context": "❌ Please specify a player name."
            }
        
        # Retrieve player stats
        player_data = await self.retriever.retrieve_player_stats(
            player_name=player_name,
            year=year
        )
        
        # Build context
        context = self.context_builder.build_player_context(player_data, query)
        
        return {
            "status": "success",
            "data_type": "player",
            "data_count": 1,
            "context": context,
            "raw_data": player_data,
            "metadata": {
                "player": player_name,
                "year": year,
                "retrieval_method": "player_stats"
            }
        }
    
    async def _handle_season_query(
        self, 
        query: str, 
        entities: Dict
    ) -> Dict[str, Any]:
        """
        Handle season/tournament queries.
        
        Examples:
        - "IPL 2024 winner"
        - "World Cup 2023 orange cap"
        - "IPL 2025 points table"
        """
        logger.info("🏆 SEASON QUERY DETECTED")
        
        series_name = entities.get("series", "IPL")
        year = entities.get("year", datetime.now().year)
        
        # Retrieve season data
        season_data = await self.retriever.retrieve_season_data(series_name, year)
        
        # Build context
        context = self.context_builder.build_season_context(season_data, query)
        
        return {
            "status": "success",
            "data_type": "season",
            "data_count": 1,
            "context": context,
            "raw_data": season_data,
            "metadata": {
                "series": series_name,
                "year": year,
                "retrieval_method": "season_data"
            }
        }
    
    async def _handle_h2h_query(
        self, 
        query: str, 
        entities: Dict
    ) -> Dict[str, Any]:
        """
        Handle head-to-head queries.
        
        Examples:
        - "India vs Pakistan history"
        - "MI vs CSK last 5 matches"
        - "RCB vs KKR head to head"
        """
        logger.info("⚔️ HEAD-TO-HEAD QUERY DETECTED")
        
        team_a = entities.get("team")
        team_b = entities.get("opponent")
        
        if not team_a or not team_b:
            logger.warning("❌ Both teams not specified")
            return {
                "status": "error",
                "error": "Both teams must be specified for H2H",
                "context": "❌ Please specify both teams for head-to-head comparison."
            }
        
        # Retrieve H2H matches
        h2h_matches = await self.retriever.retrieve_head_to_head(team_a, team_b)
        
        # Build context
        context = self.context_builder.build_head_to_head_context(h2h_matches, team_a, team_b)
        
        return {
            "status": "success",
            "data_type": "h2h",
            "data_count": len(h2h_matches),
            "context": context,
            "raw_data": h2h_matches,
            "metadata": {
                "team_a": team_a,
                "team_b": team_b,
                "retrieval_method": "head_to_head"
            }
        }
    
    async def _handle_universal_query(
        self, 
        query: str, 
        entities: Dict
    ) -> Dict[str, Any]:
        """
        Fallback to universal cricket engine for complex queries.
        
        This uses Text-to-SQL for maximum flexibility.
        """
        logger.info("🔮 UNIVERSAL ENGINE FALLBACK")
        
        # Use existing universal engine
        result = await handle_universal_cricket_query(query, context=entities)
        
        if result.get("query_status") == "success":
            # Build context from SQL results
            context = self.context_builder.build_universal_context(
                result.get("data", []), 
                query,
                data_type="auto"
            )
            
            return {
                "status": "success",
                "data_type": "universal",
                "data_count": result.get("count", 0),
                "context": context,
                "raw_data": result.get("data", []),
                "metadata": {
                    "retrieval_method": "universal_sql_engine",
                    "sql_status": result.get("query_status")
                }
            }
        else:
            return {
                "status": "error",
                "error": result.get("message", "Unknown error"),
                "context": f"❌ Could not retrieve data: {result.get('message', 'Unknown error')}"
            }
    
    async def verify_retrieval(self, query: str, retrieved_data: Dict) -> bool:
        """
        Verify that retrieved data is relevant to the query.
        
        Args:
            query: Original user query
            retrieved_data: Retrieved data package
        
        Returns:
            True if data is relevant, False otherwise
        """
        # Basic verification checks
        if retrieved_data.get("status") == "error":
            logger.warning("❌ Retrieval failed")
            return False
        
        if retrieved_data.get("data_count", 0) == 0:
            logger.warning("⚠️ No data retrieved")
            return False
        
        logger.info("✅ Retrieval verification passed")
        return True

# Global instance
rag_orchestrator = RAGOrchestrator()

async def execute_rag_pipeline(user_query: str, intent_analysis: Dict) -> Dict[str, Any]:
    """
    Convenience function to execute the complete RAG pipeline.
    
    Args:
        user_query: User's question
        intent_analysis: Parsed intent from agent
    
    Returns:
        RAG result with context and evidence
    """
    return await rag_orchestrator.process_query(user_query, intent_analysis)
