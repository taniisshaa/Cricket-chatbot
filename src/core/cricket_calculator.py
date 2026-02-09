from typing import Dict, Optional

class CricketCalculator:
    """
    Dedicated module for calculating cricket statistics and projections.
    Handles metrics that are not directly available in standard scorecards.
    """
    
    @staticmethod
    def calculate_projected_score(current_runs: int, overs_bowled: float, wickets_lost: int, total_overs: int = 20) -> Dict[str, str]:
        """
        Calculate projected score based on Current Run Rate (CRR).
        """
        if overs_bowled <= 0:
            return {"error": "Overs bowled must be greater than 0"}
            
        crr = current_runs / overs_bowled
        projected_crr = int(crr * total_overs)
        
        # Simple heuristic adjustments for wickets lost
        # If wickets < 3, can accelerate (add 10-15%)
        # If wickets > 7, likely to collapse (subtract 10-20%)
        
        optimistic = projected_crr
        conservative = projected_crr
        
        if wickets_lost <= 2:
            optimistic = int(projected_crr * 1.15)
        elif wickets_lost >= 7:
            conservative = int(projected_crr * 0.85)
            
        return {
            "current_run_rate": f"{crr:.2f}",
            "projected_score_at_crr": str(projected_crr),
            "optimistic_projection": str(optimistic),
            "conservative_projection": str(conservative),
            "note": f"At {crr:.2f} RPO. Projections vary based on remaining wickets ({10 - wickets_lost} left)."
        }

    @staticmethod
    def calculate_required_run_rate(target: int, current_runs: int, overs_remaining: float) -> Dict[str, str]:
        """
        Calculate Required Run Rate (RRR) to win.
        """
        runs_needed = target - current_runs
        if overs_remaining <= 0:
            if runs_needed <= 0:
                return {"result": "Match Won"}
            return {"result": "Match Lost (Overs finished)"}
            
        rrr = runs_needed / overs_remaining
        return {
            "runs_needed": str(runs_needed),
            "balls_remaining": str(int(overs_remaining * 6)),
            "required_run_rate": f"{rrr:.2f}",
            "equation": f"{runs_needed} runs needed in {int(overs_remaining * 6)} balls."
        }
        
    @staticmethod
    def calculate_nrr(runs_scored: int, overs_faced: float, runs_conceded: int, overs_bowled: float) -> str:
        """
        Calculate Net Run Rate.
        NRR = (Runs Scored / Overs Faced) - (Runs Conceded / Overs Bowled)
        """
        if overs_faced <= 0 or overs_bowled <= 0:
            return "N/A (Overs data missing)"
            
        scoring_rate = runs_scored / overs_faced
        conceding_rate = runs_conceded / overs_bowled
        nrr = scoring_rate - conceding_rate
        return f"{nrr:+.3f}"

    @staticmethod
    def interpret_asking_rate(runs_needed: int, balls_remaining: int) -> str:
        """
        Provide a verbal interpretation of the difficulty.
        """
        if balls_remaining == 0: return "Match Finished"
        rpo = (runs_needed / balls_remaining) * 6
        
        if rpo > 15: return "Impossible (Requires specific miracle)"
        if rpo > 12: return "Very Difficult (Requires boundaries every over)"
        if rpo > 10: return "Difficult (Pressure High)"
        if rpo > 8: return "Challenging but doable"
        if rpo > 6: return "Moderate (Standard chase)"
        return "Easy (Cruising)"

cricket_calculator = CricketCalculator()
