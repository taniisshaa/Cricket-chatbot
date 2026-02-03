
import re

def _normalize(s):
    return (s or "").strip().lower()

def _is_initials_match(query, name):
    """
    Checks if 'query' is an acronym/initials for 'name'.
    e.g. "MI" -> "Mumbai Indians" (True)
         "CSK" -> "Chennai Super Kings" (True)
    """
    if not query or not name: return False
    q = _normalize(query).replace(" ", "")
    n_parts = _normalize(name).split()
    initials = "".join([p[0] for p in n_parts if p])
    if q == initials: return True
    if len(q) >= 2 and q in initials: return True
    return False

def _smart_ctx_match(m, scope):
    """
    Checks if match 'm' is relevant to 'scope' (team/series), handling aliases dynamically.
    """
    if not scope: return False
    scope_norm = _normalize(scope)
    name_norm = _normalize(m.get("name"))
    if scope_norm in name_norm: return True
    tag = _normalize(m.get("_matched_entity") or "")
    if tag and (scope_norm in tag or tag in scope_norm): return True
    if _is_initials_match(scope_norm, name_norm): return True
    return False

def _is_team_match(team, match_name):
    """
    Robustly checks if a team name matches a match name using logic rather than hardcoding.
    Handles acronyms (RCB, CSK), partial names (Royal Challengers), and variations.
    """
    if not team or not match_name: return False
    t_norm = _normalize(team)
    m_norm = _normalize(match_name)
    if t_norm in m_norm: return True
    if _is_initials_match(team, match_name) or _is_initials_match(match_name, team):
        return True
    t_tokens = [w for w in t_norm.split() if len(w) > 2]
    if not t_tokens: t_tokens = t_norm.split()
    matches = [w for w in t_tokens if w in m_norm]
    if t_tokens and len(matches) / len(t_tokens) >= 0.5:
        return True
    if len(t_norm) <= 5 and not t_norm.isnumeric():
        m_tokens = [w for w in m_norm.split() if len(w) > 2]
        m_initials = "".join([w[0] for w in m_tokens if w])
        if t_norm in m_initials: return True
    return False

def _is_finished(m):
    """
    Determines if a match is finished using a hierarchy of checks:
    1. API Flags (matchEnded, matchWinner)
    2. Cricket Rules Logic (Scores, Overs, Wickets)
    3. Failure Safety (Status text)
    """
    if m.get("matchEnded"): return True
    if m.get("matchWinner") or m.get("winner"): return True
    try:
        score = m.get("score")
        if score and isinstance(score, list):
            m_type = str(m.get("matchType", "")).lower()
            max_overs = 20 if "t20" in m_type else 50 if "odi" in m_type else 100
            if "test" in m_type: max_overs = 0
            inn1 = next((s for s in score if "Inning 1" in s.get("inning", "")), None)
            inn2 = next((s for s in score if "Inning 2" in s.get("inning", "")), None)
            if inn1 and inn2:
                r1 = int(inn1.get("r", 0))
                r2 = int(inn2.get("r", 0))
                w2 = int(inn2.get("w", 0))
                o2 = float(inn2.get("o", 0))
                if r2 > r1: return True
                if w2 >= 10: return True
                if max_overs > 0 and o2 >= max_overs: return True
    except Exception:
        pass
    status = str(m.get("status", "")).lower()
    if "won by" in status: return True
    return False

def _match_series_name(query, candidate_name):
    """
    Logic-based matcher: checks substings and acronyms.
    e.g. 'IPL' matches 'Indian Premier League' dynamically.
    """
    if not query or not candidate_name: return False
    q = _normalize(query)
    c = _normalize(candidate_name)
    
    # 1. Direct substring match
    if q in c or c in q: return True
    
    # 2. Acronym Match (e.g. IPL == Indian Premier League)
    # Clean non-alphanumeric chars but keep spaces to split
    clean_c = "".join([x if x.isalpha() or x.isspace() else "" for x in c]) # Remove digits/symbols
    words = clean_c.split()
    acronym = "".join([w[0] for w in words])
    
    if q == acronym: return True
    
    # Handle Acronym+Year in Query (e.g. IPL2025) vs Name
    q_no_digits = "".join([x for x in q if x.isalpha()])
    if q_no_digits == acronym: return True
    
    # Check for number inclusion (T20WC -> T 20 World Cup)
    if any(char.isdigit() for char in q):
         pass 

    return False
