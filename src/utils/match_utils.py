import re
def _normalize(s):
    """Deeply normalize string for cricket entity matching."""
    if not s: return ""
    s = str(s).lower().strip()
    s = re.sub(r'[^a-z0-9\s]', '', s)
    return " ".join(s.split())
def _is_initials_match(query, name):
    """Checks if 'query' is an initials/acronym match for 'name'."""
    q = _normalize(query).replace(" ", "")
    if len(q) < 2: return False
    n_parts = _normalize(name).split()
    if not n_parts: return False
    initials = "".join([p[0] for p in n_parts if p])
    if q == initials: return True
    if len(q) <= len(initials) and q == initials[:len(q)]: return True
    return False
def _is_team_match(query_name, target_name):
    """Robust matching for team names."""
    q = _normalize(query_name)
    t = _normalize(target_name)
    if not q or not t: return False
    if q in t or t in q: return True
    if _is_initials_match(query_name, target_name): return True
    q_tokens = q.split()
    t_tokens = t.split()
    for qt in q_tokens:
        if len(qt) > 3 and qt in t_tokens: return True
    return False
def _smart_ctx_match(fixture, scope):
    if not scope: return False
    f_name = fixture.get("name") or ""
    return _is_team_match(scope, f_name)
def _match_series_name(query, candidate_name):
    """Matches series names using robust fuzzy logic."""
    if not query or not candidate_name: return False
    return _is_team_match(query, candidate_name)