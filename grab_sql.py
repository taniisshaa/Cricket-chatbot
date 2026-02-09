def grab_full_sql():
    with open('logs/universal_engine.log', 'r') as f:
        content = f.read()
    
    matches = list(re.finditer(r"Generated SQL: (.*?)(?=\d{4}-\d{2}-\d{2}|$)", content, re.DOTALL))
    if matches:
        last_match = matches[-1]
        print("--- FULL SQL ---")
        print(last_match.group(1).split('|')[0].strip())
        
    errors = list(re.finditer(r"SQL Error: (.*?)(?=\n|$)", content))
    if errors:
        print("\n--- FULL ERROR ---")
        print(errors[-1].group(1).strip())

if __name__ == "__main__":
    import re
    grab_full_sql()
