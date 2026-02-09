def extract_last_sql():
    with open('logs/universal_engine.log', 'r') as f:
        content = f.read()
        
    start_tag = "Generated SQL:"
    end_tag = "Execution Result:"
    
    last_start = content.rfind(start_tag)
    if last_start == -1:
        print("No SQL found")
        return
        
    last_end = content.find(end_tag, last_start)
    if last_end == -1:
        # Maybe it failed before execution?
        sql = content[last_start + len(start_tag):].strip()
    else:
        sql = content[last_start + len(start_tag):last_end].strip()
        
    print("--- LAST GENERATED SQL ---")
    print(sql)
    
    # Also check for errors
    error_tag = "SQL Error:"
    last_error = content.rfind(error_tag)
    if last_error != -1:
        print("\n--- LAST SQL ERROR ---")
        print(content[last_error:].split('\n')[0])

if __name__ == "__main__":
    extract_last_sql()
