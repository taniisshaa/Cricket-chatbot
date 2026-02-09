import re

def peek_logs():
    with open('logs/universal_engine.log', 'r') as f:
        lines = f.readlines()
    
    # Get last 50 lines
    for line in lines[-50:]:
        print(line.strip())

if __name__ == "__main__":
    peek_logs()
