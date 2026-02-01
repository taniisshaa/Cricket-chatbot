import os
import subprocess
import sys

def run_command(command):
    try:
        subprocess.check_call(command, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    print("\n🏏 Cricket Chatbot GitHub Setup 🏏\n")
    print("Step 1: Go to https://github.com/new and create a new repository.")
    print("Step 2: Copy the HTTPS URL (e.g., https://github.com/YourName/cricket-chatbot.git)\n")
    
    repo_url = input("Enter your new GitHub Repository URL: ").strip()
    
    if not repo_url.startswith("https://github.com"):
        print("❌ Invalid URL. It should start with 'https://github.com'")
        return

    print(f"\n🚀 Configuring remote 'origin' to {repo_url}...")
    
    # Remove existing origin if it exists to avoid errors
    run_command("git remote remove origin")
    
    # Add remote
    if run_command(f"git remote add origin {repo_url}"):
        print("✅ Remote added.")
    else:
        print("❌ Failed to add remote.")
        return

    # Rename branch
    print("🔄 Renaming branch to 'main'...")
    run_command("git branch -M main")
    
    # Push
    print("📤 Pushing code to GitHub... (You may be asked to sign in via browser)")
    if run_command("git push -u origin main"):
        print("\n✅ SUCCESS! Your code is now on GitHub.")
        print("\n👇 NEXT STEP: ADD COLLABORATOR 👇")
        print(f"1. Go to: {repo_url}/settings/access")
        print("2. Click 'Add people'")
        print("3. Enter your friend's username or email.")
    else:
        print("\n❌ Push failed. Please check your internet or GitHub permissions.")

if __name__ == "__main__":
    main()
