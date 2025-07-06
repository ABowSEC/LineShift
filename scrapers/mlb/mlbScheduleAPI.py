#!/usr/bin/env python3
import sqlite3
import requests
from datetime import datetime, timezone, timedelta
import json

# Config
DB_NAME = "data/mlb_odds.db"
MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
PROVIDER = "MLB-API"

def fetch_mlb_schedule(start_date=None, end_date=None):
    """
    Fetch MLB schedule from the official API
    """
    if not start_date:
        start_date = datetime.now().date()
    if not end_date:
        end_date = start_date + timedelta(days=7)
    
    url = f"{MLB_API_BASE}/schedule"
    params = {
        "sportId": 1,  # MLB
        "hydrate": "probablePitcher",
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d")
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching MLB schedule: {e}")
        return None

def insert_game_schedule(cursor, game_data):
    """
    Insert or update game schedule data from MLB API
    """
    game_pk = game_data["gamePk"]
    game_date = game_data["officialDate"]
    game_time = game_data["gameDate"]  # ISO timestamp
    
    # Parse teams
    away_team = game_data["teams"]["away"]["team"]["name"]
    home_team = game_data["teams"]["home"]["team"]["name"]
    
    # Get probable pitchers
    away_pitcher = None
    home_pitcher = None
    
    # Check for probable pitchers in away team
    away_team_data = game_data["teams"]["away"]
    if "probablePitcher" in away_team_data and away_team_data["probablePitcher"]:
        away_pitcher = away_team_data["probablePitcher"].get("fullName", "TBD")
    
    # Check for probable pitchers in home team
    home_team_data = game_data["teams"]["home"]
    if "probablePitcher" in home_team_data and home_team_data["probablePitcher"]:
        home_pitcher = home_team_data["probablePitcher"].get("fullName", "TBD")
    
    # Convert game time to local time
    try:
        dt_utc = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(timezone(timedelta(hours=-6)))  # Adjust to your timezone
        start_time = dt_local.strftime("%I:%M%p").lstrip("0")
    except:
        start_time = "TBD"
    
    # Create game ID that matches your existing format
    game_id = f"{away_team}@{home_team} {start_time}"
    
    # Format date for display
    display_date = datetime.strptime(game_date, "%Y-%m-%d").strftime("%a %b %d").upper()
    
    cursor.execute(
        """
        INSERT OR REPLACE INTO games
            (game_id, start_time, game_date, home_team, away_team, home_pitcher, away_pitcher)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (game_id, start_time, display_date, home_team, away_team, home_pitcher, away_pitcher),
    )
    
    return game_id

def main():
    print("Fetching MLB schedule from official API...")
    
    # Fetch schedule for next 7 days to focus on games closer to game day
    # Teams typically announce probable pitchers 1-2 days before the game
    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=7)
    
    schedule_data = fetch_mlb_schedule(start_date, end_date)
    if not schedule_data:
        print("Failed to fetch schedule data")
        return
    
    games_processed = 0
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cur = conn.cursor()
            
            # Process each date in the schedule
            for date_data in schedule_data.get("dates", []):
                date = date_data["date"]
                games = date_data.get("games", [])
                
                print(f"Processing {len(games)} games for {date}")
                
                for game in games:
                    try:
                        game_id = insert_game_schedule(cur, game)
                        games_processed += 1
                        
                        # Debug: Show pitcher info
                        away_team = game["teams"]["away"]["team"]["name"]
                        home_team = game["teams"]["home"]["team"]["name"]
                        away_pitcher = game["teams"]["away"].get("probablePitcher", {}).get("fullName", "None")
                        home_pitcher = game["teams"]["home"].get("probablePitcher", {}).get("fullName", "None")
                        
                        # Show game status
                        game_status = game.get("status", {}).get("detailedState", "Unknown")
                        print(f"Added: {game_id} ({game_status})")
                        print(f"  Away: {away_pitcher} | Home: {home_pitcher}")
                    except Exception as e:
                        print(f"Error processing game: {e}")
                        continue
            
            conn.commit()
            print(f"Successfully processed {games_processed} games from MLB API")
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main() 