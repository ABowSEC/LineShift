# A. Bowman LineShift — Custom Proprietary Software License
# Copyright © 2025 A. Bowman. All rights reserved.
#
import requests
import sqlite3
from datetime import datetime 

websiteURL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"

#COnnecting SQLite
conn = sqlite3.connect("nfl_odds.db")
cursor = conn.cursor()


#Table Creation
                        #Game
cursor.execute('''CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    start_time,
    home_team TEXT,
    away_team TEXT
)''')
                        #ODDS
cursor.execute('''CREATE TABLE IF NOT EXISTS odds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT,
    provider TEXT,
    spread_details TEXT,
    over_under REAL,
    moneyline_home INTEGER,
    moneyline_away INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
)''')


def fetch_data():
    try:
        response = requests.get(websiteURL)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(" Failed to fetch ESPN data:", e)
        return {}


def insertGameAndOdds(event):
    gameID= event["id"]
    start_time= event["date"] #ISO Format

    comp = event["competitions"][0]
    teams= comp["competitors"]

    homeTeam = next(t["team"]["name"] for t in teams if t["homeAway"] == "home")
    awayTeam = next(t["team"]["name"] for t in teams if t["homeAway"] == "away")#????

# Insert into games (if not exists)
    cursor.execute("INSERT OR IGNORE INTO games (game_id, start_time, home_team, away_team) VALUES (?, ?, ?, ?)",
              (gameID, start_time, homeTeam, awayTeam))

    # Odds block
    if "odds" in comp and len(comp["odds"]) > 0:
        odds = comp["odds"][0]  
        provider = odds.get("provider", {}).get("name", "unknown")
        spread_details = odds.get("details")
        over_under = odds.get("overUnder")
        moneyline_home = odds.get("moneylineHome")
        moneyline_away = odds.get("moneylineAway")

        cursor.execute('''INSERT INTO odds (game_id, provider, spread_details, over_under, moneyline_home, moneyline_away)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (gameID, provider, spread_details, over_under, moneyline_home, moneyline_away))
        print(f"Inserted {awayTeam} @ {homeTeam} | Provider: {provider}")

def main():
    data = fetch_data()
    for event in data.get("events", []):
        insertGameAndOdds(event)
    conn.commit()
    print("Import complete.")

if __name__ == "__main__":
    main()    

#Still Needs Auto Pull
