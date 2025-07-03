#!/usr/bin/env python3
import sqlite3
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Config
DB_NAME   = "nfl_odds.db"
ESPN_URL  = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
PROVIDER  = "ESPN"


# Data Fetch with retries
def fetch_scoreboard_json():
    """Fetch ESPN NFL scoreboard JSON with a simple retry policy."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    resp = session.get(ESPN_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()


# Database helper functions
def insert_game(cursor, game_id, start_time, game_date, home_team, away_team):
    """
    Insert a game if it doesn't already exist.
    """
    cursor.execute(
        """
        INSERT OR IGNORE INTO games
            (game_id, start_time, game_date, home_team, away_team)
        VALUES (?, ?, ?, ?, ?)
        """,
        (game_id, start_time, game_date, home_team, away_team),
    )

def insert_odds(cursor, game_id, provider, spread_details, over_under, ml_home, ml_away):
    """
    Insert a new odds row with a UTC timestamp.
    """
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    cursor.execute(
        """
        INSERT INTO odds
            (game_id, provider, spread_details, over_under, moneyline_home, moneyline_away, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (game_id, provider, spread_details, over_under, ml_home, ml_away, ts),
    )

# Main orchestration
def main():
    data = fetch_scoreboard_json().get("events", [])
    if not data:
        print(" No events found in ESPN response.")
        return

    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.cursor()

        for event in data:
            # Basic game info
            game_id    = event["id"]
            iso_ts = event["date"]  # ISO string
            dt = datetime.fromisoformat(iso_ts.replace("Z","+00:00"))
            game_date= dt.date().isoformat()
            comp  = event.get("competitions", [])[0]
            teams = comp.get("competitors", [])
            home  = next(t for t in teams if t["homeAway"] == "home")
            away  = next(t for t in teams if t["homeAway"] == "away")

            insert_game(
                cur,
                game_id,
                iso_ts,
                game_date,
                home["team"]["name"],
                away["team"]["name"]
            )

            # Odds (if present)
            odds_list = comp.get("odds", [])
            if odds_list:
                oList = odds_list[0]
                provider       = oList.get("provider", {}).get("name", PROVIDER)
                spread_details = oList.get("details")
                over_under     = oList.get("overUnder")
                ml_home        = oList.get("moneylineHome")
                ml_away        = oList.get("moneylineAway")

                insert_odds(
                    cur,
                    game_id,
                    provider,
                    spread_details,
                    over_under,
                    ml_home,
                    ml_away
                )
                print(f"Odds for {away['team']['name']} @ {home['team']['name']} via {provider}")

        # commit happens automatically on with-block exit

    print("ESPN odds import complete.")

if __name__ == "__main__":
    main()

