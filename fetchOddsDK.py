#!/usr/bin/env python3
import sqlite3
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Config

DB_NAME    = "nfl_odds.db"
NFL_URL    = "https://sportsbook.draftkings.com/leagues/football/nfl"
PROVIDER   = "DraftKings-Web"


# Database helpers

def insert_game(cursor, game_id, start_time, home_team, away_team):
    """
    Insert the game row if it doesn't already exist.
    """
    cursor.execute(
        """
        INSERT OR IGNORE INTO games
            (game_id, start_time, home_team, away_team)
        VALUES (?, ?, ?, ?)
        """,
        (game_id, start_time, home_team, away_team),
    )

def insert_odds(cursor, game_id, spread, total, ml_home, ml_away):
    """
    Insert a new odds row with UTC timestamp (no microseconds).
    """
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    cursor.execute(
        """
        INSERT INTO odds
            (game_id, provider, spread_details, over_under, moneyline_home, moneyline_away, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (game_id, PROVIDER, spread, total, ml_home, ml_away, ts),
    )

# Scraping logic

def scrape_nfl_odds():
    """
    Headlessly scrape DraftKings NFL odds and return a list of dicts.
    """
    games = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()

        try:
            page.goto(NFL_URL, timeout=60000)
            page.wait_for_selector("table.sportsbook-table tbody tr", timeout=30000)
        except PlaywrightTimeoutError as e:
            print(f"Failed to load page: {e}")
            browser.close()
            return games

        rows = page.query_selector_all("table.sportsbook-table tbody tr")
        print(f"Found {len(rows)} rows (2 per game)")

        # Process pairs: away_row, home_row
        for i in range(0, len(rows), 2):
            try:
                away = rows[i]
                home = rows[i + 1]

                # Teams
                away_team = away.query_selector(".event-cell__name-text").inner_text().strip()
                home_team = home.query_selector(".event-cell__name-text").inner_text().strip()

                # Spread
                spreads = [e.inner_text().strip()
                           for e in away.query_selector_all('[data-testid="sportsbook-outcome-cell-line"]')]
                spread = " | ".join(spreads) if spreads else None

                # Total (O/U)
                total_cell = away.query_selector_all("td")[1]
                total_elem = total_cell.query_selector('[data-testid="sportsbook-outcome-cell-line"]')
                total = total_elem.inner_text().strip() if total_elem else None

                # Moneylines
                away_ml = away.query_selector_all('[data-testid="sportsbook-odds"]')
                home_ml = home.query_selector_all('[data-testid="sportsbook-odds"]')
                ml_away = away_ml[-1].inner_text().strip() if away_ml else None
                ml_home = home_ml[-1].inner_text().strip() if home_ml else None

                # Start time
                start_elem = away.query_selector(".event-cell__start-time")
                start_time = start_elem.inner_text().strip() if start_elem else "TBD"

                game_id = f"{away_team}@{home_team} {start_time}"

                games.append({
                    "game_id":     game_id,
                    "start_time":  start_time,
                    "home_team":   home_team,
                    "away_team":   away_team,
                    "spread":      spread,
                    "total":       total,
                    "ml_home":     ml_home,
                    "ml_away":     ml_away,
                })

                print(f"Parsed: {away_team} @ {home_team} ({start_time})")

            except Exception as e:
                print(f"Skipped row {i}: {e}")

        browser.close()
    return games

# main
def main():
    data = scrape_nfl_odds()
    if not data:
        print("No games scraped; exiting.")
        return

    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.cursor()
        for g in data:
            insert_game(cur, g["game_id"], g["start_time"], g["home_team"], g["away_team"])
            insert_odds(cur, g["game_id"], g["spread"], g["total"], g["ml_home"], g["ml_away"])
        conn.commit()

    print(f"Stored odds for {len(data)} games into `{DB_NAME}`")

if __name__ == "__main__":
    main()
