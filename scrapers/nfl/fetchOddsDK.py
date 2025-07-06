#!/usr/bin/env python3
import sqlite3
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Config

DB_NAME    = "data/nfl_odds.db"
NFL_URL    = "https://sportsbook.draftkings.com/leagues/football/nfl"
PROVIDER   = "DraftKings-Web"


# Database helpers

def nickname(full_name: str) -> str:
    """Return the team's nickname (last word of the full name)."""
    return full_name.strip().split()[-1]



def insert_game(cursor, game_id, start_time, game_date, home_team, away_team):
    """
    Insert the game row if it doesn't already exist.
    """
    cursor.execute(
        """
        INSERT OR IGNORE INTO games
            (game_id, start_time, game_date, home_team, away_team)
        VALUES (?, ?, ?, ?, ?)
        """,
        (game_id, start_time, game_date, home_team, away_team),
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

        # Find all tables and process each one
        tables = page.query_selector_all("table.sportsbook-table")
        print(f"Found {len(tables)} tables")
        
        for table_idx, table in enumerate(tables):
            # Extract date from this specific table's header
            try:
                date_elem = table.query_selector(".sportsbook-table-header__title span span")
                if date_elem:
                    game_date = date_elem.inner_text().strip()
                    print(f"Game date for table {table_idx + 1}: {game_date}")
                else:
                    game_date = "TBD"
                    print(f"Could not find game date in table {table_idx + 1} header")
            except Exception as e:
                game_date = "TBD"
                print(f"Error extracting game date from table {table_idx + 1}: {e}")
            
            # Get rows from this specific table
            rows = table.query_selector_all("tbody tr")
            print(f"Found {len(rows)} rows in table {table_idx + 1} (2 per game)")

            # Process pairs: away_row, home_row
            for i in range(0, len(rows), 2):
                try:
                    # Check if we have both away and home rows
                    if i + 1 >= len(rows):
                        print(f"Skipping incomplete game pair at row {i}")
                        continue
                        
                    away = rows[i]
                    home = rows[i + 1]

                    # Teams with null checks
                    away_team_elem = away.query_selector(".event-cell__name-text")
                    home_team_elem = home.query_selector(".event-cell__name-text")
                    
                    if not away_team_elem or not home_team_elem:
                        print(f"Skipping row {i}: Missing team names")
                        continue
                        
                    away_team = away_team_elem.inner_text().strip()
                    home_team = home_team_elem.inner_text().strip()

                    # Spread
                    spreads = [e.inner_text().strip()
                               for e in away.query_selector_all('[data-testid="sportsbook-outcome-cell-line"]')]
                    spread = " | ".join(spreads) if spreads else None

                    # Total (O/U)
                    total_cell = away.query_selector_all("td")[1] if len(away.query_selector_all("td")) > 1 else None
                    total = None
                    if total_cell:
                        total_elem = total_cell.query_selector('[data-testid="sportsbook-outcome-cell-line"]')
                        total = total_elem.inner_text().strip() if total_elem else None

                    # Moneylines
                    away_ml = away.query_selector_all('[data-testid="sportsbook-odds"]')
                    home_ml = home.query_selector_all('[data-testid="sportsbook-odds"]')
                    ml_away = away_ml[-1].inner_text().strip() if away_ml else None
                    ml_home = home_ml[-1].inner_text().strip() if home_ml else None

                    # Start time
                    raw_time_elem = away.query_selector(".event-cell__start-time")
                    raw_time = raw_time_elem.inner_text().strip() if raw_time_elem else "TBD"
                    time_str = raw_time.replace(" ", "").upper()    
         

                    home_nick = nickname(home_team)
                    away_nick = nickname(away_team)        

                    game_id = f"{away_nick}@{home_nick} {time_str}"

                    games.append({
                        "game_id":     game_id,
                        "start_time":  time_str,
                        "game_date":   game_date,
                        "home_team":   home_team,
                        "away_team":   away_team,
                        "spread":      spread,
                        "total":       total,
                        "ml_home":     ml_home,
                        "ml_away":     ml_away,
                    })

                    print(f"Parsed: {away_team} @ {home_team} ({time_str}) on {game_date}")

                except Exception as e:
                    print(f"Skipped row {i}: {e}")
                    continue

        browser.close()
    return games

# main
def main():
    data = scrape_nfl_odds()
    if not data:
        print("No games scraped; exiting.")
        return

    try:
        with sqlite3.connect(DB_NAME) as conn:
            cur = conn.cursor()
            for g in data:
                try:
                    insert_game(cur, g["game_id"], g["start_time"], g["game_date"], g["home_team"], g["away_team"])
                    insert_odds(cur, g["game_id"], g["spread"], g["total"], g["ml_home"], g["ml_away"])
                except Exception as e:
                    print(f"Error inserting game {g.get('game_id', 'unknown')}: {e}")
                    continue
            conn.commit()
        print(f"Stored odds for {len(data)} games into `{DB_NAME}`")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
