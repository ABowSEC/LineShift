import sqlite3
from datetime import datetime, timezone, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Config
DB_NAME = "data/mlb_odds.db"
MLB_URL = "https://sportsbook.draftkings.com/leagues/baseball/mlb"
PROVIDER = "DraftKings-MLB-Web"

# Database helpers
def insert_game(cursor, game_id, start_time, game_date, home_team, away_team, home_pitcher, away_pitcher):
    """
    Upsert the game entry. We REPLACE if it already exists.
    """
    cursor.execute(
        """
        INSERT OR REPLACE INTO games
            (game_id, start_time, game_date, home_team, away_team, home_pitcher, away_pitcher)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (game_id, start_time, game_date, home_team, away_team, home_pitcher, away_pitcher),
    )

def insert_odds(cursor, game_id, total, moneyline_home, moneyline_away):
    """
    Insert a new odds row. We timestamp with UTC (no microseconds).
    """
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    cursor.execute(
        """
        INSERT INTO odds
            (game_id, provider, spread_details, over_under, moneyline_home, moneyline_away, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (game_id, PROVIDER, None, total, moneyline_home, moneyline_away, timestamp),
    )

# Scraping logic
def scrape_mlb_odds():
    """
    Launch a headless browser, scrape the DraftKings MLB odds table,
    and return a list of dicts with all the fields we need.
    """
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(MLB_URL, timeout=60000)
            page.wait_for_selector("table.sportsbook-table tbody tr", timeout=30000)
        except PlaywrightTimeoutError as e:
            print(f"Page load failed: {e}")
            browser.close()
            return results

        # Find all tables and process each one
        tables = page.query_selector_all("table.sportsbook-table")
        print(f"Found {len(tables)} tables")
        
        for table_idx, table in enumerate(tables):
            # Extract date from this specific table's header
            try:
                date_elem = table.query_selector(".sportsbook-table-header__title span span")
                if date_elem:
                    raw_date = date_elem.inner_text().strip()
                    print(f"Raw date for table {table_idx + 1}: {raw_date}")
                    
                    # Convert relative dates to actual dates
                    if raw_date == "TODAY":
                        game_date = datetime.now().strftime("%a %b %d").upper()
                    elif raw_date == "TOMORROW":
                        tomorrow = datetime.now() + timedelta(days=1)
                        game_date = tomorrow.strftime("%a %b %d").upper()
                    else:
                        game_date = raw_date
                    
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

            # Iterate in pairs: away_row, home_row
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

                    # Over/Under (total)
                    total_cell = away.query_selector_all("td")[1] if len(away.query_selector_all("td")) > 1 else None
                    total = None
                    if total_cell:
                        total_elem = total_cell.query_selector('[data-testid="sportsbook-outcome-cell-line"]')
                        total = total_elem.inner_text().strip() if total_elem else None

                    # Moneylines
                    away_ml = away.query_selector_all('[data-testid="sportsbook-odds"]')
                    home_ml = home.query_selector_all('[data-testid="sportsbook-odds"]')
                    moneyline_away = away_ml[-1].inner_text().strip() if away_ml else None
                    moneyline_home = home_ml[-1].inner_text().strip() if home_ml else None

                    # Start time
                    start_elem = away.query_selector(".event-cell__start-time")
                    start_time = start_elem.inner_text().strip() if start_elem else "TBD"

                    # Pitchers
                    away_pitcher_elem = away.query_selector(".event-cell__pitcher")
                    home_pitcher_elem = home.query_selector(".event-cell__pitcher")
                    away_pitcher = away_pitcher_elem.inner_text().strip() if away_pitcher_elem else None
                    home_pitcher = home_pitcher_elem.inner_text().strip() if home_pitcher_elem else None

                    game_id = f"{away_team}@{home_team} {start_time}"

                    results.append({
                        "game_id":      game_id,
                        "start_time":   start_time,
                        "game_date":    game_date,
                        "home_team":    home_team,
                        "away_team":    away_team,
                        "home_pitcher": home_pitcher,
                        "away_pitcher": away_pitcher,
                        "total":        total,
                        "moneyline_home": moneyline_home,
                        "moneyline_away": moneyline_away,
                    })

                    print(f"Parsed: {away_team} @ {home_team} ({start_time}) on {game_date}")

                except Exception as e:
                    print(f"Skipping row {i}: {e}")
                    continue

        browser.close()
    return results


# Main
def main():
    odds_data = scrape_mlb_odds()
    if not odds_data:
        print("No games scraped; exiting.")
        return

    # Persist into SQLite
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cur = conn.cursor()
            for g in odds_data:
                try:
                    insert_game(
                        cur,
                        g["game_id"],
                        g["start_time"],
                        g["game_date"],
                        g["home_team"],
                        g["away_team"],
                        g["home_pitcher"],
                        g["away_pitcher"],
                    )
                    insert_odds(
                        cur,
                        g["game_id"],
                        g["total"],
                        g["moneyline_home"],
                        g["moneyline_away"],
                    )
                except Exception as e:
                    print(f"Error inserting game {g.get('game_id', 'unknown')}: {e}")
                    continue
            conn.commit()
        print(f"Stored odds for {len(odds_data)} games into `{DB_NAME}`")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
