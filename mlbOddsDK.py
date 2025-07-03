import sqlite3
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Config
DB_NAME = "mlb_odds.db"
MLB_URL = "https://sportsbook.draftkings.com/leagues/baseball/mlb"
PROVIDER = "DraftKings-MLB-Web"

# Database helpers
def insert_game(cursor, game_id, start_time, home_team, away_team, home_pitcher, away_pitcher):
    """
    Upsert the game entry. We IGNORE if it already exists.
    """
    cursor.execute(
        """
        INSERT OR IGNORE INTO games
            (game_id, start_time, home_team, away_team, home_pitcher, away_pitcher)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (game_id, start_time, home_team, away_team, home_pitcher, away_pitcher),
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

        rows = page.query_selector_all("table.sportsbook-table tbody tr")
        print(f"Found {len(rows)} rows (should be 2 per game)")

        # Iterate in pairs: away_row, home_row
        for i in range(0, len(rows), 2):
            try:
                away = rows[i]
                home = rows[i + 1]

                away_team = away.query_selector(".event-cell__name-text").inner_text().strip()
                home_team = home.query_selector(".event-cell__name-text").inner_text().strip()

                # Over/Under (total)
                total_cell = away.query_selector_all("td")[1]
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
                    "home_team":    home_team,
                    "away_team":    away_team,
                    "home_pitcher": home_pitcher,
                    "away_pitcher": away_pitcher,
                    "total":        total,
                    "moneyline_home": moneyline_home,
                    "moneyline_away": moneyline_away,
                })

                print(f"Parsed: {away_team} @ {home_team} ({start_time})")

            except Exception as e:
                print(f"Skipping row {i}: {e}")

        browser.close()
    return results


# Main
def main():
    odds_data = scrape_mlb_odds()
    if not odds_data:
        print("No games scraped; exiting.")
        return

    # Persist into SQLite
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.cursor()
        for g in odds_data:
            insert_game(
                cur,
                g["game_id"],
                g["start_time"],
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
        conn.commit()
    print(f"Stored odds for {len(odds_data)} games into `{DB_NAME}`")

if __name__ == "__main__":
    main()
