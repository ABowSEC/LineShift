import sqlite3
from datetime import datetime
from playwright.sync_api import sync_playwright

conn = sqlite3.connect("nfl_odds.db")
cursor = conn.cursor()

NFL_URL = "https://sportsbook.draftkings.com/leagues/football/nfl"
PROVIDER = "DraftKings-Web"

def insert_dk_odds(home_team, away_team, start_time, spread, total, moneyline_home, moneyline_away):
    game_id = f"{away_team}@{home_team} {start_time}"

    # Insert game entry if it doesn't exist
    cursor.execute('''
        INSERT OR IGNORE INTO games (game_id, start_time, home_team, away_team)
        VALUES (?, ?, ?, ?)
    ''', (game_id, start_time, home_team, away_team))

    # Insert odds entry
    cursor.execute('''
        INSERT INTO odds (game_id, provider, spread_details, over_under, moneyline_home, moneyline_away, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        game_id,
        PROVIDER,
        spread,
        total,
        moneyline_home,
        moneyline_away,
        datetime.utcnow().isoformat()
    ))

    print(f"✔️ Stored odds for {away_team} @ {home_team}")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto(NFL_URL, timeout=60000)
    page.wait_for_selector("table.sportsbook-table", timeout=30000)

    rows = page.query_selector_all("table.sportsbook-table tbody tr")
    print("Found game rows:", len(rows))

    for i in range(0, len(rows), 2):  # Two rows per game
        try:
            away_row = rows[i]
            home_row = rows[i + 1]

            # Teams
            away_team = away_row.query_selector(".event-cell__name-text").inner_text().strip()
            home_team = home_row.query_selector(".event-cell__name-text").inner_text().strip()

            # Spreads
            spread_elems = [e.inner_text().strip() for e in away_row.query_selector_all('[data-testid="sportsbook-outcome-cell-line"]')]
            spread = " | ".join(spread_elems) if spread_elems else None

            # Total
            try:
                total_cell = away_row.query_selector_all("td")[1]
                total_elem = total_cell.query_selector('[data-testid="sportsbook-outcome-cell-line"]')
                total = total_elem.inner_text().strip() if total_elem else None
            except:
                total = None

            # Moneylines
            away_moneyline_elem = away_row.query_selector_all('[data-testid="sportsbook-odds"]')
            home_moneyline_elem = home_row.query_selector_all('[data-testid="sportsbook-odds"]')
            moneyline_away = away_moneyline_elem[-1].inner_text().strip() if away_moneyline_elem else None
            moneyline_home = home_moneyline_elem[-1].inner_text().strip() if home_moneyline_elem else None

            # Start Time
            start_elem = away_row.query_selector(".event-cell__start-time")
            start_time = start_elem.inner_text().strip() if start_elem else "TBD"

            # Insert into DB
            insert_dk_odds(
                home_team=home_team,
                away_team=away_team,
                start_time=start_time,
                spread=spread,
                total=total,
                moneyline_home=moneyline_home,
                moneyline_away=moneyline_away
            )

        except Exception as e:
            print("Error scraping game row:", e)

    conn.commit()
    conn.close()
    browser.close()
