import sqlite3
from datetime import datetime
from playwright.sync_api import sync_playwright

conn = sqlite3.connect("mlb_odds.db")
cursor = conn.cursor()

MLB_URL = "https://sportsbook.draftkings.com/leagues/baseball/mlb"
PROVIDER = "DraftKings-MLB-Web"

# Ensure pitcher columns exist in games table
cursor.execute("""
    ALTER TABLE games ADD COLUMN away_pitcher TEXT
""")
cursor.execute("""
    ALTER TABLE games ADD COLUMN home_pitcher TEXT
""")


def insert_mlb_odds(home_team, away_team, start_time, total, moneyline_home, moneyline_away, away_pitcher, home_pitcher):
    game_id = f"{away_team}@{home_team} {start_time}"

    # Insert game entry if it doesn't exist
    cursor.execute('''
        INSERT OR IGNORE INTO games (game_id, start_time, home_team, away_team, away_pitcher, home_pitcher)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (game_id, start_time, home_team, away_team, away_pitcher, home_pitcher))

    # Insert odds entry
    cursor.execute('''
        INSERT INTO odds (game_id, provider, spread_details, over_under, moneyline_home, moneyline_away, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        game_id,
        PROVIDER,
        None,  # No spread for now
        total,
        moneyline_home,
        moneyline_away,
        datetime.utcnow().strftime("%Y-%m-%d")
    ))

    print(f" Stored MLB odds for {away_team} @ {home_team}")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto(MLB_URL, timeout=60000)
    page.wait_for_selector("table.sportsbook-table", timeout=30000)

    rows = page.query_selector_all("table.sportsbook-table tbody tr")
    print("Found game rows:", len(rows))

    for i in range(0, len(rows), 2):
        try:
            away_row = rows[i]
            home_row = rows[i + 1]

            away_team = away_row.query_selector(".event-cell__name-text").inner_text().strip()
            home_team = home_row.query_selector(".event-cell__name-text").inner_text().strip()

            # Total
            try:
                total_cell = away_row.query_selector_all("td")[1]
                total_elem = total_cell.query_selector('[data-testid="sportsbook-outcome-cell-line"]')
                total = total_elem.inner_text().strip() if total_elem else None
            except:
                total = None

            # Moneylines
            away_odds = away_row.query_selector_all('[data-testid="sportsbook-odds"]')
            home_odds = home_row.query_selector_all('[data-testid="sportsbook-odds"]')
            moneyline_away = away_odds[-1].inner_text().strip() if away_odds else None
            moneyline_home = home_odds[-1].inner_text().strip() if home_odds else None

            # Start Time
            start_elem = away_row.query_selector(".event-cell__start-time")
            start_time = start_elem.inner_text().strip() if start_elem else "TBD"

            # Pitchers
            away_pitcher_elem = away_row.query_selector(".event-cell__pitcher")
            home_pitcher_elem = home_row.query_selector(".event-cell__pitcher")
            away_pitcher = away_pitcher_elem.inner_text().strip() if away_pitcher_elem else None
            home_pitcher = home_pitcher_elem.inner_text().strip() if home_pitcher_elem else None

            insert_mlb_odds(
                home_team=home_team,
                away_team=away_team,
                start_time=start_time,
                total=total,
                moneyline_home=moneyline_home,
                moneyline_away=moneyline_away,
                away_pitcher=away_pitcher,
                home_pitcher=home_pitcher
            )
        except Exception as e:
            print(" Error scraping MLB game row:", e)

    conn.commit()
    conn.close()
    browser.close()
