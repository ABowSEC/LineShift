import sqlite3
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

DB_NAME = "mlb_stats.db"
FANGRAPHS_URL = (
    "https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&type=8"
    "&season=2024&month=0&season1=2024&ind=0&team=0&rost=0&age=0&filter=&players=0"
)

COLUMNS = [
    "player_name", "team", "games_played", "plate_appearances", "home_runs",
    "runs", "rbi", "stolen_bases", "walk_rate", "strikeout_rate", "iso", "babip",
    "batting_avg", "obp", "slg", "woba", "xwoba", "wrc_plus", "bsr", "off", "def", "war", "last_updated"
]
#Init DB in migrations now

def parse_fangraphs_table(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.select_one("div.leaders-major_leaders-major__table__hcmbm table")
    if not table:
        print("❌ Stats table not found.")
        return []

    data = []
    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 23:
            continue

        def parse_percent(text):
            try:
                return float(text.strip().replace('%', '')) / 100
            except:
                return None

        def parse_float(text):
            try:
                return float(text.strip())
            except:
                return None

        def parse_int(text):
            try:
                return int(text.strip())
            except:
                return None

        try:
            data.append((
                cells[1].text.strip(),                    # player_name
                cells[2].text.strip(),                    # team
                parse_int(cells[3].text),                 # G
                parse_int(cells[4].text),                 # PA
                parse_int(cells[5].text),                 # HR
                parse_int(cells[6].text),                 # R
                parse_int(cells[7].text),                 # RBI
                parse_int(cells[8].text),                 # SB
                parse_percent(cells[9].text),             # BB%
                parse_percent(cells[10].text),            # K%
                parse_float(cells[11].text),              # ISO
                parse_float(cells[12].text),              # BABIP
                parse_float(cells[13].text),              # AVG
                parse_float(cells[14].text),              # OBP
                parse_float(cells[15].text),              # SLG
                parse_float(cells[16].text),              # wOBA
                parse_float(cells[17].text),              # xwOBA
                parse_int(cells[18].text),                # wRC+
                parse_float(cells[19].text),              # BsR
                parse_float(cells[20].text),              # Off
                parse_float(cells[21].text),              # Def
                parse_float(cells[22].text),              # WAR
                datetime.now(timezone.utc).isoformat()    # last_updated
            ))
        except Exception as e:
            print("Failed to parse row:", e)

    return data

def fetch_and_parse_table():
    print("Fetching FanGraphs table...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(FANGRAPHS_URL, timeout=90000)
        page.wait_for_selector("div.leaders-major_leaders-major__table__hcmbm table", timeout=60000)
        html = page.inner_html("div.leaders-major_leaders-major__table__hcmbm")
        browser.close()

    return parse_fangraphs_table(html)

def store_stats(stats):
    if not stats:
        print("No data to store.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for row in stats:
        c.execute(f"""
            INSERT INTO player_stats ({', '.join(COLUMNS)})
            VALUES ({', '.join(['?'] * len(COLUMNS))})
            ON CONFLICT(player_name, team) DO UPDATE SET
                games_played      = excluded.games_played,
                plate_appearances = excluded.plate_appearances,
                home_runs         = excluded.home_runs,
                runs              = excluded.runs,
                rbi               = excluded.rbi,
                stolen_bases      = excluded.stolen_bases,
                walk_rate         = excluded.walk_rate,
                strikeout_rate    = excluded.strikeout_rate,
                iso               = excluded.iso,
                babip             = excluded.babip,
                batting_avg       = excluded.batting_avg,
                obp               = excluded.obp,
                slg               = excluded.slg,
                woba              = excluded.woba,
                xwoba             = excluded.xwoba,
                wrc_plus          = excluded.wrc_plus,
                bsr               = excluded.bsr,
                off               = excluded.off,
                def               = excluded.def,
                war               = excluded.war,
                last_updated      = excluded.last_updated;
        """, row)

    conn.commit()
    conn.close()
    print(f"Stored {len(stats)} player records.")

def main():
    stats = fetch_and_parse_table()
    store_stats(stats)

if __name__ == "__main__":
    main()
