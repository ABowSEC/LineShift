import sqlite3
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

#Changed to baseball savant source

DB_NAME = "data/mlb_stats.db"
SAVANT_URL = (
    "https://baseballsavant.mlb.com/leaderboard/custom?year=2025&type=batter&filter=&min=q&selections=ab%2Cpa%2Chit%2Csingle%2Cdouble%2Chome_run%2Cstrikeout%2Cwalk%2Ck_percent%2Cbb_percent%2Cbatting_avg%2Cslg_percent%2Con_base_percent%2Cisolated_power%2Cb_rbi%2Cr_total_stolen_base%2Cb_game%2Cwoba%2Cxwoba%2Csweet_spot_percent%2Cbarrel_batted_rate%2Chard_hit_percent%2Cavg_best_speed%2Cavg_hyper_speed%2Cwhiff_percent%2Cswing_percent&chart=false&x=ab&y=ab&r=no&chartType=beeswarm&sort=xwoba&sortDir=desc"
)

COLUMNS = [
    "player_name", "year", "at_bats", "plate_appearances", "hits", "singles", "doubles",
    "home_runs", "strikeouts", "walks", "strikeout_rate", "walk_rate", "batting_avg",
    "slg", "obp", "iso", "rbi", "stolen_bases", "games_played", "woba", "xwoba",
    "la_sweet_spot_pct", "barrel_pct", "hard_hit_pct", "ev50", "adjusted_ev", 
    "whiff_pct", "swing_pct", "last_updated"
]
#Init DB in migrations now

def parse_savant_table(html):
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try multiple table selectors for Baseball Savant
    table_selectors = [
        "#sortable_stats table",
        "div.table-savant table",
        "table.table-savant",
        "table",
        "tbody"
    ]
    
    table = None
    for selector in table_selectors:
        table = soup.select_one(selector)
        if table:
            print(f"Found table with selector: {selector}")
            break
    
    if not table:
        print("Stats table not found with any selector.")
        print("Available tables on page:")
        tables = soup.find_all("table")
        for i, t in enumerate(tables):
            print(f"  Table {i}: {t.get('class', 'no-class')} - {t.get('id', 'no-id')}")
        return []

    data = []
    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        
        # Skip rows that don't have enough data cells (should have at least 28 data cells)
        if len(cells) < 28:
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
            # Extract player name from the anchor tag
            player_cell = cells[1]
            player_link = player_cell.find('a')
            player_name = player_link.text.strip() if player_link else cells[1].text.strip()
            
            data.append((
                player_name,                              # player_name
                parse_int(cells[2].text.strip()),        # year
                parse_int(cells[3].text.strip()),        # AB
                parse_int(cells[4].text.strip()),        # PA
                parse_int(cells[5].text.strip()),        # H
                parse_int(cells[6].text.strip()),        # 1B
                parse_int(cells[7].text.strip()),        # 2B
                parse_int(cells[8].text.strip()),        # HR
                parse_int(cells[9].text.strip()),        # SO
                parse_int(cells[10].text.strip()),       # BB
                parse_percent(cells[11].text.strip()),   # K%
                parse_percent(cells[12].text.strip()),   # BB%
                parse_float(cells[13].text.strip()),     # AVG
                parse_float(cells[14].text.strip()),     # SLG
                parse_float(cells[15].text.strip()),     # OBP
                parse_float(cells[16].text.strip()),     # ISO
                parse_int(cells[17].text.strip()),       # RBI
                parse_int(cells[18].text.strip()),       # SB
                parse_int(cells[19].text.strip()),       # G
                parse_float(cells[20].text.strip()),     # wOBA
                parse_float(cells[21].text.strip()),     # xwOBA
                parse_percent(cells[22].text.strip()),   # LA Sweet-Spot %
                parse_percent(cells[23].text.strip()),   # Barrel%
                parse_percent(cells[24].text.strip()),   # Hard Hit %
                parse_float(cells[25].text.strip()),     # EV50
                parse_float(cells[26].text.strip()),     # Adjusted EV
                parse_percent(cells[27].text.strip()),   # Whiff %
                parse_percent(cells[28].text.strip()),   # Swing %
                datetime.now(timezone.utc).isoformat()   # last_updated
            ))
        except Exception as e:
            print("Failed to parse row:", e)

    return data

def fetch_and_parse_table():
    print("Fetching Baseball Savant table...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(SAVANT_URL, timeout=90000)
            print("Page loaded successfully")
            
            # Try multiple selectors with longer waits
            selectors_to_try = [
                "#sortable_stats table",
                "div.table-savant table",
                "table.table-savant",
                "table",
                "tbody"
            ]
            
            table_found = False
            for selector in selectors_to_try:
                try:
                    print(f"Trying selector: {selector}")
                    page.wait_for_selector(selector, timeout=10000)
                    print(f"Found table with selector: {selector}")
                    table_found = True
                    break
                except Exception as e:
                    print(f"Selector '{selector}' failed: {e}")
                    continue
            
            if not table_found:
                print("No table found with any selector. Taking screenshot for debugging...")
                page.screenshot(path="savant_debug.png")
                print("Screenshot saved as savant_debug.png")
                
                # Try to get the page content anyway
                html = page.content()
                print("Got page content, attempting to parse...")
            else:
                # Try to get the table HTML
                try:
                    html = page.inner_html("#sortable_stats")
                except:
                    try:
                        html = page.inner_html("div.table-savant")
                    except:
                        try:
                            html = page.inner_html("table")
                        except:
                            html = page.content()
                            print("Falling back to full page content")
            
        except Exception as e:
            print(f"Error during page load: {e}")
            return []
        finally:
            browser.close()

    return parse_savant_table(html)

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
            ON CONFLICT(player_name, year) DO UPDATE SET
                at_bats             = excluded.at_bats,
                plate_appearances   = excluded.plate_appearances,
                hits                = excluded.hits,
                singles             = excluded.singles,
                doubles             = excluded.doubles,
                home_runs           = excluded.home_runs,
                strikeouts          = excluded.strikeouts,
                walks               = excluded.walks,
                strikeout_rate      = excluded.strikeout_rate,
                walk_rate           = excluded.walk_rate,
                batting_avg         = excluded.batting_avg,
                slg                 = excluded.slg,
                obp                 = excluded.obp,
                iso                 = excluded.iso,
                rbi                 = excluded.rbi,
                stolen_bases        = excluded.stolen_bases,
                games_played        = excluded.games_played,
                woba                = excluded.woba,
                xwoba               = excluded.xwoba,
                la_sweet_spot_pct   = excluded.la_sweet_spot_pct,
                barrel_pct          = excluded.barrel_pct,
                hard_hit_pct        = excluded.hard_hit_pct,
                ev50                = excluded.ev50,
                adjusted_ev         = excluded.adjusted_ev,
                whiff_pct           = excluded.whiff_pct,
                swing_pct           = excluded.swing_pct,
                last_updated        = excluded.last_updated;
        """, row)

    conn.commit()
    conn.close()
    print(f"Stored {len(stats)} player records.")

def main():
    stats = fetch_and_parse_table()
    store_stats(stats)

if __name__ == "__main__":
    main()
