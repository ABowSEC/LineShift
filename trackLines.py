import sqlite3

def changed(new, old):
    return new != old and new is not None and old is not None

file = sqlite3.connect("nfl_odds.db")
c = file.cursor()

# Get game IDs with multiple odds entries
c.execute("""
    SELECT game_id FROM odds
    GROUP BY game_id
    HAVING COUNT(*) >= 2
""")

gameIDs = [row[0] for row in c.fetchall()]

for game_id in gameIDs:
    c.execute("SELECT home_team, away_team FROM games WHERE game_id = ?", (game_id,))
    result = c.fetchone()
    if not result:
        continue
    home_team, away_team = result

    c.execute("""
        SELECT spread_details, over_under, moneyline_home, moneyline_away, updated_at
        FROM odds
        WHERE game_id = ?
        ORDER BY updated_at DESC
        LIMIT 2
    """, (game_id,))
    rows = c.fetchall()

    if len(rows) < 2:
        continue

    latest, previous = rows[0], rows[1]

    if any([
        changed(latest[0], previous[0]),  # spread
        changed(latest[1], previous[1]),  # total
        changed(latest[2], previous[2]),  # ML home
        changed(latest[3], previous[3])   # ML away
    ]):
        print(f"\n📊 Line Movement Detected for {away_team} @ {home_team}")
        fields = [
            ("Spread", latest[0], previous[0]),
            ("Total (O/U)", latest[1], previous[1]),
            ("Moneyline Home", latest[2], previous[2]),
            ("Moneyline Away", latest[3], previous[3])
        ]
        for label, new_val, old_val in fields:
            if changed(new_val, old_val):
                print(f"  🔄 {label}: {old_val} → {new_val}")

file.close()
