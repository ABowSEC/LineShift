import sqlite3
#IMPLEMENT MORE SOURCES FOR COMPARISION
file = sqlite3.connect("nfl_odds.db")
c= file.cursor

#Check for ones with muliple odds to compare change 2+entries
c.execute("""
    SELECT game_id FROM odds
    GROUP by game_id
    HAVING COUNT(*)>=1
""")

gameIDs = [row[0] for row in c.fetchall()]

def changed(new, old):
    return new != old and new is not None and old is not None

for game_id in gameIDs:

    c.execute("SELECT home_team, away_team FROM games WHERE game_id = ?",(game_id))
    
    result = c.fetchone()

    if not result:
        continue
    
    home_team, away_team = result


    # Get the last two odds entries for this game
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
        print(f"\n Line Movement from Game ID {game_id}:")                                                     
        fields = [
    ("Spread", latest[0], previous[0]),
    ("Total (O/U)", latest[1], previous[1]),
    ("Moneyline Home Team", latest[2], previous[2]),
    ("Moneyline Away Team", latest[3], previous[3])
]

for label, new_val, old_val in fields:
    if changed(new_val, old_val):
        print(f"  {label}: {old_val} → {new_val}")

file.close()