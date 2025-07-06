import sqlite3
import pandas as pd

DB_NAME = "mlb_stats.db"

def fetch_and_display_player_stats():
    conn = sqlite3.connect(DB_NAME)
    query = """
        SELECT player_name, team, games_played, plate_appearances, home_runs,
               runs, rbi, stolen_bases, walk_rate, strikeout_rate,
               batting_avg, obp, slg, war, last_updated
        FROM player_stats
        ORDER BY games_played DESC
        LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    print("\n=== TOP 10 MLB PLAYER STATS ===")
    print(df.to_string(index=False))

if __name__ == "__main__":
    fetch_and_display_player_stats()
