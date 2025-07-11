#Table creations for SQLite
import sqlite3

def migrate_nfl_odds_db():
    conn = sqlite3.connect("data/nfl_odds.db")
    c = conn.cursor()

    #  Create games table
    c.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id       TEXT PRIMARY KEY,
        start_time    TEXT,
        game_date     TEXT,
        home_team     TEXT,
        away_team     TEXT
    );
    """)

    # Create odds table
    c.execute("""
    CREATE TABLE IF NOT EXISTS odds (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id        TEXT,
        provider       TEXT,
        spread_details TEXT,
        over_under     REAL,
        moneyline_home INTEGER,
        moneyline_away INTEGER,
        updated_at     TEXT,
        FOREIGN KEY(game_id) REFERENCES games(game_id)
    );
    """)

    conn.commit()
    conn.close()

def migrate_mlb_odds_db():
    conn = sqlite3.connect("data/mlb_odds.db")
    c = conn.cursor()

    # Create games table with pitcher columns
    c.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id       TEXT PRIMARY KEY,
        start_time    TEXT,
        game_date     TEXT,
        home_team     TEXT,
        away_team     TEXT,
        away_pitcher  TEXT,
        home_pitcher  TEXT
    );
    """)

    # Create odds table (same schema as NFL)
    c.execute("""
    CREATE TABLE IF NOT EXISTS odds (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id        TEXT,
        provider       TEXT,
        spread_details TEXT,
        over_under     REAL,
        moneyline_home INTEGER,
        moneyline_away INTEGER,
        updated_at     TEXT,
        FOREIGN KEY(game_id) REFERENCES games(game_id)
    );
    """)

    conn.commit()
    conn.close()

def migrate_mlb_stats_db_FG():
    conn = sqlite3.connect("data/mlb_stats.db")
    c = conn.cursor()

    # Player stats table with a composite primary key
    c.execute("""
    CREATE TABLE IF NOT EXISTS player_stats (
        player_name       TEXT,
        team              TEXT,
        games_played      INTEGER,
        plate_appearances INTEGER,
        home_runs         INTEGER,
        runs              INTEGER,
        rbi               INTEGER,
        stolen_bases      INTEGER,
        walk_rate         REAL,
        strikeout_rate    REAL,
        iso               REAL,
        babip             REAL,
        batting_avg       REAL,
        obp               REAL,
        slg               REAL,
        woba              REAL,
        xwoba             REAL,
        wrc_plus          INTEGER,
        bsr               REAL,
        off               REAL,
        def               REAL,
        war               REAL,
        last_updated      TEXT,
        PRIMARY KEY(player_name, team)
    );
    """)

    conn.commit()
    conn.close()

def migrate_mlb_stats_db_SC():
    conn = sqlite3.connect("data/mlb_stats.db")
    c = conn.cursor()

    # Drop the old table if it exists
    c.execute("DROP TABLE IF EXISTS player_stats")

    # Create new player stats table with updated schema
    c.execute("""
    CREATE TABLE IF NOT EXISTS player_stats (
        player_name         TEXT,
        year                INTEGER,
        at_bats             INTEGER,
        plate_appearances   INTEGER,
        hits                INTEGER,
        singles             INTEGER,
        doubles             INTEGER,
        home_runs           INTEGER,
        strikeouts          INTEGER,
        walks               INTEGER,
        strikeout_rate      REAL,
        walk_rate           REAL,
        batting_avg         REAL,
        slg                 REAL,
        obp                 REAL,
        iso                 REAL,
        rbi                 INTEGER,
        stolen_bases        INTEGER,
        games_played        INTEGER,
        woba                REAL,
        xwoba               REAL,
        la_sweet_spot_pct   REAL,
        barrel_pct          REAL,
        hard_hit_pct        REAL,
        ev50                REAL,
        adjusted_ev         REAL,
        whiff_pct           REAL,
        swing_pct           REAL,
        last_updated        TEXT,
        PRIMARY KEY(player_name, year)
    );
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    migrate_nfl_odds_db()
    migrate_mlb_odds_db()
    migrate_mlb_stats_db_SC()
    print("All Migrations Complete")