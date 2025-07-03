import sqlite3
import pandas as pd
import streamlit as st

st.set_page_config(page_title="LineShift Dashboard", layout="wide")
st.title("LineShift - Odds Dashboard")

#  Sidebar filters 
with st.sidebar:
    st.header("Filters")
    sport       = st.selectbox("Select Sport", ["NFL", "MLB"])
    #date_filter = st.date_input("Game Date", value=pd.Timestamp.today().date())
    team_filter = st.text_input("Team Name")

#  Load data 
if sport == "NFL":
    db_file = "nfl_odds.db"
    query = """
        SELECT
            g.game_date,
            g.home_team,
            g.away_team,
            o.spread_details AS spread,
            o.over_under     AS total,
            o.moneyline_home,
            o.moneyline_away,
            MAX(o.updated_at) AS last_updated
        FROM games g
        JOIN odds o ON g.game_id = o.game_id
        GROUP BY g.game_id
        ORDER BY last_updated DESC
    """
    show_pitchers = False

else:  # MLB
    db_file = "mlb_odds.db"
    query = """
        SELECT
            g.game_date,
            g.home_team,
            g.away_team,
            g.home_pitcher,
            g.away_pitcher,
            o.over_under     AS total,
            o.moneyline_home,
            o.moneyline_away,
            MAX(o.updated_at) AS last_updated
        FROM games g
        JOIN odds o ON g.game_id = o.game_id
        GROUP BY g.game_id
        ORDER BY last_updated DESC
    """
    show_pitchers = True

# read into DataFrame
conn = sqlite3.connect(db_file)
df = pd.read_sql_query(query, conn)
conn.close()

#  Apply filters 
# Parse game_date from text → date
#df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

# Filter by game date
#df = df[df["game_date"] == date_filter]

# Filter by team name
if team_filter:
    mask = (
        df["home_team"].str.contains(team_filter, case=False) |
        df["away_team"].str.contains(team_filter, case=False)
    )
    df = df[mask]

#  Clean up timestamps 
df["last_updated"] = (
    pd.to_datetime(df["last_updated"], errors="coerce")
      .dt.strftime("%Y-%m-%d")
)

# Ensure numeric columns are strings for Streamlit
for col in ["total", "moneyline_home", "moneyline_away"]:
    df[col] = df[col].astype(str)

# display table
if show_pitchers:
    df["Matchup"] = df.apply(
        lambda r: f"{r['away_team']} ({r['away_pitcher'] or 'TBD'}) "
                  f"@ {r['home_team']} ({r['home_pitcher'] or 'TBD'})",
        axis=1
    )
    display_cols = [
        "Game Date", "Matchup", "Total", "Moneyline Home",
        "Moneyline Away", "Last Updated"
    ]
    df = df[
        ["game_date", "Matchup", "total", "moneyline_home", "moneyline_away", "last_updated"]
    ]
else:
    df["Matchup"] = df.apply(
        lambda r: f"{r['away_team']} @ {r['home_team']}",
        axis=1
    )
    df = df[
        ["game_date", "Matchup", "spread", "total", "moneyline_home", "moneyline_away", "last_updated"]
    ]

# Rename columns to title case
df.columns = [c.replace("_", " ").title() for c in df.columns]

#Render
st.dataframe(df, use_container_width=True)
st.markdown("Refresh the app or change filters to update odds.")

# MLB Stats 
if sport == "MLB":
    st.subheader("MLB Player Stats (FanGraphs)")
    try:
        conn_stats = sqlite3.connect("mlb_stats.db")
        stats_df = pd.read_sql_query("""
            SELECT player_name, team, games_played, plate_appearances,
                   home_runs, runs, rbi, stolen_bases, walk_rate,
                   strikeout_rate, iso, babip, batting_avg, obp, slg,
                   woba, xwoba, wrc_plus, bsr, off, def, war, last_updated
            FROM player_stats
            ORDER BY last_updated DESC
        """, conn_stats)
        conn_stats.close()

        stats_df["last_updated"] = (
            pd.to_datetime(stats_df["last_updated"], errors="coerce")
              .dt.strftime("%Y-%m-%d")
        )

        player_filter = st.text_input("Filter by Player or Team", "")
        if player_filter:
            mask = (
                stats_df["player_name"].str.contains(player_filter, case=False) |
                stats_df["team"].str.contains(player_filter, case=False)
            )
            stats_df = stats_df[mask]

        st.dataframe(stats_df, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load player stats: {e}")
