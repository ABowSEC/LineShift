import sqlite3 
import pandas as pd
import streamlit as st

st.set_page_config(page_title="LineShift Dashboard", layout="wide")
st.title("LineShift - Odds Dashboard")

# Sidebar: Select Sport
with st.sidebar:
    st.header("Filters")
    sport = st.selectbox("Select Sport", ["NFL", "MLB"])
    team_filter = st.text_input("Team Name")

# Load data based on sport
if sport == "NFL":
    conn = sqlite3.connect("nfl_odds.db")
    query = """
        SELECT
            g.home_team,
            g.away_team,
            o.spread_details AS spread,
            o.over_under AS total,
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
    conn = sqlite3.connect("mlb_odds.db")
    query = """
        SELECT
            g.home_team,
            g.away_team,
            g.home_pitcher,
            g.away_pitcher,
            o.over_under AS total,
            o.moneyline_home,
            o.moneyline_away,
            MAX(o.updated_at) AS last_updated
        FROM games g
        JOIN odds o ON g.game_id = o.game_id
        GROUP BY g.game_id
        ORDER BY last_updated DESC
    """
    show_pitchers = True

# Load odds into DataFrame
df = pd.read_sql_query(query, conn)
conn.close()

# Filter teams
if team_filter:
    df = df[df["home_team"].str.contains(team_filter, case=False) |
            df["away_team"].str.contains(team_filter, case=False)]

# Clean timestamp
df["last_updated"] = pd.to_datetime(
    df["last_updated"], 
    errors="coerce"
)
df["last_updated"] = df["last_updated"].dt.strftime("%Y-%m-%d")

# Ensure string type for Arrow serialization
for col in ["total", "moneyline_home", "moneyline_away"]:
    df[col] = df[col].astype(str)

# Format matchup column
if show_pitchers:
    df["Matchup"] = df.apply(
        lambda row: f"{row['away_team']} ({row['away_pitcher'] or 'TBD'}) @ {row['home_team']} ({row['home_pitcher'] or 'TBD'})", axis=1)
    df = df[["Matchup", "total", "moneyline_home", "moneyline_away", "last_updated"]]
else:
    df["Matchup"] = df.apply(
        lambda row: f"{row['away_team']} @ {row['home_team']}", axis=1)
    df = df[["Matchup", "spread", "total", "moneyline_home", "moneyline_away", "last_updated"]]

# Rename columns
df.columns = [col.replace("_", " ").title() for col in df.columns]

# Display odds
st.dataframe(df, use_container_width=True)
st.markdown("Refresh the app to update odds.")

# --------------------------------------------
# Updated: MLB Player Stats Viewer
# --------------------------------------------
if sport == "MLB":
    st.subheader("MLB Player Stats (FanGraphs)")

    try:
        conn_stats = sqlite3.connect("mlb_stats.db")
        stats_df = pd.read_sql_query("""
            SELECT player_name, team, games_played, plate_appearances, home_runs,
                   runs, rbi, stolen_bases, walk_rate, strikeout_rate,
                   iso, babip, batting_avg, obp, slg, woba, xwoba, wrc_plus,
                   bsr, off, def, war, last_updated
            FROM player_stats
            ORDER BY last_updated DESC
        """, conn_stats)
        conn_stats.close()

        # Clean date
        stats_df["last_updated"] = pd.to_datetime(stats_df["last_updated"], errors="coerce").dt.strftime("%Y-%m-%d")

        # Optional player/team filter
        player_filter = st.text_input("Filter by Player or Team", "")
        if player_filter:
            stats_df = stats_df[
                stats_df["player_name"].str.contains(player_filter, case=False) |
                stats_df["team"].str.contains(player_filter, case=False)
            ]

        st.dataframe(stats_df, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load player stats: {e}")
