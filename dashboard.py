import sqlite3
import pandas as pd
import streamlit as st

st.set_page_config(page_title = "LineShift Dashboard", layout = "wide")
st.title("LineShift - NFL ODDS Dashboard")

#connect to generated DBs
conn = sqlite3.connect("nfl_odds.db")

#retrieve latest odds send to dashboard
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

df = pd.read_sql_query(query, conn)

# Filter sidebar
with st.sidebar:
    st.header("Filters")                                                                                #Search ICON could be added 
    team = st.text_input("Team Name")
    if team:
        df = df[df['home_team'].str.contains(team, case=False) |
            df['away_team'].str.contains(team, case=False)]

# WAS CHANGED TO ABOVE NEEDS TEST
#team_filter = st.sidebar.text_input("Filter by Team Name:")
#if team_filter:
#    df = df[df['home_team'].str.contains(team_filter, case=False) |
#            df['away_team'].str.contains(team_filter, case=False)]

#FOrmat Display
def formatMatch(row):
    return f"{row['away_team']} @ {row['home_team']}"


df["Matchup"] = df.apply(formatMatch, axis=1)
df = df[["Matchup", "spread", "total", "moneyline_home", "moneyline_away", "last_updated"]]
df.columns = ["Matchup", "Spread", "Total (O/U)", "Moneyline (Home)", "Moneyline (Away)", "Last Updated"]


# Display table
st.dataframe(df, use_container_width=True)


#$Refresh
st.markdown("Refresh to pull Latest Odds")

conn.close()