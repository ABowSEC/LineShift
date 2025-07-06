import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

st.set_page_config(page_title="LineShift Dashboard", layout="wide")
st.title("LineShift - Odds Dashboard")

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(sport, team_filter=None, date_filter=None, date_option="All Games"):
    """Load and process data with error handling"""
    try:
        if sport == "NFL":
            db_file = "data/nfl_odds.db"
            query = """
                SELECT
                    g.game_date,
                    g.start_time,
                    g.home_team,
                    g.away_team,
                    o.spread_details AS spread,
                    o.over_under AS total,
                    CASE 
                        WHEN CAST(REPLACE(o.moneyline_home, '-', '-') AS INTEGER) > 0 THEN '+' || o.moneyline_home 
                        ELSE o.moneyline_home 
                    END AS moneyline_home,
                    CASE 
                        WHEN CAST(REPLACE(o.moneyline_away, '-', '-') AS INTEGER) > 0 THEN '+' || o.moneyline_away 
                        ELSE o.moneyline_away 
                    END AS moneyline_away,
                    MAX(o.updated_at) AS last_updated
                FROM games g
                JOIN odds o ON g.game_id = o.game_id
                GROUP BY g.game_id
                ORDER BY last_updated DESC
            """
        else:  # MLB
            db_file = "data/mlb_odds.db"
            query = """
                SELECT
                    g.game_date,
                    g.start_time,
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
        
        with sqlite3.connect(db_file) as conn:
            df = pd.read_sql_query(query, conn)
        
        if df.empty:
            st.warning(f"No {sport} data found in database")
            return pd.DataFrame()
        
        # Process data
        df = process_dataframe(df, sport, team_filter, date_filter, date_option)
        return df
        
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def nickname(full_name: str) -> str:
    """Extract team nickname from full name"""
    if pd.isna(full_name) or not full_name:
        return ""
    return full_name.strip().split()[-1]

def process_dataframe(df, sport, team_filter=None, date_filter=None, date_option="All Games"):
    """Process and filter dataframe"""
    # Convert dates // handle the DraftKings format like "THU SEP 4TH"
    def parse_game_date(date_str):
        if pd.isna(date_str) or not date_str:
            return pd.NaT
        
        try:
            parts = str(date_str).split()
            if len(parts) >= 3:
                month = parts[1]
                day = parts[2]
                month_map = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                if month in month_map:
                    month_num = month_map[month]
                    # Handle both "4TH" and "05"
                    day_num = int(''.join(filter(str.isdigit, day)))
                    # Use current year for MLB, or your NFL logic for NFL
                    year = datetime.now().year
                    return pd.Timestamp(year, month_num, day_num)
            
            # Fallback to regular datetime parsing
            return pd.to_datetime(date_str, errors="coerce")
        except:
            return pd.NaT
    
    df["game_date"] = df["game_date"].apply(parse_game_date).dt.date
    
    # Format dates for display
    df["game_date"] = df["game_date"].apply(lambda x: x.strftime("%b %d") if pd.notna(x) else "TBD")
    
    # Filter by date based on option
    if date_option == "Today":
        df = df[df["game_date"] == datetime.now().date()]
    elif date_option == "Tomorrow":
        df = df[df["game_date"] == (datetime.now() + timedelta(days=1)).date()]
    elif date_option == "This Week":
        today = datetime.now().date()
        week_end = today + timedelta(days=7)
        df = df[(df["game_date"] >= today) & (df["game_date"] <= week_end)]
    elif date_option == "Specific Date" and date_filter:
        df = df[df["game_date"] == date_filter]
    # "All Games" shows everything, so no filtering needed
    
    # Filter by team
    if team_filter:
        mask = (
            df["home_team"].str.contains(team_filter, case=False, na=False) |
            df["away_team"].str.contains(team_filter, case=False, na=False)
        )
        df = df[mask]
    
    # Create team nicknames
    df["away_nick"] = df["away_team"].apply(nickname)
    df["home_nick"] = df["home_team"].apply(nickname)
    
    # use the date for filtering purposes
    
    df["last_updated"] = (
        pd.to_datetime(df["last_updated"], errors="coerce")
          .dt.strftime("%Y-%m-%d %H:%M")
    )
    
    # Ensure numeric columns are strings for display
    numeric_cols = ["total", "moneyline_home", "moneyline_away"]
    if sport == "NFL":
        numeric_cols.append("spread")
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna("N/A").astype(str)
    
    return df

def format_display_data(df, sport):
    """Format data for display"""
    if df.empty:
        return df
    
    if sport == "MLB":
        df["Matchup"] = df.apply(
            lambda r: f"{r['away_team']} ({r.get('away_pitcher', 'TBD') or 'TBD'}) "
                      f"@ {r['home_team']} ({r.get('home_pitcher', 'TBD') or 'TBD'}) - {r.get('start_time', 'TBD')}",
            axis=1
        )
        display_df = df[
            ["game_date", "Matchup", "total", "moneyline_home", "moneyline_away", "last_updated"]
        ]
    else:  # NFL
        df["Matchup"] = df.apply(
            lambda r: f"{r['away_nick']} @ {r['home_nick']}",
            axis=1
        )
        display_df = df[
            ["game_date", "Matchup", "spread", "total", "moneyline_home", "moneyline_away", "last_updated"]
        ]
    
    # Rename columns
    display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]
    return display_df

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_mlb_stats(player_filter=None):
    """Load MLB player stats with error handling"""
    try:
        with sqlite3.connect("data/mlb_stats.db") as conn:
            stats_df = pd.read_sql_query("""
                SELECT player_name, year, at_bats, plate_appearances, hits, singles, doubles,
                       home_runs, strikeouts, walks, strikeout_rate, walk_rate, batting_avg,
                       slg, obp, iso, rbi, stolen_bases, games_played, woba, xwoba,
                       la_sweet_spot_pct, barrel_pct, hard_hit_pct, ev50, adjusted_ev, 
                       whiff_pct, swing_pct, last_updated
                FROM player_stats
                ORDER BY last_updated DESC
            """, conn)
        
        if stats_df.empty:
            return pd.DataFrame()
        
        stats_df["last_updated"] = (
            pd.to_datetime(stats_df["last_updated"], errors="coerce")
              .dt.strftime("%Y-%m-%d")
        )
        
        # Filter by player name only 
        if player_filter:
            mask = (
                stats_df["player_name"].str.contains(player_filter, case=False, na=False)
            )
            stats_df = stats_df[mask]
        
        return stats_df
        
    except sqlite3.Error as e:
        st.error(f"Stats database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading stats: {e}")
        return pd.DataFrame()

# Main App
def main():
    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        sport = st.selectbox("Select Sport", ["NFL", "MLB"])
        
        # Date filter options
        date_option = st.selectbox(
            "Date Filter", 
            ["All Games", "Today", "Tomorrow", "This Week", "Specific Date"],
            help="Choose how to filter games by date"
        )
        
        date_filter = None
        if date_option == "Today":
            date_filter = datetime.now().date()
        elif date_option == "Tomorrow":
            date_filter = (datetime.now() + timedelta(days=1)).date()
        elif date_option == "This Week":
            # We'll handle this in the filtering logic
            pass
        elif date_option == "Specific Date":
            date_filter = st.date_input(
                "Select Date", 
                value=datetime.now().date()
            )
        
        # Team filter
        team_filter = st.text_input(
            "Team Name", 
            placeholder="Enter team name to filter"
        )
        
        # Refresh button
        if st.button("Refresh Data"):
            with st.spinner("Running scrapers to get fresh data..."):
                import subprocess
                import sys
                import os
                
                try:
                    # Run the scrapers
                    result = subprocess.run(
                        [sys.executable, "run_scrapers.py"],
                        capture_output=True,
                        text=True,
                        cwd=os.getcwd(),
                        timeout=300  # 5 minute timeout
                    )
                    
                    if result.returncode == 0:
                        st.success("Scrapers completed successfully!")
                        if result.stdout:
                            st.text("Scraper output:")
                            st.code(result.stdout, language="bash")
                    else:
                        st.error("Some scrapers failed!")
                        if result.stderr:
                            st.error(f"Error: {result.stderr}")
                        if result.stdout:
                            st.text("Partial output:")
                            st.code(result.stdout, language="bash")
                            
                except subprocess.TimeoutExpired:
                    st.error("Scrapers timed out after 5 minutes")
                except Exception as e:
                    st.error(f"Error running scrapers: {e}")
            
            # Clear cache and reload
            st.cache_data.clear()
            st.rerun()
    
    # Load and display odds data
    with st.spinner("Loading odds data..."):
        df = load_data(sport, team_filter, date_filter, date_option)
    
    if not df.empty:
        display_df = format_display_data(df, sport)
        
        # Dynamic title based on date selection
        if date_option == "All Games":
            title = f"{sport} Games - All Dates"
        elif date_option == "Today":
            title = f"{sport} Games - Today"
        elif date_option == "Tomorrow":
            title = f"{sport} Games - Tomorrow"
        elif date_option == "This Week":
            title = f"{sport} Games - This Week"
        elif date_option == "Specific Date":
            title = f"{sport} Games - {date_filter}"
        
        st.subheader(title)
        st.dataframe(display_df, use_container_width=True)
        
        # Show data summary
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Games", len(display_df))
        with col2:
            if not display_df.empty and len(display_df) > 0:
                last_update = display_df["Last Updated"].iloc[0]
                st.metric("Last Updated", last_update)
            else:
                st.metric("Last Updated", "N/A")
        with col3:
            st.metric("Sport", sport)
    else:
        st.info("No games found for the selected filters")
    
    # MLB Stats section
    if sport == "MLB":
        st.subheader("MLB Player Stats (Baseball Savant)")
        
        player_filter = st.text_input(
            "Filter by Player", 
            placeholder="Enter player name"
        )
        
        with st.spinner("Loading player stats..."):
            stats_df = load_mlb_stats(player_filter)
        
        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True)
            st.caption(f"Showing {len(stats_df)} players")
        else:
            st.info("No player stats found")
    
    # Footer
    st.markdown("---")
    st.markdown("**Tip:** Use the refresh button to get the latest odds")

if __name__ == "__main__":
    main()