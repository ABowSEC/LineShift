#!/usr/bin/env python3
import requests
import json
from datetime import datetime, timedelta

def test_mlb_api():
    """Test the MLB API to see what data is actually returned"""
    
    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {
        "sportId": 1,
        "hydrate": "probablePitcher",
        "startDate": "2025-08-03",
        "endDate": "2025-08-05"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        print("API Response Structure:")
        print(f"Total games: {data.get('totalGames', 0)}")
        print(f"Dates: {len(data.get('dates', []))}")
        
        # Look at first game structure
        if data.get('dates'):
            first_date = data['dates'][0]
            print(f"\nFirst date: {first_date['date']}")
            print(f"Games on this date: {len(first_date.get('games', []))}")
            
            if first_date.get('games'):
                first_game = first_date['games'][0]
                print(f"\nFirst game structure:")
                print(f"Game PK: {first_game.get('gamePk')}")
                print(f"Teams: {first_game['teams']['away']['team']['name']} @ {first_game['teams']['home']['team']['name']}")
                print(f"Game date: {first_game.get('gameDate')}")
                print(f"Official date: {first_game.get('officialDate')}")
                
                # Check for probable pitchers
                away_team = first_game['teams']['away']
                home_team = first_game['teams']['home']
                
                print(f"\nAway team keys: {list(away_team.keys())}")
                print(f"Home team keys: {list(home_team.keys())}")
                
                if 'probablePitcher' in away_team:
                    print(f"Away probable pitcher: {away_team['probablePitcher']}")
                else:
                    print("No probable pitcher data for away team")
                    
                if 'probablePitcher' in home_team:
                    print(f"Home probable pitcher: {home_team['probablePitcher']}")
                else:
                    print("No probable pitcher data for home team")
                
                # Let's see the full structure of one team
                print(f"\nFull away team structure:")
                print(json.dumps(away_team, indent=2))
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_mlb_api() 