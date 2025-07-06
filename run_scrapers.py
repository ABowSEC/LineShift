#!/usr/bin/env python3
"""
Convenience script to run all scrapers from the root directory.
This handles the new directory structure.
"""

import subprocess
import sys
import os

def run_scraper(script_path):
    """Run a scraper script and handle any errors"""
    try:
        print(f"\n{'='*50}")
        print(f"Running: {script_path}")
        print(f"{'='*50}")
        
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, cwd=os.getcwd())
        
        if result.returncode == 0:
            print("Success!")
            if result.stdout:
                print(result.stdout)
        else:
            print("Failed!")
            if result.stderr:
                print(result.stderr)
            if result.stdout:
                print(result.stdout)
                
    except Exception as e:
        print(f"Error running {script_path}: {e}")

def main():
    """Run all scrapers"""
    print("Starting LineShift scrapers...")
    
    # MLB scrapers
    run_scraper("scrapers/mlb/mlbScheduleAPI.py")
    run_scraper("scrapers/mlb/mlbOddsDK.py")
    run_scraper("scrapers/mlb/mlbStatScraper.py")
    
    # NFL scrapers
    run_scraper("scrapers/nfl/fetchOddsDK.py")
    run_scraper("scrapers/nfl/fetchOddsESPN.py")
    
    print("\n All scrapers completed!")

if __name__ == "__main__":
    main() 