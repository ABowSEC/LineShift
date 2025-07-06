#!/usr/bin/env python3
"""
Convenience script to run the dashboard from the root directory.
"""

import subprocess
import sys
import os

def main():
    """Run the dashboard"""
    print("Starting LineShift Dashboard...")
    print("Dashboard will be available at: http://localhost:8501")
    print("Press Ctrl+C to stop the dashboard")
    
    try:
        # Run the dashboard
        subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard/dashboard.py"], 
                      cwd=os.getcwd())
    except KeyboardInterrupt:
        print("\nDashboard stopped by user")
    except Exception as e:
        print(f"Error running dashboard: {e}")

if __name__ == "__main__":
    main() 