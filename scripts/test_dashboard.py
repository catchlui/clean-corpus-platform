#!/usr/bin/env python3
"""Simple test script to verify dashboard can display."""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from clean_corpus.monitor.unified_app import create_unified_app

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", nargs="?", default="storage_example")
    parser.add_argument("--refresh", "-r", type=float, default=5.0)
    args = parser.parse_args()
    
    print(f"Testing dashboard with output_dir: {args.output_dir}")
    print("If you see this message, the script is running...")
    print("Dashboard should appear shortly...\n")
    
    try:
        create_unified_app(args.output_dir, args.refresh)
    except KeyboardInterrupt:
        print("\nDashboard stopped by user.")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
