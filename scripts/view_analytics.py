"""View analytics data from a clean-corpus run.

Usage:
    python scripts/view_analytics.py storage_example
"""

from __future__ import annotations
import sys
import os
import pyarrow.parquet as pq
import pandas as pd

def view_analytics(out_dir: str):
    """Display analytics summary from a run."""
    print(f"\n{'='*60}")
    print(f"Analytics Summary: {out_dir}")
    print(f"{'='*60}\n")
    
    # Check aggregates
    agg_path = os.path.join(out_dir, "analytics", "aggregates", "daily_aggregates.parquet")
    if os.path.exists(agg_path):
        print("📊 Daily Aggregates:")
        print("-" * 60)
        try:
            df = pd.read_parquet(agg_path)
            print(df.to_string(index=False))
            print(f"\nTotal rows: {len(df)}")
        except Exception as e:
            print(f"Error reading aggregates: {e}")
    else:
        print("⚠️  No aggregates file found")
    
    # Check events
    events_dir = os.path.join(out_dir, "analytics", "events")
    if os.path.exists(events_dir):
        print(f"\n📈 Events by Stage:")
        print("-" * 60)
        for stage_dir in os.listdir(events_dir):
            stage_path = os.path.join(events_dir, stage_dir)
            if os.path.isdir(stage_path):
                stage_name = stage_dir.replace("stage=", "")
                date_dirs = [d for d in os.listdir(stage_path) if os.path.isdir(os.path.join(stage_path, d))]
                for date_dir in date_dirs:
                    date_path = os.path.join(stage_path, date_dir)
                    event_file = os.path.join(date_path, "events.parquet")
                    if os.path.exists(event_file):
                        try:
                            df = pd.read_parquet(event_file)
                            print(f"\n  Stage: {stage_name} | Date: {date_dir.replace('date=', '')}")
                            print(f"    Events: {len(df)}")
                            if len(df) > 0:
                                print(f"    Columns: {', '.join(df.columns)}")
                                # Show summary stats if available
                                if 'counts' in df.columns:
                                    print(f"    Sample counts: {df['counts'].iloc[0] if len(df) > 0 else 'N/A'}")
                        except Exception as e:
                            print(f"    Error reading events: {e}")
    
    # Check rejections
    rej_path = os.path.join(out_dir, "rejections", "rejections.jsonl")
    if os.path.exists(rej_path):
        print(f"\n❌ Rejections Summary:")
        print("-" * 60)
        try:
            import json
            rejs = []
            with open(rej_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        rejs.append(json.loads(line))
            
            if rejs:
                df_rej = pd.DataFrame(rejs)
                print(f"Total rejections: {len(rejs)}")
                print("\nBy stage:")
                stage_counts = df_rej['stage'].value_counts()
                for stage, count in stage_counts.items():
                    print(f"  {stage}: {count}")
                print("\nBy reason:")
                reason_counts = df_rej['reason_code'].value_counts()
                for reason, count in reason_counts.items():
                    print(f"  {reason}: {count}")
            else:
                print("No rejections")
        except Exception as e:
            print(f"Error reading rejections: {e}")
    else:
        print("\n✅ No rejections file (all documents passed)")
    
    # Check manifest
    manifest_dir = os.path.join(out_dir, "manifests")
    if os.path.exists(manifest_dir):
        manifest_files = [f for f in os.listdir(manifest_dir) if f.endswith('.json')]
        if manifest_files:
            import json
            manifest_path = os.path.join(manifest_dir, manifest_files[0])
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            print(f"\n📋 Run Summary:")
            print("-" * 60)
            print(f"  Run ID: {manifest.get('run_id')}")
            print(f"  Policy Version: {manifest.get('policy_version')}")
            print(f"  Total Written: {manifest.get('total_written_docs', 0)}")
            print(f"  Total Rejected: {manifest.get('total_rejected_docs', 0)}")
    
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/view_analytics.py <out_dir>")
        print("Example: python scripts/view_analytics.py storage_example")
        sys.exit(1)
    
    out_dir = sys.argv[1]
    if not os.path.exists(out_dir):
        print(f"Error: Directory not found: {out_dir}")
        sys.exit(1)
    
    view_analytics(out_dir)
