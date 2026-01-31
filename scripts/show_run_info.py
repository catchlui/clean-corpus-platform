"""Show information about a completed run, including which datasets were processed.

Usage:
    python scripts/show_run_info.py [output_dir]
"""

from __future__ import annotations
import sys
import json
import os
from pathlib import Path
import glob

def show_run_info(out_dir: str):
    """Display run information including sources processed."""
    print(f"\n{'='*60}")
    print(f"Run Information: {out_dir}")
    print(f"{'='*60}\n")
    
    # Check manifest
    manifest_dir = os.path.join(out_dir, "manifests")
    if os.path.exists(manifest_dir):
        manifest_files = glob.glob(os.path.join(manifest_dir, "*.json"))
        if manifest_files:
            with open(manifest_files[0], 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            print("📋 Run Summary:")
            print("-" * 60)
            print(f"  Run ID: {manifest.get('run_id')}")
            print(f"  Policy Version: {manifest.get('policy_version')}")
            print(f"  Total Written: {manifest.get('total_written_docs', 0):,} docs")
            print(f"  Total Rejected: {manifest.get('total_rejected_docs', 0):,} docs")
            
            sources = manifest.get('sources', {})
            if sources:
                print(f"\n📊 Sources Processed:")
                print("-" * 60)
                for src_name, src_info in sources.items():
                    processed = src_info.get('processed_docs', 0)
                    shard_idx = src_info.get('shard_idx', 0)
                    print(f"  {src_name}:")
                    print(f"    Processed: {processed:,} docs")
                    print(f"    Shards Written: {shard_idx}")
                    
                    # Check output directories
                    docs_dir = os.path.join(out_dir, "docs", f"source={src_name}")
                    if os.path.exists(docs_dir):
                        shard_files = glob.glob(os.path.join(docs_dir, "*.parquet"))
                        print(f"    Output Files: {len(shard_files)} shards")
            
            print()
    
    # Check output directories
    docs_dir = os.path.join(out_dir, "docs")
    if os.path.exists(docs_dir):
        print("📁 Output Directories:")
        print("-" * 60)
        source_dirs = [d for d in os.listdir(docs_dir) if os.path.isdir(os.path.join(docs_dir, d))]
        for src_dir in source_dirs:
            src_name = src_dir.replace("source=", "")
            src_path = os.path.join(docs_dir, src_dir)
            shard_files = glob.glob(os.path.join(src_path, "*.parquet"))
            total_size = sum(os.path.getsize(f) for f in shard_files)
            print(f"  {src_name}:")
            print(f"    Shards: {len(shard_files)}")
            print(f"    Total Size: {total_size / 1024 / 1024:.2f} MB")
    
    # Check analytics
    analytics_dir = os.path.join(out_dir, "analytics", "events")
    if os.path.exists(analytics_dir):
        print(f"\n📈 Analytics Sources:")
        print("-" * 60)
        stage_dirs = [d for d in os.listdir(analytics_dir) if os.path.isdir(os.path.join(analytics_dir, d))]
        sources_seen = set()
        for stage_dir in stage_dirs:
            stage_path = os.path.join(analytics_dir, stage_dir)
            date_dirs = [d for d in os.listdir(stage_path) if os.path.isdir(os.path.join(stage_path, d))]
            for date_dir in date_dirs:
                event_file = os.path.join(stage_path, date_dir, "events.parquet")
                if os.path.exists(event_file):
                    try:
                        import pandas as pd
                        df = pd.read_parquet(event_file)
                        if 'source' in df.columns:
                            sources_seen.update(df['source'].unique())
                    except:
                        pass
        
        if sources_seen:
            for src in sorted(sources_seen):
                print(f"  {src}")
    
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "storage_example"
    
    if not os.path.exists(out_dir):
        print(f"Error: Directory not found: {out_dir}")
        sys.exit(1)
    
    show_run_info(out_dir)
