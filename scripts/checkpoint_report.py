"""Generate detailed checkpoint report for resume/rerun planning.

Shows:
- Current checkpoint state per source
- Progress summary
- Resume instructions
- What needs to be rerun

Usage:
    python scripts/checkpoint_report.py [output_dir] [run_id]
"""

from __future__ import annotations
import sys
import os
import json
import glob
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

def load_checkpoint(out_dir: str, run_id: str) -> Optional[Dict[str, Any]]:
    """Load checkpoint file."""
    ckpt_path = os.path.join(out_dir, "checkpoints", f"{run_id}.json")
    if os.path.exists(ckpt_path):
        with open(ckpt_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def load_manifest(out_dir: str) -> Optional[Dict[str, Any]]:
    """Load run manifest."""
    manifest_dir = os.path.join(out_dir, "manifests")
    if not os.path.exists(manifest_dir):
        return None
    
    manifest_files = glob.glob(os.path.join(manifest_dir, "*.json"))
    if not manifest_files:
        return None
    
    with open(manifest_files[0], 'r', encoding='utf-8') as f:
        return json.load(f)

def get_output_shards(out_dir: str, source_name: str) -> int:
    """Count output shards for a source."""
    docs_dir = os.path.join(out_dir, "docs", f"source={source_name}")
    if os.path.exists(docs_dir):
        return len(glob.glob(os.path.join(docs_dir, "*.parquet")))
    return 0

def load_config_from_manifest(out_dir: str) -> Optional[Dict[str, Any]]:
    """Try to load config info from manifest or checkpoint."""
    manifest = load_manifest(out_dir)
    if manifest:
        # Check if config path is stored in manifest
        return manifest.get('config', {})
    return None

def generate_report(out_dir: str, run_id: Optional[str] = None, config_path: Optional[str] = None):
    """Generate checkpoint report."""
    
    # Find run_id if not provided
    if not run_id:
        manifest = load_manifest(out_dir)
        if manifest:
            run_id = manifest.get('run_id')
        else:
            # Try to find from checkpoint files
            ckpt_dir = os.path.join(out_dir, "checkpoints")
            if os.path.exists(ckpt_dir):
                ckpt_files = glob.glob(os.path.join(ckpt_dir, "*.json"))
                if ckpt_files:
                    # Extract run_id from filename
                    run_id = Path(ckpt_files[0]).stem
    
    if not run_id:
        print(f"❌ Error: Could not determine run_id for {out_dir}")
        print("   Specify run_id: python scripts/checkpoint_report.py <out_dir> <run_id>")
        sys.exit(1)
    
    checkpoint = load_checkpoint(out_dir, run_id)
    manifest = load_manifest(out_dir)
    
    # Load config if provided
    config = None
    if config_path and os.path.exists(config_path):
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
    
    print("\n" + "=" * 70)
    print("CHECKPOINT REPORT - Resume/Rerun Planning")
    print("=" * 70)
    print(f"\nRun ID: {run_id}")
    print(f"Output Directory: {out_dir}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    if not checkpoint:
        print("⚠️  No checkpoint file found.")
        print(f"   Expected: {out_dir}/checkpoints/{run_id}.json")
        print("\n   This run may not have started yet, or checkpointing is disabled.")
        return
    
    # Checkpoint metadata
    updated_at = checkpoint.get('updated_at_ms')
    if updated_at:
        updated_dt = datetime.fromtimestamp(updated_at / 1000)
        print(f"Last Checkpoint: {updated_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        age_seconds = (datetime.now().timestamp() * 1000 - updated_at) / 1000
        if age_seconds < 3600:
            print(f"Age: {int(age_seconds / 60)} minutes ago")
        elif age_seconds < 86400:
            print(f"Age: {int(age_seconds / 3600)} hours ago")
        else:
            print(f"Age: {int(age_seconds / 86400)} days ago")
    print()
    
    # Sources status
    sources = checkpoint.get('sources', {})
    if not sources:
        print("⚠️  No source progress found in checkpoint.")
        print("   Run may not have processed any sources yet.\n")
        return
    
    # Source configuration metadata
    if config:
        sources_config = config.get('sources', [])
        source_config_map = {src.get('name'): src for src in sources_config}
    else:
        source_config_map = {}
    
    print("=" * 70)
    print("SOURCE PROGRESS & METADATA")
    print("=" * 70 + "\n")
    
    total_processed = 0
    total_shards = 0
    
    for src_name, src_info in sources.items():
        processed = src_info.get('processed_docs', 0)
        shard_idx = src_info.get('shard_idx', 0)
        total_processed += processed
        total_shards += shard_idx
        
        # Check actual output
        actual_shards = get_output_shards(out_dir, src_name)
        
        print(f"Source: {src_name}")
        print("-" * 70)
        
        # Show source metadata if available
        if src_name in source_config_map:
            src_cfg = source_config_map[src_name]
            print(f"  Source Type: {src_cfg.get('kind', 'unknown')}")
            dataset = src_cfg.get('dataset', 'N/A')
            print(f"  Dataset/File: {dataset}")
            
            if src_cfg.get('kind') == 'hf_stream':
                print(f"  Split: {src_cfg.get('split', 'train')}")
                print(f"  Text Field: {src_cfg.get('text_field', 'text')}")
            elif src_cfg.get('kind') == 'local_jsonl':
                if os.path.exists(dataset):
                    file_size = os.path.getsize(dataset)
                    print(f"  File Size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
                else:
                    print(f"  ⚠️  File not found: {dataset}")
            
            print(f"  License Field: {src_cfg.get('license_field', 'license')}")
            print(f"  URL Field: {src_cfg.get('url_field', 'url')}")
            print()
        
        print(f"  Processed Documents: {processed:,}")
        print(f"  Shards Written: {shard_idx}")
        print(f"  Actual Output Shards: {actual_shards}")
        
        if shard_idx != actual_shards:
            print(f"  ⚠️  Mismatch: Checkpoint shows {shard_idx} shards, but {actual_shards} exist")
            print(f"     Possible incomplete write or manual deletion")
        
        # Resume info
        print(f"\n  Resume Information:")
        print(f"    - Will skip first {processed:,} documents")
        print(f"    - Will start writing at shard_{shard_idx:06d}")
        
        # Check if source is complete
        if processed > 0 and actual_shards > 0:
            print(f"    - Status: {'✅ Complete' if shard_idx == actual_shards else '⚠️  In Progress'}")
        elif processed == 0:
            print(f"    - Status: ⏳ Not Started")
        else:
            print(f"    - Status: ⚠️  Checkpointed but no output found")
        
        print()
    
    # Overall summary
    print("=" * 70)
    print("OVERALL SUMMARY")
    print("=" * 70 + "\n")
    print(f"Total Sources: {len(sources)}")
    print(f"Total Processed: {total_processed:,} documents")
    print(f"Total Shards Written: {total_shards}")
    
    if manifest:
        written = manifest.get('total_written_docs', 0)
        rejected = manifest.get('total_rejected_docs', 0)
        print(f"\nFrom Manifest:")
        print(f"  Written: {written:,} documents")
        print(f"  Rejected: {rejected:,} documents")
        print(f"  Total: {written + rejected:,} documents")
    
    print()
    
    # Resume instructions
    print("=" * 70)
    print("RESUME INSTRUCTIONS")
    print("=" * 70 + "\n")
    print("To resume this run:")
    print(f"  1. Use the same config file")
    print(f"  2. Ensure output directory is: {out_dir}")
    print(f"  3. Run: clean-corpus build --config <your_config.yaml>")
    print(f"\nThe pipeline will automatically:")
    print(f"  - Load checkpoint: {out_dir}/checkpoints/{run_id}.json")
    print(f"  - Skip already processed documents per source")
    print(f"  - Continue writing from last shard index")
    print()
    
    # Rerun instructions
    print("=" * 70)
    print("RERUN INSTRUCTIONS")
    print("=" * 70 + "\n")
    print("To rerun from scratch:")
    print(f"  1. Delete checkpoint: rm {out_dir}/checkpoints/{run_id}.json")
    print(f"  2. (Optional) Delete outputs: rm -rf {out_dir}/docs/*")
    print(f"  3. Run: clean-corpus build --config <your_config.yaml>")
    print()
    print("To rerun specific source:")
    print(f"  1. Edit checkpoint: {out_dir}/checkpoints/{run_id}.json")
    print(f"  2. Set source progress to 0:")
    print(f'     "sources": {{"source_name": {{"processed_docs": 0, "shard_idx": 0}}}}')
    print(f"  3. Delete source outputs: rm -rf {out_dir}/docs/source=source_name/*")
    print(f"  4. Run pipeline again")
    print()
    
    # Checkpoint file location
    print("=" * 70)
    print("CHECKPOINT FILE")
    print("=" * 70 + "\n")
    ckpt_path = os.path.join(out_dir, "checkpoints", f"{run_id}.json")
    print(f"Location: {ckpt_path}")
    if os.path.exists(ckpt_path):
        file_size = os.path.getsize(ckpt_path)
        print(f"Size: {file_size:,} bytes")
        print(f"\nContents:")
        print("-" * 70)
        with open(ckpt_path, 'r', encoding='utf-8') as f:
            print(json.dumps(json.load(f), indent=2))
    print()
    
    # Warnings
    print("=" * 70)
    print("IMPORTANT NOTES")
    print("=" * 70 + "\n")
    print("⚠️  Checkpoint Resume Limitations:")
    print("  - Streaming sources: Best-effort resume (skips N records)")
    print("  - Not deterministic for non-seekable streams")
    print("  - Exact dedup state is lost on restart (will reprocess)")
    print("\n✅ Safe to Resume:")
    print("  - Batch sources (local_jsonl)")
    print("  - Deterministic sources")
    print("\n⚠️  May Need Rerun:")
    print("  - If checkpoint is very old")
    print("  - If source data changed")
    print("  - If you need exact dedup across full run")
    print()
    
    print("=" * 70 + "\n")

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate checkpoint report")
    parser.add_argument("output_dir", help="Output directory")
    parser.add_argument("run_id", nargs="?", help="Run ID (auto-detected if not provided)")
    parser.add_argument("--config", help="Config file path (for source metadata)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        print(f"Error: Directory not found: {args.output_dir}")
        sys.exit(1)
    
    # Try to find config if not provided
    config_path = args.config
    if not config_path:
        # Look for common config locations
        for possible_config in ["configs/build.yaml", "examples/build_local_jsonl.yaml"]:
            if os.path.exists(possible_config):
                config_path = possible_config
                break
    
    generate_report(args.output_dir, args.run_id, config_path)

if __name__ == "__main__":
    main()
