#!/usr/bin/env python3
"""Reset checkpoint to start fresh.

Usage:
    python scripts/reset_checkpoint.py <out_dir> [run_id]
    
Example:
    python scripts/reset_checkpoint.py storage_example
    python scripts/reset_checkpoint.py storage_example ExampleJSONL_2026-01-28
"""

from __future__ import annotations
import sys
import os
import json
import glob

# Fix Unicode encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

def reset_checkpoint(out_dir: str, run_id: str | None = None):
    """Reset checkpoint for a run."""
    checkpoint_dir = os.path.join(out_dir, "checkpoints")
    
    if not os.path.exists(checkpoint_dir):
        print(f"Checkpoint directory not found: {checkpoint_dir}")
        return
    
    if run_id:
        checkpoint_file = os.path.join(checkpoint_dir, f"{run_id}.json")
        if os.path.exists(checkpoint_file):
            print(f"Resetting checkpoint: {checkpoint_file}")
            # Reset to empty state
            new_state = {
                "run_id": run_id,
                "sources": {}
            }
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(new_state, f, indent=2)
            print(f"[OK] Checkpoint reset - will start from beginning")
        else:
            print(f"Checkpoint file not found: {checkpoint_file}")
    else:
        # Find all checkpoints
        checkpoint_files = glob.glob(os.path.join(checkpoint_dir, "*.json"))
        if not checkpoint_files:
            print(f"No checkpoint files found in {checkpoint_dir}")
            return
        
        print(f"Found {len(checkpoint_files)} checkpoint file(s):")
        for cf in checkpoint_files:
            print(f"  - {os.path.basename(cf)}")
        
        # Reset all
        for checkpoint_file in checkpoint_files:
            run_id_from_file = os.path.basename(checkpoint_file).replace('.json', '')
            print(f"\nResetting checkpoint: {os.path.basename(checkpoint_file)}")
            new_state = {
                "run_id": run_id_from_file,
                "sources": {}
            }
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(new_state, f, indent=2)
            print(f"[OK] Reset - will start from beginning")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/reset_checkpoint.py <out_dir> [run_id]")
        print("\nExample:")
        print("  python scripts/reset_checkpoint.py storage_example")
        print("  python scripts/reset_checkpoint.py storage_example ExampleJSONL_2026-01-28")
        sys.exit(1)
    
    out_dir = sys.argv[1]
    run_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    reset_checkpoint(out_dir, run_id)
