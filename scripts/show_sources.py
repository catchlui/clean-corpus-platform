"""Show which datasets/sources are configured in a build config.

Usage:
    python scripts/show_sources.py [config_file]
"""

from __future__ import annotations
import sys
import yaml
from pathlib import Path

def show_sources(config_path: str):
    """Display source information from config."""
    print(f"\n{'='*60}")
    print(f"Source Configuration: {config_path}")
    print(f"{'='*60}\n")
    
    # Load config
    with open(config_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}
    
    sources = cfg.get("sources", [])
    
    if not sources:
        print("⚠️  No sources configured")
        return
    
    for idx, src_cfg in enumerate(sources, 1):
        print(f"Source #{idx}:")
        print(f"  Name: {src_cfg.get('name', 'N/A')}")
        print(f"  Type: {src_cfg.get('type', 'N/A')}")
        print(f"  Kind: {src_cfg.get('kind', 'N/A')}")
        
        # Show dataset-specific info
        dataset = src_cfg.get('dataset', 'N/A')
        print(f"  Dataset/Path: {dataset}")
        
        if src_cfg.get('kind') == 'hf_stream':
            print(f"  Split: {src_cfg.get('split', 'train')}")
            print(f"  Text Field: {src_cfg.get('text_field', 'text')}")
        elif src_cfg.get('kind') == 'local_jsonl':
            # Check if file exists
            dataset_path = Path(dataset)
            if dataset_path.exists():
                file_size = dataset_path.stat().st_size
                print(f"  File Size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
                # Count lines (approximate)
                try:
                    with open(dataset_path, 'r', encoding='utf-8') as f:
                        line_count = sum(1 for _ in f)
                    print(f"  Estimated Documents: {line_count:,}")
                except:
                    pass
            else:
                print(f"  ⚠️  File not found: {dataset}")
        
        print(f"  License Field: {src_cfg.get('license_field', 'license')}")
        print(f"  URL Field: {src_cfg.get('url_field', 'url')}")
        print()
    
    print(f"{'='*60}\n")

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/build.yaml"
    
    if not Path(config_path).exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    show_sources(config_path)
