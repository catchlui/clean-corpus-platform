#!/usr/bin/env python3
"""Convert HuggingFace dataset to JSONL format.

This script converts a HuggingFace dataset (including locally downloaded ones)
to JSONL format for processing with the local_jsonl source.

Usage:
    python scripts/convert_hf_to_jsonl.py <dataset_name> <split> <output_file> [--local-dir DIR]
    
Examples:
    # Convert from HuggingFace Hub
    python scripts/convert_hf_to_jsonl.py datasets/pg19 train pg19_train.jsonl
    
    # Convert from local directory (downloaded via huggingface-cli)
    python scripts/convert_hf_to_jsonl.py datasets/pg19 train pg19_train.jsonl --local-dir pg19_raw
"""

from __future__ import annotations
import sys
import json
import argparse
from pathlib import Path

def convert_dataset(dataset_name: str, split: str, output_file: str, local_dir: Optional[str] = None):
    """Convert HuggingFace dataset to JSONL."""
    try:
        from datasets import load_dataset
    except ImportError:
        print("Error: 'datasets' library not installed.")
        print("Install with: pip install datasets")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Converting HuggingFace Dataset to JSONL")
    print(f"{'='*60}\n")
    print(f"Dataset: {dataset_name}")
    print(f"Split: {split}")
    print(f"Output: {output_file}")
    if local_dir:
        print(f"Local Directory: {local_dir}")
    print()
    
    try:
        # Load dataset
        if local_dir:
            print(f"Loading from local directory: {local_dir}")
            # Try loading from local directory
            try:
                dataset = load_dataset(
                    path=dataset_name,
                    data_dir=local_dir,
                    split=split
                )
            except Exception as e:
                print(f"Warning: Could not load from {local_dir}: {e}")
                print("Trying standard load (will use cache)...")
                dataset = load_dataset(dataset_name, split=split)
        else:
            print("Loading from HuggingFace Hub (will use cache if available)...")
            dataset = load_dataset(dataset_name, split=split)
        
        print(f"✅ Dataset loaded: {len(dataset)} examples\n")
        
        # Show schema
        if len(dataset) > 0:
            sample = dataset[0]
            print("Dataset schema:")
            print("-" * 60)
            for key in sample.keys():
                print(f"  - {key}")
            print()
        
        # Convert to JSONL
        print(f"Converting to JSONL...")
        count = 0
        with open(output_file, 'w', encoding='utf-8') as f:
            for example in dataset:
                # Convert to dict and ensure all values are JSON-serializable
                record = {}
                for key, value in example.items():
                    # Handle various types
                    if value is None:
                        record[key] = None
                    elif isinstance(value, (str, int, float, bool)):
                        record[key] = value
                    elif isinstance(value, list):
                        record[key] = value
                    elif isinstance(value, dict):
                        record[key] = value
                    else:
                        # Convert to string for other types
                        record[key] = str(value)
                
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
                count += 1
                
                if count % 1000 == 0:
                    print(f"  Converted {count:,} examples...", end='\r')
        
        print(f"\n✅ Conversion complete!")
        print(f"   Total examples: {count:,}")
        print(f"   Output file: {output_file}")
        print(f"   File size: {Path(output_file).stat().st_size / 1024 / 1024:.2f} MB")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert HuggingFace dataset to JSONL format"
    )
    parser.add_argument("dataset", help="HuggingFace dataset name (e.g., datasets/pg19)")
    parser.add_argument("split", help="Dataset split (e.g., train, test)")
    parser.add_argument("output", help="Output JSONL file path")
    parser.add_argument("--local-dir", help="Local directory where dataset was downloaded")
    
    args = parser.parse_args()
    
    convert_dataset(args.dataset, args.split, args.output, args.local_dir)
