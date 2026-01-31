#!/usr/bin/env python3
"""Test HuggingFace dataset download functionality.

This script tests the automatic download feature without processing a full pipeline.

Usage:
    python scripts/test_hf_download.py <dataset_name> [--local-dir DIR]
    
Example:
    python scripts/test_hf_download.py datasets/pg19
"""

from __future__ import annotations
import sys
import argparse

def test_download(dataset_name: str, local_dir: str | None = None):
    """Test downloading a HuggingFace dataset."""
    print(f"\n{'='*60}")
    print(f"Testing HuggingFace Dataset Download")
    print(f"{'='*60}\n")
    print(f"Dataset: {dataset_name}")
    if local_dir:
        print(f"Local Directory: {local_dir}")
    print()
    
    # Test 1: Check if huggingface_hub is available
    print("Test 1: Checking dependencies...")
    try:
        from huggingface_hub import snapshot_download
        print("✅ huggingface_hub is installed")
    except ImportError:
        print("❌ huggingface_hub not installed")
        print("   Install with: pip install huggingface_hub")
        return False
    
    try:
        from datasets import load_dataset
        print("✅ datasets library is installed")
    except ImportError:
        print("❌ datasets library not installed")
        print("   Install with: pip install datasets")
        return False
    
    print()
    
    # Test 2: Try downloading
    print("Test 2: Attempting download...")
    try:
        from huggingface_hub import snapshot_download
        download_path = snapshot_download(
            repo_id=dataset_name,
            repo_type="dataset",
            local_dir=local_dir,
            resume_download=True,
        )
        print(f"✅ Download successful!")
        print(f"   Location: {download_path}")
        
        # Check if files exist
        from pathlib import Path
        if Path(download_path).exists():
            files = list(Path(download_path).rglob('*'))
            file_count = len([f for f in files if f.is_file()])
            print(f"   Files found: {file_count}")
        
    except Exception as e:
        print(f"❌ Download failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print()
    
    # Test 3: Try loading the dataset
    print("Test 3: Testing dataset loading...")
    try:
        from datasets import load_dataset
        print("   Loading dataset (streaming mode)...")
        ds = load_dataset(dataset_name, split="train", streaming=True)
        
        # Get first sample
        print("   Fetching first sample...")
        sample = next(iter(ds))
        
        print(f"✅ Dataset loaded successfully!")
        print(f"   Fields: {', '.join(sample.keys())}")
        print(f"   Sample preview:")
        for key, value in list(sample.items())[:3]:
            value_str = str(value)[:50] if value else "None"
            if len(str(value)) > 50:
                value_str += "..."
            print(f"     {key}: {value_str}")
        
    except Exception as e:
        print(f"⚠️  Warning: Could not load dataset: {e}")
        print("   This might be okay if the dataset structure is different")
        return True  # Download worked even if loading failed
    
    print()
    print(f"{'='*60}")
    print("✅ All tests passed!")
    print(f"{'='*60}\n")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test HuggingFace dataset download functionality"
    )
    parser.add_argument("dataset", help="HuggingFace dataset name (e.g., datasets/pg19)")
    parser.add_argument("--local-dir", help="Local directory to download to")
    
    args = parser.parse_args()
    
    success = test_download(args.dataset, args.local_dir)
    sys.exit(0 if success else 1)
