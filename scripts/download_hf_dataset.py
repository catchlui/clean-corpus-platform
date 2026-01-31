#!/usr/bin/env python3
"""Download HuggingFace dataset programmatically.

This script downloads HuggingFace datasets using Python code instead of CLI.

Usage:
    python scripts/download_hf_dataset.py <dataset_name> [--local-dir DIR] [--repo-type TYPE]
    
Examples:
    # Download to default cache
    python scripts/download_hf_dataset.py datasets/pg19
    
    # Download to custom directory
    python scripts/download_hf_dataset.py datasets/pg19 --local-dir pg19_raw
    
    # Download dataset (not model)
    python scripts/download_hf_dataset.py datasets/pg19 --repo-type dataset
"""

from __future__ import annotations
import sys
import argparse
from pathlib import Path

def download_dataset(
    dataset_name: str,
    local_dir: str | None = None,
    repo_type: str = "dataset",
    resume_download: bool = True
) -> str:
    """Download HuggingFace dataset programmatically.
    
    Args:
        dataset_name: HuggingFace dataset name (e.g., "datasets/pg19")
        local_dir: Optional local directory to download to
        repo_type: Repository type ("dataset" or "model")
        resume_download: Whether to resume interrupted downloads
        
    Returns:
        Path where dataset was downloaded
    """
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        print("Error: 'huggingface_hub' library not installed.")
        print("Install with: pip install huggingface_hub")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Downloading HuggingFace Dataset")
    print(f"{'='*60}\n")
    print(f"Dataset: {dataset_name}")
    print(f"Repository Type: {repo_type}")
    if local_dir:
        print(f"Local Directory: {local_dir}")
    else:
        print(f"Download Location: HuggingFace cache (default)")
    print()
    
    try:
        # Download dataset
        print("Starting download...")
        download_path = snapshot_download(
            repo_id=dataset_name,
            repo_type=repo_type,
            local_dir=local_dir,
            resume_download=resume_download,
        )
        
        print(f"\n✅ Download complete!")
        print(f"   Location: {download_path}")
        
        # Show directory size if available
        if Path(download_path).exists():
            total_size = sum(
                f.stat().st_size
                for f in Path(download_path).rglob('*')
                if f.is_file()
            )
            print(f"   Size: {total_size / 1024 / 1024 / 1024:.2f} GB")
        
        return download_path
        
    except Exception as e:
        print(f"\n❌ Error downloading dataset: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download HuggingFace dataset programmatically"
    )
    parser.add_argument("dataset", help="HuggingFace dataset name (e.g., datasets/pg19)")
    parser.add_argument("--local-dir", help="Local directory to download to")
    parser.add_argument("--repo-type", default="dataset", choices=["dataset", "model"],
                       help="Repository type (default: dataset)")
    parser.add_argument("--no-resume", action="store_true",
                       help="Don't resume interrupted downloads")
    
    args = parser.parse_args()
    
    download_dataset(
        args.dataset,
        local_dir=args.local_dir,
        repo_type=args.repo_type,
        resume_download=not args.no_resume
    )
