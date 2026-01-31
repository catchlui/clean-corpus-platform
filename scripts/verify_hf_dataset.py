"""Verify HuggingFace dataset exists and show schema.

Usage:
    python scripts/verify_hf_dataset.py <dataset_name> [split]
    
Example:
    python scripts/verify_hf_dataset.py common-pile/comma_v0.1_training_dataset train
"""

from __future__ import annotations
import sys

def verify_dataset(dataset_name: str, split: str = "train"):
    """Verify dataset and show schema."""
    try:
        from datasets import load_dataset
    except ImportError:
        print("Error: 'datasets' library not installed.")
        print("Install with: pip install datasets")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Verifying HuggingFace Dataset")
    print(f"{'='*60}\n")
    print(f"Dataset: {dataset_name}")
    print(f"Split: {split}\n")
    
    try:
        # Try loading in streaming mode
        print("Loading dataset (streaming mode)...")
        ds = load_dataset(dataset_name, split=split, streaming=True)
        
        # Get first sample
        print("Fetching first sample...")
        sample = next(iter(ds))
        
        print(f"\n✅ Dataset loaded successfully!\n")
        print("Schema (field names):")
        print("-" * 60)
        for key, value in sample.items():
            value_preview = str(value)[:100] if value else "None"
            if len(str(value)) > 100:
                value_preview += "..."
            print(f"  {key}: {type(value).__name__} = {value_preview}")
        
        print(f"\n{'='*60}\n")
        print("✅ This dataset can be used with 'hf_stream' source type")
        print("\nSuggested config:")
        print("-" * 60)
        print(f'sources:')
        print(f'  - name: "my_source"')
        print(f'    type: "streaming"')
        print(f'    kind: "hf_stream"')
        print(f'    dataset: "{dataset_name}"')
        print(f'    split: "{split}"')
        
        # Suggest field names
        text_fields = [k for k in sample.keys() if 'text' in k.lower()]
        license_fields = [k for k in sample.keys() if 'license' in k.lower()]
        url_fields = [k for k in sample.keys() if 'url' in k.lower()]
        
        print(f'    text_field: "{text_fields[0] if text_fields else "text"}"')
        if license_fields:
            print(f'    license_field: "{license_fields[0]}"')
        else:
            print(f'    license_field: "license"  # ⚠️ Verify this field exists')
        if url_fields:
            print(f'    url_field: "{url_fields[0]}"')
        else:
            print(f'    url_field: "url"  # ⚠️ Verify this field exists')
        
        print()
        
    except Exception as e:
        print(f"\n❌ Error loading dataset:\n")
        print(f"  {type(e).__name__}: {e}\n")
        print("Possible issues:")
        print("  1. Dataset name incorrect - check https://huggingface.co/datasets")
        print("  2. Split name incorrect - check dataset card for available splits")
        print("  3. Dataset requires authentication - may need HuggingFace token")
        print("  4. Network issues - check internet connection")
        print()
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/verify_hf_dataset.py <dataset_name> [split]")
        print("\nExample:")
        print("  python scripts/verify_hf_dataset.py common-pile/comma_v0.1_training_dataset train")
        sys.exit(1)
    
    dataset_name = sys.argv[1]
    split = sys.argv[2] if len(sys.argv) > 2 else "train"
    
    verify_dataset(dataset_name, split)
