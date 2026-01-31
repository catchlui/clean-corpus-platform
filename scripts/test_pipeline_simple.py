#!/usr/bin/env python3
"""Simple pipeline test that avoids problematic dependencies.

This test verifies the pipeline can run with local JSONL files
without requiring HuggingFace datasets or problematic scipy/torch imports.
"""

from __future__ import annotations
import sys
import os

# Fix Unicode encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def test_basic_imports():
    """Test that basic modules can be imported."""
    print("Testing basic imports...")
    try:
        from clean_corpus.sources.local_jsonl import LocalJSONLSource
        from clean_corpus.sources.base import SourceSpec
        print("[OK] Basic source imports successful")
        return True
    except Exception as e:
        print(f"[FAIL] Import error: {e}")
        return False

def test_source_creation():
    """Test creating a local JSONL source."""
    print("\nTesting source creation...")
    try:
        from clean_corpus.sources.local_jsonl import LocalJSONLSource
        from clean_corpus.sources.base import SourceSpec
        
        spec = SourceSpec(
            name="test_source",
            type="batch",
            kind="local_jsonl",
            dataset="examples/sample_internal.jsonl",
            text_field="text",
            license_field="license",
            url_field="url"
        )
        
        source = LocalJSONLSource(spec)
        print(f"[OK] Source created: {source.name}")
        
        # Test metadata
        metadata = source.metadata()
        print(f"[OK] Metadata: {metadata}")
        
        return True
    except Exception as e:
        print(f"❌ Source creation error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_file_reading():
    """Test reading from sample file."""
    print("\nTesting file reading...")
    try:
        from clean_corpus.sources.local_jsonl import LocalJSONLSource
        from clean_corpus.sources.base import SourceSpec
        
        spec = SourceSpec(
            name="test_source",
            type="batch",
            kind="local_jsonl",
            dataset="examples/sample_internal.jsonl",
            text_field="text",
            license_field="license",
            url_field="url"
        )
        
        source = LocalJSONLSource(spec)
        
        # Read first few documents
        count = 0
        for doc in source.stream():
            count += 1
            print(f"  Document {count}: id={doc.raw_id[:20]}, text_length={len(doc.text)}")
            if count >= 3:
                break
        
        print(f"[OK] Successfully read {count} documents")
        return True
    except Exception as e:
        print(f"[FAIL] File reading error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_config_loading():
    """Test loading YAML config."""
    print("\nTesting config loading...")
    try:
        import yaml
        
        config_path = "examples/build_local_jsonl.yaml"
        if not os.path.exists(config_path):
            print(f"⚠️  Config file not found: {config_path}")
            return False
        
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        
        print(f"[OK] Config loaded: {len(cfg.get('sources', []))} source(s)")
        print(f"   Run ID: {cfg.get('run', {}).get('run_id')}")
        print(f"   Output: {cfg.get('run', {}).get('out_dir')}")
        return True
    except Exception as e:
        print(f"[FAIL] Config loading error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("Simple Pipeline Test")
    print("=" * 60)
    
    results = []
    
    results.append(("Basic Imports", test_basic_imports()))
    results.append(("Source Creation", test_source_creation()))
    results.append(("File Reading", test_file_reading()))
    results.append(("Config Loading", test_config_loading()))
    
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    
    for test_name, passed in results:
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status}: {test_name}")
    
    all_passed = all(result[1] for result in results)
    
    if all_passed:
        print("\n[OK] All basic tests passed!")
        print("\nTo run full pipeline:")
        print("  python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml")
    else:
        print("\n[WARN] Some tests failed. Check errors above.")
        print("\nNote: Full pipeline may have dependency issues.")
        print("Try fixing scipy/torch dependencies or use a clean environment.")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
