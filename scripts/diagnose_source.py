"""Diagnose why a source isn't yielding documents.

Usage:
    python scripts/diagnose_source.py examples/build_local_jsonl.yaml
"""

from __future__ import annotations
import sys
import os
import yaml
from pathlib import Path

def diagnose_source(config_path: str):
    """Diagnose source configuration and file."""
    print(f"\n{'='*70}")
    print(f"Source Diagnosis: {config_path}")
    print(f"{'='*70}\n")
    
    # Load config
    if not os.path.exists(config_path):
        print(f"❌ Config file not found: {config_path}")
        return
    
    with open(config_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}
    
    sources = cfg.get("sources", [])
    if not sources:
        print("❌ No sources configured in config file")
        return
    
    for idx, src_cfg in enumerate(sources, 1):
        print(f"\nSource #{idx}: {src_cfg.get('name', 'unnamed')}")
        print("-" * 70)
        
        src_kind = src_cfg.get('kind', 'unknown')
        dataset = src_cfg.get('dataset', 'N/A')
        
        print(f"Kind: {src_kind}")
        print(f"Dataset/Path: {dataset}")
        
        if src_kind == 'local_jsonl':
            # Check file
            if not os.path.exists(dataset):
                print(f"❌ File not found: {dataset}")
                print(f"   Current directory: {os.getcwd()}")
                print(f"   Absolute path: {os.path.abspath(dataset)}")
                continue
            
            file_size = os.path.getsize(dataset)
            print(f"✅ File exists: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
            
            if file_size == 0:
                print("❌ File is empty!")
                continue
            
            # Count lines
            try:
                line_count = 0
                valid_json_count = 0
                empty_lines = 0
                text_field = src_cfg.get('text_field', 'text')
                min_text_length = 0
                
                with open(dataset, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            empty_lines += 1
                            continue
                        
                        line_count += 1
                        try:
                            ex = json.loads(line)
                            valid_json_count += 1
                            
                            # Check text field
                            text = ex.get(text_field, "") or ""
                            if len(text) < min_text_length:
                                print(f"   ⚠️  Line {line_num}: Text too short ({len(text)} chars)")
                            
                            if line_num <= 3:
                                print(f"   Line {line_num}: text_length={len(text)}, id={ex.get('id', 'N/A')}")
                        except json.JSONDecodeError as e:
                            print(f"   ❌ Line {line_num}: Invalid JSON - {e}")
                
                print(f"\nFile Statistics:")
                print(f"   Total lines: {line_count + empty_lines}")
                print(f"   Non-empty lines: {line_count}")
                print(f"   Valid JSON: {valid_json_count}")
                print(f"   Empty lines: {empty_lines}")
                
                if valid_json_count == 0:
                    print("\n❌ No valid JSON documents found in file!")
                else:
                    print(f"\n✅ File looks good - {valid_json_count} valid documents")
                    
            except Exception as e:
                print(f"❌ Error reading file: {e}")
                import traceback
                traceback.print_exc()
        
        elif src_kind == 'pdf':
            pdf_path = Path(dataset)
            if pdf_path.is_file():
                if not pdf_path.exists():
                    print(f"❌ PDF file not found: {dataset}")
                else:
                    file_size = pdf_path.stat().st_size
                    print(f"✅ PDF file exists: {file_size:,} bytes")
            elif pdf_path.is_dir():
                pdf_files = list(pdf_path.glob("*.pdf"))
                print(f"✅ Directory exists: {len(pdf_files)} PDF files found")
                if len(pdf_files) == 0:
                    print("❌ No PDF files found in directory!")
            else:
                print(f"❌ Path not found: {dataset}")
        
        # Try to create source and check if it yields documents
        print(f"\nTesting source iterator...")
        try:
            from clean_corpus.sources.registry import make_source
            from clean_corpus.sources.base import SourceSpec
            
            spec = SourceSpec(**src_cfg)
            src = make_source(spec)
            
            # Try to get first few documents
            doc_count = 0
            for i, raw in enumerate(src.stream()):
                doc_count += 1
                if i == 0:
                    print(f"   ✅ First document received:")
                    print(f"      raw_id: {raw.raw_id[:30] if raw.raw_id else 'N/A'}")
                    print(f"      text_length: {len(raw.text)}")
                    print(f"      source: {raw.source}")
                    print(f"      license: {raw.license}")
                
                if i >= 2:  # Check first 3 documents
                    break
            
            if doc_count == 0:
                print(f"   ❌ Source iterator yielded 0 documents!")
                print(f"      Check source file and configuration")
            else:
                print(f"   ✅ Source iterator working - yielded {doc_count} document(s)")
                
        except Exception as e:
            print(f"   ❌ Error creating/testing source: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    import json
    
    if len(sys.argv) < 2:
        print("Usage: python scripts/diagnose_source.py <config_file>")
        print("Example: python scripts/diagnose_source.py examples/build_local_jsonl.yaml")
        sys.exit(1)
    
    diagnose_source(sys.argv[1])
