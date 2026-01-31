#!/usr/bin/env python3
"""All-in-one pipeline runner for Clean Corpus Platform.

This script handles:
- Bootstrap PII detectors
- Verify configuration
- Run pipeline
- Show results
- Optional real-time monitoring

Usage:
    python scripts/run_pipeline.py [config_file] [options]
    
Examples:
    # Basic usage
    python scripts/run_pipeline.py examples/build_local_jsonl.yaml
    
    # With monitoring
    python scripts/run_pipeline.py examples/build_local_jsonl.yaml --monitor
    
    # Skip verification
    python scripts/run_pipeline.py examples/build_local_jsonl.yaml --no-verify
"""

from __future__ import annotations
import sys
import os
import argparse
import json
import time
import subprocess
from pathlib import Path
from typing import Optional

# Fix Unicode encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

def safe_print(text: str):
    """Print with fallback for Unicode issues."""
    try:
        print(text)
    except UnicodeEncodeError:
        # Remove emojis and special characters for Windows console
        import re
        text_clean = re.sub(r'[^\x00-\x7F]+', '', text)
        print(text_clean)

def bootstrap_pii():
    """Bootstrap PII detectors."""
    try:
        print("🔧 Bootstrapping PII detectors...")
    except UnicodeEncodeError:
        print("Bootstrapping PII detectors...")
    try:
        from clean_corpus.pii.registry import register_detector
        from clean_corpus.pii.detectors.email import EmailDetector
        from clean_corpus.pii.detectors.phone import PhoneDetector
        from clean_corpus.pii.detectors.aadhaar import AadhaarDetector
        
        register_detector(EmailDetector())
        register_detector(PhoneDetector())
        register_detector(AadhaarDetector())
        print("✅ Registered detectors: email, phone, aadhaar\n")
        return True
    except Exception as e:
        print(f"⚠️  Warning: Could not bootstrap PII detectors: {e}\n")
        return False

def verify_config(config_path: str) -> bool:
    """Verify configuration file."""
    safe_print(f"📋 Verifying configuration: {config_path}")
    
    if not os.path.exists(config_path):
        print(f"❌ Error: Config file not found: {config_path}\n")
        return False
    
    try:
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
        
        # Check required sections
        required = ['run', 'sources', 'policies', 'stages']
        missing = [r for r in required if r not in cfg]
        if missing:
            print(f"❌ Error: Missing required sections: {', '.join(missing)}\n")
            return False
        
        # Check sources
        sources = cfg.get('sources', [])
        if not sources:
            print("⚠️  Warning: No sources configured\n")
        else:
            print(f"✅ Found {len(sources)} source(s)")
            for src in sources:
                src_name = src.get('name', 'unnamed')
                src_kind = src.get('kind', 'unknown')
                src_dataset = src.get('dataset', 'N/A')
                print(f"   - {src_name} ({src_kind}): {src_dataset}")
        
        # Check policies
        policies = cfg.get('policies', {})
        missing_policies = []
        for policy_name, policy_path in policies.items():
            if not os.path.exists(policy_path):
                missing_policies.append(f"{policy_name}: {policy_path}")
        
        if missing_policies:
            print(f"⚠️  Warning: Policy files not found:")
            for mp in missing_policies:
                print(f"   - {mp}")
        else:
            print(f"✅ All {len(policies)} policy files found")
        
        # Check execution mode
        exec_mode = cfg.get('execution', {}).get('mode', 'local')
        print(f"✅ Execution mode: {exec_mode}")
        
        if exec_mode == 'ray_data':
            print("   ⚠️  Note: Ray cluster should be running (ray start --head)")
        
        print()
        return True
        
    except Exception as e:
        print(f"❌ Error reading config: {e}\n")
        return False

def download_hf_dataset(dataset_name: str, local_dir: Optional[str] = None, repo_type: str = "dataset") -> bool:
    """Download HuggingFace dataset programmatically."""
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        print("⚠️  Warning: 'huggingface_hub' not installed. Cannot auto-download.")
        print("   Install with: pip install huggingface_hub")
        return False
    
    print(f"📥 Downloading HuggingFace dataset: {dataset_name}")
    try:
        download_path = snapshot_download(
            repo_id=dataset_name,
            repo_type=repo_type,
            local_dir=local_dir,
            resume_download=True,
        )
        print(f"✅ Dataset downloaded to: {download_path}\n")
        return True
    except Exception as e:
        print(f"❌ Error downloading dataset: {e}\n")
        return False

def verify_hf_dataset(dataset_name: str, split: str = "train", auto_download: bool = False) -> bool:
    """Verify HuggingFace dataset exists. Optionally download if not found."""
    print(f"🔍 Verifying HuggingFace dataset: {dataset_name}")
    try:
        from datasets import load_dataset
        ds = load_dataset(dataset_name, split=split, streaming=True)
        sample = next(iter(ds))
        print(f"✅ Dataset verified - Fields: {', '.join(sample.keys())[:5]}...\n")
        return True
    except Exception as e:
        if auto_download:
            print(f"⚠️  Dataset not found in cache. Attempting download...")
            if download_hf_dataset(dataset_name):
                # Try again after download
                try:
                    ds = load_dataset(dataset_name, split=split, streaming=True)
                    sample = next(iter(ds))
                    print(f"✅ Dataset verified after download - Fields: {', '.join(sample.keys())[:5]}...\n")
                    return True
                except Exception as e2:
                    print(f"⚠️  Warning: Could not verify after download: {e2}")
                    print("   Dataset may still work - will attempt to load during processing\n")
                    return False
            else:
                print(f"⚠️  Warning: Could not download dataset: {e}")
                print("   Dataset may still work - will attempt to load during processing\n")
                return False
        else:
            print(f"⚠️  Warning: Could not verify dataset: {e}")
            print("   Dataset may still work - will attempt to load during processing")
            print("   Tip: Use --auto-download to automatically download missing datasets\n")
            return False

def verify_sources(cfg: dict, auto_download: bool = False):
    """Verify all sources. Optionally download HuggingFace datasets if missing."""
    sources = cfg.get('sources', [])
    for src in sources:
        if src.get('kind') == 'hf_stream':
            dataset = src.get('dataset', '')
            split = src.get('split', 'train')
            if dataset:
                verify_hf_dataset(dataset, split, auto_download=auto_download)
        elif src.get('kind') == 'local_jsonl':
            dataset_path = src.get('dataset', '')
            if dataset_path and not os.path.exists(dataset_path):
                print(f"⚠️  Warning: Local file not found: {dataset_path}\n")

def run_pipeline(config_path: str, ray_config: Optional[str] = None) -> bool:
    """Run the pipeline."""
    safe_print("🚀 Starting pipeline...")
    print("=" * 60)
    
    try:
        import yaml
        
        # Load config to check mode
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
        
        exec_mode = cfg.get('execution', {}).get('mode', 'local').lower()
        
        # Import and run directly (more reliable than subprocess)
        from clean_corpus.pipeline.build import build_local, build_ray
        from clean_corpus.pipeline.ray_data_build import build_ray_data
        from clean_corpus.logging_ import setup_logging
        
        run_cfg = cfg.get('run', {})
        out_dir = run_cfg.get('out_dir', 'storage')
        run_id = run_cfg.get('run_id', 'run')
        setup_logging(out_dir=out_dir, run_id=run_id)
        
        # Store config path for manifest
        import os
        os.environ['CLEAN_CORPUS_CONFIG_PATH'] = os.path.abspath(config_path)
        
        # Run based on mode
        if exec_mode == 'ray_data':
            if not ray_config:
                ray_config = "configs/ray.yaml"
            if os.path.exists(ray_config):
                with open(ray_config, 'r', encoding='utf-8') as f:
                    ray_cfg = yaml.safe_load(f) or {}
            else:
                ray_cfg = {}
            build_ray_data(cfg, ray_cfg)
        elif exec_mode == 'ray':
            if not ray_config:
                ray_config = "configs/ray.yaml"
            if os.path.exists(ray_config):
                with open(ray_config, 'r', encoding='utf-8') as f:
                    ray_cfg = yaml.safe_load(f) or {}
            else:
                ray_cfg = {}
            build_ray(cfg, ray_cfg)
        else:
            build_local(cfg)
        
        print("\n" + "=" * 60)
        print("✅ Pipeline completed successfully!")
        return True
            
    except Exception as e:
        print(f"\n❌ Error running pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_manifest(out_dir: str) -> Optional[dict]:
    """Load run manifest."""
    import glob
    manifest_dir = os.path.join(out_dir, "manifests")
    if os.path.exists(manifest_dir):
        manifest_files = glob.glob(os.path.join(manifest_dir, "*.json"))
        if manifest_files:
            with open(manifest_files[0], 'r', encoding='utf-8') as f:
                return json.load(f)
    return None

def show_results(out_dir: str):
    """Show pipeline results."""
    print("\n" + "=" * 60)
    print("📊 Pipeline Results")
    print("=" * 60 + "\n")
    
    # Load manifest
    manifest_dir = os.path.join(out_dir, "manifests")
    if os.path.exists(manifest_dir):
        import glob
        manifest_files = glob.glob(os.path.join(manifest_dir, "*.json"))
        if manifest_files:
            with open(manifest_files[0], 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            run_id = manifest.get('run_id', 'unknown')
            
            print("Run Summary:")
            print(f"  Run ID: {run_id}")
            print(f"  Policy Version: {manifest.get('policy_version')}")
            print(f"  Written: {manifest.get('total_written_docs', 0):,} docs")
            print(f"  Rejected: {manifest.get('total_rejected_docs', 0):,} docs")
            
            total = manifest.get('total_written_docs', 0) + manifest.get('total_rejected_docs', 0)
            if total > 0:
                success_rate = (manifest.get('total_written_docs', 0) / total * 100)
                print(f"  Success Rate: {success_rate:.1f}%")
            
            print()
            
            # Show checkpoint info
            ckpt_path = os.path.join(out_dir, "checkpoints", f"{run_id}.json")
            if os.path.exists(ckpt_path):
                with open(ckpt_path, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                sources = checkpoint.get('sources', {})
                if sources:
                    print("Checkpoint Status:")
                    for src_name, src_info in sources.items():
                        processed = src_info.get('processed_docs', 0)
                        shards = src_info.get('shard_idx', 0)
                        print(f"  {src_name}: {processed:,} docs processed, {shards} shards written")
                    print()
                    print(f"💡 To see detailed checkpoint report:")
                    print(f"   python scripts/checkpoint_report.py {out_dir} {run_id}")
                    print()
    
    # Show outputs
    print("Output Locations:")
    if os.path.exists(os.path.join(out_dir, "docs")):
        import glob
        doc_dirs = [d for d in os.listdir(os.path.join(out_dir, "docs")) if os.path.isdir(os.path.join(out_dir, "docs", d))]
        for doc_dir in doc_dirs:
            shards = glob.glob(os.path.join(out_dir, "docs", doc_dir, "*.parquet"))
            print(f"  {doc_dir}: {len(shards)} shards")
    
    print(f"  Analytics: {out_dir}/analytics/")
    print(f"  Logs: {out_dir}/logs/")
    print(f"  Checkpoint: {out_dir}/checkpoints/")
    print()

def monitor_dashboard(out_dir: str, duration: Optional[float] = None):
    """Launch monitoring dashboard."""
    print("📈 Launching monitoring dashboard...")
    print("   (Press Ctrl+C in dashboard to return)\n")
    
    try:
        from clean_corpus.monitor.dashboard import create_dashboard
        if duration:
            # Run for specified duration
            import signal
            import threading
            
            def timeout_handler():
                time.sleep(duration)
                os._exit(0)
            
            thread = threading.Thread(target=timeout_handler, daemon=True)
            thread.start()
        
        create_dashboard(out_dir, refresh_interval=1.0)
    except KeyboardInterrupt:
        print("\n✅ Dashboard closed\n")
    except Exception as e:
        print(f"⚠️  Could not launch dashboard: {e}\n")
        print("   View results manually:")
        print(f"   - python scripts/view_analytics.py {out_dir}")
        print(f"   - python scripts/show_run_info.py {out_dir}")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="All-in-one pipeline runner for Clean Corpus Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python scripts/run_pipeline.py examples/build_local_jsonl.yaml
  
  # With real-time monitoring
  python scripts/run_pipeline.py examples/build_local_jsonl.yaml --monitor
  
  # Skip verification (faster startup)
  python scripts/run_pipeline.py examples/build_local_jsonl.yaml --no-verify
  
  # Auto-download missing HuggingFace datasets
  python scripts/run_pipeline.py examples/build_pg19_local.yaml --auto-download
  
  # Custom Ray config
  python scripts/run_pipeline.py configs/build.yaml --ray-config configs/ray.yaml
        """
    )
    
    parser.add_argument(
        "config",
        nargs="?",
        default="examples/build_local_jsonl.yaml",
        help="Path to config YAML file (default: examples/build_local_jsonl.yaml)"
    )
    parser.add_argument(
        "--ray-config",
        help="Path to Ray configuration file (default: configs/ray.yaml)"
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip configuration and dataset verification"
    )
    parser.add_argument(
        "--no-bootstrap",
        action="store_true",
        help="Skip PII detector bootstrap"
    )
    parser.add_argument(
        "--monitor",
        action="store_true",
        help="Launch monitoring dashboard after pipeline completes"
    )
    parser.add_argument(
        "--monitor-duration",
        type=float,
        help="Monitor for specified seconds then exit (use with --monitor)"
    )
    parser.add_argument(
        "--skip-results",
        action="store_true",
        help="Skip showing results summary"
    )
    parser.add_argument(
        "--auto-download",
        action="store_true",
        help="Automatically download missing HuggingFace datasets"
    )
    
    args = parser.parse_args()
    
    # Step 1: Bootstrap PII detectors
    if not args.no_bootstrap:
        bootstrap_pii()
    
    # Step 2: Verify configuration
    if not args.no_verify:
        if not verify_config(args.config):
            sys.exit(1)
        
        # Verify sources if HF datasets
        try:
            import yaml
            with open(args.config, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
            verify_sources(cfg, auto_download=args.auto_download)
        except:
            pass
    
    # Step 3: Run pipeline
    success = run_pipeline(args.config, args.ray_config)
    
    if not success:
        sys.exit(1)
    
    # Step 4: Show results
    if not args.skip_results:
        try:
            import yaml
            with open(args.config, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
            out_dir = cfg.get('run', {}).get('out_dir', 'storage')
            show_results(out_dir)
            
            # Generate checkpoint report
            try:
                manifest = load_manifest(out_dir)
                if manifest:
                    run_id = manifest.get('run_id')
                    print("💾 Generating checkpoint report...")
                    # Import checkpoint_report module
                    import importlib.util
                    checkpoint_report_path = os.path.join(os.path.dirname(__file__), "checkpoint_report.py")
                    if os.path.exists(checkpoint_report_path):
                        spec = importlib.util.spec_from_file_location("checkpoint_report", checkpoint_report_path)
                        checkpoint_report = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(checkpoint_report)
                        checkpoint_report.generate_report(out_dir, run_id)
            except Exception as e:
                # Silently fail - report generation is optional
                pass
        except:
            pass
    
    # Step 5: Launch monitoring (if requested)
    if args.monitor:
        try:
            import yaml
            with open(args.config, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
            out_dir = cfg.get('run', {}).get('out_dir', 'storage')
            monitor_dashboard(out_dir, args.monitor_duration)
        except:
            pass
    
    print("✅ All done!\n")

if __name__ == "__main__":
    main()
