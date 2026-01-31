"""Generate summary report after pipeline completion.

This is called automatically at the end of build_local() to create
a checkpoint-based summary report.
"""

from __future__ import annotations
import os
import json
import glob
from datetime import datetime
from typing import Dict, Any, Optional

def generate_summary_report(out_dir: str, run_id: str, manifest: Dict[str, Any], config: Optional[Dict[str, Any]] = None) -> str:
    """Generate summary report file."""
    report_path = os.path.join(out_dir, "reports", f"{run_id}_summary.txt")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    # Load checkpoint
    ckpt_path = os.path.join(out_dir, "checkpoints", f"{run_id}.json")
    checkpoint = None
    if os.path.exists(ckpt_path):
        with open(ckpt_path, 'r', encoding='utf-8') as f:
            checkpoint = json.load(f)
    
    lines = []
    lines.append("=" * 70)
    lines.append("CLEAN CORPUS PLATFORM - RUN SUMMARY REPORT")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"Run ID: {run_id}")
    lines.append(f"Policy Version: {manifest.get('policy_version', 'N/A')}")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # Source metadata section
    if config:
        sources_config = config.get('sources', [])
        if sources_config:
            lines.append("=" * 70)
            lines.append("SOURCE CONFIGURATION")
            lines.append("=" * 70)
            lines.append("")
            for src_cfg in sources_config:
                src_name = src_cfg.get('name', 'unnamed')
                src_kind = src_cfg.get('kind', 'unknown')
                dataset = src_cfg.get('dataset', 'N/A')
                
                lines.append(f"Source: {src_name}")
                lines.append(f"  Type: {src_kind}")
                lines.append(f"  Dataset/File: {dataset}")
                
                if src_cfg.get('kind') == 'hf_stream':
                    lines.append(f"  Split: {src_cfg.get('split', 'train')}")
                    lines.append(f"  Text Field: {src_cfg.get('text_field', 'text')}")
                elif src_cfg.get('kind') == 'local_jsonl':
                    if os.path.exists(dataset):
                        file_size = os.path.getsize(dataset)
                        lines.append(f"  File Size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
                
                lines.append(f"  License Field: {src_cfg.get('license_field', 'license')}")
                lines.append(f"  URL Field: {src_cfg.get('url_field', 'url')}")
                lines.append("")
    
    # Statistics
    lines.append("=" * 70)
    lines.append("STATISTICS")
    lines.append("=" * 70)
    lines.append("")
    total_written = manifest.get('total_written_docs', 0)
    total_rejected = manifest.get('total_rejected_docs', 0)
    total_processed = manifest.get('total_processed_docs', total_written + total_rejected)
    
    lines.append(f"Total Processed: {total_processed:,} documents")
    lines.append(f"Total Written: {total_written:,} documents")
    lines.append(f"Total Rejected: {total_rejected:,} documents")
    
    if total_processed == 0:
        lines.append("")
        lines.append("⚠️  WARNING: No documents were processed!")
        lines.append("   Possible reasons:")
        lines.append("   - Source file is empty or doesn't exist")
        lines.append("   - Source iterator yielded no documents")
        lines.append("   - All documents were filtered before processing")
        lines.append("   - Check logs for detailed error messages")
    elif total_processed > 0:
        success_rate = (total_written / total_processed * 100) if total_processed > 0 else 0
        lines.append(f"Success Rate: {success_rate:.1f}%")
    lines.append("")
    
    # Source progress
    if checkpoint:
        sources = checkpoint.get('sources', {})
        if sources:
            lines.append("=" * 70)
            lines.append("SOURCE PROGRESS")
            lines.append("=" * 70)
            lines.append("")
            
            for src_name, src_info in sources.items():
                processed = src_info.get('processed_docs', 0)
                shards = src_info.get('shard_idx', 0)
                
                # Count actual shards
                docs_dir = os.path.join(out_dir, "docs", f"source={src_name}")
                actual_shards = 0
                if os.path.exists(docs_dir):
                    actual_shards = len(glob.glob(os.path.join(docs_dir, "*.parquet")))
                
                lines.append(f"Source: {src_name}")
                lines.append(f"  Processed: {processed:,} documents")
                lines.append(f"  Shards Written: {shards}")
                lines.append(f"  Actual Shards: {actual_shards}")
                if shards != actual_shards:
                    lines.append(f"  ⚠️  Mismatch detected")
                lines.append("")
    
    # Output locations
    lines.append("=" * 70)
    lines.append("OUTPUT LOCATIONS")
    lines.append("=" * 70)
    lines.append("")
    outputs = manifest.get('outputs', {})
    for key, path in outputs.items():
        lines.append(f"{key}: {path}")
    lines.append("")
    
    # Resume instructions
    lines.append("=" * 70)
    lines.append("RESUME/RERUN INSTRUCTIONS")
    lines.append("=" * 70)
    lines.append("")
    lines.append("To resume this run:")
    lines.append(f"  - Use same config file")
    lines.append(f"  - Pipeline will auto-resume from checkpoint")
    lines.append(f"  - Checkpoint: {out_dir}/checkpoints/{run_id}.json")
    lines.append("")
    lines.append("To rerun from scratch:")
    lines.append(f"  - Delete checkpoint: {out_dir}/checkpoints/{run_id}.json")
    lines.append(f"  - (Optional) Delete outputs")
    lines.append(f"  - Run pipeline again")
    lines.append("")
    lines.append("For detailed checkpoint report:")
    lines.append(f"  python scripts/checkpoint_report.py {out_dir} {run_id}")
    lines.append("")
    
    report_text = "\n".join(lines)
    
    # Write report
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    return report_path
