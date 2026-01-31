"""Real-time terminal dashboard for Clean Corpus Platform.

Mainframe-style terminal UI showing:
- Live processing stats
- Stage-by-stage breakdown
- Rejection rates
- Throughput metrics
- Source information

Usage:
    python -m clean_corpus.monitor.dashboard [output_dir]
"""

from __future__ import annotations
import sys
import os
import time
import json
import glob
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.live import Live
    from rich.text import Text
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    from rich import box
except ImportError:
    print("Error: 'rich' library not installed.")
    print("Install with: pip install rich")
    sys.exit(1)

def load_manifest(out_dir: str) -> Optional[Dict[str, Any]]:
    """Load run manifest."""
    manifest_dir = os.path.join(out_dir, "manifests")
    if not os.path.exists(manifest_dir):
        return None
    
    manifest_files = glob.glob(os.path.join(manifest_dir, "*.json"))
    if not manifest_files:
        return None
    
    with open(manifest_files[0], 'r', encoding='utf-8') as f:
        return json.load(f)

def load_analytics(out_dir: str) -> Optional[Any]:
    """Load analytics aggregates."""
    try:
        import pandas as pd
        agg_path = os.path.join(out_dir, "analytics", "aggregates", "daily_aggregates.parquet")
        if os.path.exists(agg_path):
            return pd.read_parquet(agg_path)
    except ImportError:
        pass
    return None

def load_checkpoint(out_dir: str, run_id: str) -> Optional[Dict[str, Any]]:
    """Load checkpoint file."""
    ckpt_path = os.path.join(out_dir, "checkpoints", f"{run_id}.json")
    if os.path.exists(ckpt_path):
        with open(ckpt_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def get_processing_time_info(checkpoint: Dict[str, Any], manifest: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate processing time information."""
    info = {
        "start_time": None,
        "elapsed_seconds": 0,
        "last_update": None,
        "processing_rate": 0.0,
        "estimated_completion": None
    }
    
    if checkpoint:
        updated_ms = checkpoint.get('updated_at_ms')
        if updated_ms:
            info["last_update"] = datetime.fromtimestamp(updated_ms / 1000)
            # Try to get start time from manifest or checkpoint
            start_ms = manifest.get('start_time_ms') or checkpoint.get('start_time_ms')
            if start_ms:
                info["start_time"] = datetime.fromtimestamp(start_ms / 1000)
                elapsed = (updated_ms - start_ms) / 1000
                info["elapsed_seconds"] = elapsed
                
                # Calculate processing rate
                total_processed = sum(s.get('processed_docs', 0) for s in checkpoint.get('sources', {}).values())
                if elapsed > 0 and total_processed > 0:
                    info["processing_rate"] = total_processed / elapsed  # docs per second
    
    return info

def _format_dataset_display(dataset: Any, max_length: int = 50) -> str:
    """Format dataset for display, handling lists and long paths."""
    if isinstance(dataset, list):
        if len(dataset) == 0:
            return "[]"
        elif len(dataset) == 1:
            dataset_str = str(dataset[0])
        else:
            dataset_str = f"[{len(dataset)} files]"
        # Still truncate if needed
        if len(dataset_str) > max_length:
            return "..." + dataset_str[-max_length+3:]
        return dataset_str
    
    dataset_str = str(dataset)
    if len(dataset_str) > max_length:
        return "..." + dataset_str[-max_length+3:]
    return dataset_str

def _count_jsonl_lines(dataset: Any) -> int:
    """Count total lines across all JSONL files in dataset.
    
    Handles:
    - Single file (string)
    - Multiple files (list)
    - Directory (string path to directory)
    - Glob pattern (string with wildcards)
    """
    total_lines = 0
    
    # Handle list of files
    if isinstance(dataset, list):
        for file_path in dataset:
            if isinstance(file_path, str) and os.path.exists(file_path) and os.path.isfile(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        total_lines += sum(1 for line in f if line.strip())
                except:
                    pass
        return total_lines
    
    # Handle string path
    if not isinstance(dataset, str):
        return 0
    
    # Check if it's a glob pattern
    if '*' in dataset or '?' in dataset or '[' in dataset:
        matched_files = glob.glob(dataset, recursive=True)
        for file_path in matched_files:
            if os.path.isfile(file_path) and file_path.endswith('.jsonl'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        total_lines += sum(1 for line in f if line.strip())
                except:
                    pass
        return total_lines
    
    # Check if it's a directory
    if os.path.isdir(dataset):
        jsonl_files = list(Path(dataset).glob("*.jsonl"))
        jsonl_files.extend(Path(dataset).glob("**/*.jsonl"))  # Recursive
        for file_path in jsonl_files:
            if file_path.is_file():
                try:
                    with open(str(file_path), 'r', encoding='utf-8') as f:
                        total_lines += sum(1 for line in f if line.strip())
                except:
                    pass
        return total_lines
    
    # Single file
    if os.path.exists(dataset) and os.path.isfile(dataset):
        try:
            with open(dataset, 'r', encoding='utf-8') as f:
                total_lines = sum(1 for line in f if line.strip())
        except:
            pass
    
    return total_lines

def estimate_completion_time(config: Dict[str, Any], checkpoint: Dict[str, Any], 
                            processing_rate: float) -> Optional[str]:
    """Estimate time to complete remaining sources."""
    if not config or not checkpoint or processing_rate <= 0:
        return None
    
    sources_config = config.get('sources', [])
    checkpoint_sources = checkpoint.get('sources', {})
    
    total_remaining = 0
    for src_cfg in sources_config:
        src_name = src_cfg.get('name')
        src_info = checkpoint_sources.get(src_name, {})
        processed = src_info.get('processed_docs', 0)
        
        # Estimate total documents from source metadata
        if src_cfg.get('kind') == 'local_jsonl':
            dataset = src_cfg.get('dataset', '')
            total_lines = _count_jsonl_lines(dataset)
            if total_lines > 0:
                remaining = max(0, total_lines - processed)
                total_remaining += remaining
    
    if total_remaining > 0 and processing_rate > 0:
        seconds_remaining = total_remaining / processing_rate
        hours = int(seconds_remaining // 3600)
        minutes = int((seconds_remaining % 3600) // 60)
        seconds = int(seconds_remaining % 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    return None

def load_config_from_manifest(out_dir: str, manifest: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Try to find and load config file."""
    # Check if config path is stored in manifest
    config_path = manifest.get('config_path')
    if config_path and os.path.exists(config_path):
        try:
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except:
            pass
    
    # Try common config locations
    common_configs = [
        "configs/build.yaml",
        "examples/build_local_jsonl.yaml",
        "examples/build_common_pile.yaml",
    ]
    for config_path in common_configs:
        if os.path.exists(config_path):
            try:
                import yaml
                with open(config_path, 'r', encoding='utf-8') as f:
                    cfg = yaml.safe_load(f) or {}
                    # Check if sources match
                    cfg_sources = [s.get('name') for s in cfg.get('sources', [])]
                    manifest_sources = list(manifest.get('sources', {}).keys())
                    if set(cfg_sources) == set(manifest_sources):
                        return cfg
            except:
                pass
    return None

def get_source_file_info(config: Dict[str, Any], source_name: str) -> Dict[str, Any]:
    """Get file information for a source from config."""
    if not config:
        return {}
    
    sources = config.get('sources', [])
    for src_cfg in sources:
        if src_cfg.get('name') == source_name:
            info = {
                'kind': src_cfg.get('kind', 'unknown'),
                'dataset': src_cfg.get('dataset', 'N/A'),
            }
            
            if src_cfg.get('kind') == 'local_jsonl':
                dataset = src_cfg.get('dataset', '')
                # Handle multiple files
                if isinstance(dataset, list):
                    total_size = 0
                    total_lines = 0
                    for file_path in dataset:
                        if isinstance(file_path, str) and os.path.exists(file_path) and os.path.isfile(file_path):
                            try:
                                total_size += os.path.getsize(file_path)
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    total_lines += sum(1 for _ in f)
                            except:
                                pass
                    info['file_size'] = total_size
                    info['file_size_mb'] = total_size / 1024 / 1024
                    info['line_count'] = total_lines
                    info['file_count'] = len([f for f in dataset if isinstance(f, str) and os.path.exists(f)])
                elif isinstance(dataset, str):
                    # Handle single file, directory, or glob pattern
                    total_lines = _count_jsonl_lines(dataset)
                    if total_lines > 0:
                        # Try to get file size for single file
                        if os.path.exists(dataset) and os.path.isfile(dataset):
                            try:
                                file_size = os.path.getsize(dataset)
                                info['file_size'] = file_size
                                info['file_size_mb'] = file_size / 1024 / 1024
                            except:
                                pass
                        info['line_count'] = total_lines
            
            return info
    return {}

def get_output_files(out_dir: str, source_name: str) -> List[str]:
    """Get list of output files for a source."""
    files = []
    docs_dir = os.path.join(out_dir, "docs", f"source={source_name}")
    if os.path.exists(docs_dir):
        files.extend(glob.glob(os.path.join(docs_dir, "*.parquet")))
    return sorted(files)

def get_queue_status(config: Dict[str, Any], checkpoint: Dict[str, Any]) -> Dict[str, str]:
    """Determine queue status for each source."""
    if not config or not checkpoint:
        return {}
    
    status_map = {}
    sources = config.get('sources', [])
    checkpoint_sources = checkpoint.get('sources', {})
    
    for src_cfg in sources:
        src_name = src_cfg.get('name')
        if src_name in checkpoint_sources:
            processed = checkpoint_sources[src_name].get('processed_docs', 0)
            if processed > 0:
                status_map[src_name] = "processing"  # or "complete" if we can determine
            else:
                status_map[src_name] = "queued"
        else:
            status_map[src_name] = "queued"
    
    return status_map

def get_latest_log_lines(out_dir: str, run_id: str, lines: int = 10) -> List[str]:
    """Get latest lines from log file."""
    log_path = os.path.join(out_dir, "logs", f"{run_id}.log")
    if os.path.exists(log_path):
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                return all_lines[-lines:] if len(all_lines) > lines else all_lines
        except:
            pass
    return []

def create_dashboard(out_dir: str, refresh_interval: float = 5.0, use_simple_mode: bool = False, debug: bool = False):
    """Create and display real-time dashboard.
    
    Args:
        out_dir: Output directory to monitor
        refresh_interval: Refresh interval in seconds
        use_simple_mode: If True, use simple text output instead of Rich formatting
        debug: If True, print debug information
    """
    # Debug mode: print diagnostic information
    if debug:
        print("=== Monitor Debug Mode ===")
        print(f"Output directory: {out_dir}")
        print(f"Directory exists: {os.path.exists(out_dir)}")
        
        manifest_dir = os.path.join(out_dir, "manifests")
        print(f"Manifests directory: {manifest_dir}")
        print(f"Manifests exists: {os.path.exists(manifest_dir)}")
        
        if os.path.exists(manifest_dir):
            manifest_files = glob.glob(os.path.join(manifest_dir, "*.json"))
            print(f"Manifest files found: {len(manifest_files)}")
            for mf in manifest_files:
                print(f"  - {mf}")
        
        checkpoint_dir = os.path.join(out_dir, "checkpoints")
        print(f"Checkpoints directory: {checkpoint_dir}")
        print(f"Checkpoints exists: {os.path.exists(checkpoint_dir)}")
        
        if os.path.exists(checkpoint_dir):
            checkpoint_files = glob.glob(os.path.join(checkpoint_dir, "*.json"))
            print(f"Checkpoint files found: {len(checkpoint_files)}")
            for cf in checkpoint_files:
                print(f"  - {cf}")
        
        analytics_dir = os.path.join(out_dir, "analytics")
        print(f"Analytics directory: {analytics_dir}")
        print(f"Analytics exists: {os.path.exists(analytics_dir)}")
        
        print("\nTesting Rich console...")
        try:
            test_console = Console()
            test_console.print("[green]✓ Rich console works![/green]")
            test_console.print("[yellow]Testing colors and formatting...[/yellow]")
        except Exception as e:
            print(f"✗ Rich console error: {e}")
        
        print("\nStarting dashboard...\n")
    
    if use_simple_mode:
        # Simple text mode (fallback)
        _create_simple_dashboard(out_dir, refresh_interval)
        return
    
    try:
        console = Console(force_terminal=True)  # Force terminal output
    except Exception as e:
        print(f"Warning: Could not initialize Rich console: {e}")
        print("Falling back to simple text mode...\n")
        _create_simple_dashboard(out_dir, refresh_interval)
        return
    
    # Find latest run
    manifest = load_manifest(out_dir)
    if not manifest:
        console.print(f"[red]No run found in {out_dir}[/red]")
        console.print("Run a pipeline first: python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml")
        console.print(f"\n[dim]Looking for manifest in: {os.path.join(out_dir, 'manifests')}[/dim]")
        if os.path.exists(out_dir):
            manifest_dir = os.path.join(out_dir, "manifests")
            if os.path.exists(manifest_dir):
                files = os.listdir(manifest_dir)
                console.print(f"[dim]Files in manifests directory: {files}[/dim]")
        return
    
    run_id = manifest.get('run_id', 'unknown')
    
    # Print initial header (before clearing)
    console.print(f"\n[bold cyan]Clean Corpus Platform - Real-Time Dashboard[/bold cyan]")
    console.print(f"[dim]Monitoring: {out_dir} | Run ID: {run_id}[/dim]")
    console.print("[dim]READ-ONLY mode - Safe to start/stop anytime without affecting pipeline[/dim]")
    console.print("[dim]Press Ctrl+C to exit[/dim]\n")
    
    # Wait a moment so user can see initial message
    time.sleep(0.5)
    
    start_time = time.time()
    
    try:
        while True:
            # Reload data
            manifest = load_manifest(out_dir)
            checkpoint = load_checkpoint(out_dir, run_id)
            analytics_df = load_analytics(out_dir)
            log_lines = get_latest_log_lines(out_dir, run_id, 5)
            
            # Clear screen (with fallback for Windows)
            try:
                console.clear()
            except Exception:
                # Fallback: print newlines to simulate clearing
                console.print("\n" * 50)
            
            # Header - always print to ensure visibility
            elapsed = time.time() - start_time
            try:
                header = Panel(
                    f"[bold]Clean Corpus Platform[/bold] | Run: [cyan]{run_id}[/cyan] | Elapsed: [yellow]{elapsed:.0f}s[/yellow]",
                    style="bold blue",
                    box=box.DOUBLE
                )
                console.print(header)
            except Exception as e:
                # Fallback: simple text header
                console.print(f"\n{'='*70}")
                console.print(f"Clean Corpus Platform | Run: {run_id} | Elapsed: {elapsed:.0f}s")
                console.print(f"{'='*70}\n")
            console.print()
            
            # Main stats panel
            if manifest:
                total_written = manifest.get('total_written_docs', 0)
                total_rejected = manifest.get('total_rejected_docs', 0)
                total_processed = total_written + total_rejected
                
                stats_table = Table.grid(padding=1)
                stats_table.add_column(style="cyan", justify="right")
                stats_table.add_column(style="magenta", justify="right")
                stats_table.add_column(style="green", justify="right")
                stats_table.add_column(style="red", justify="right")
                
                stats_table.add_row(
                    "[bold]Total Processed:[/bold]",
                    f"[bold]{total_processed:,}[/bold]",
                    "[bold]Written:[/bold]",
                    f"[bold green]{total_written:,}[/bold green]"
                )
                stats_table.add_row(
                    "[bold]Rejected:[/bold]",
                    f"[bold red]{total_rejected:,}[/bold red]",
                    "[bold]Success Rate:[/bold]",
                    f"[bold]{((total_written / total_processed * 100) if total_processed > 0 else 0):.1f}%[/bold]"
                )
                
                console.print(Panel(stats_table, title="[bold]Pipeline Statistics[/bold]", border_style="cyan"))
                console.print()
            
            # Load config for source file info
            config = load_config_from_manifest(out_dir, manifest) if manifest else None
            queue_status = get_queue_status(config, checkpoint) if config and checkpoint else {}
            
            # Sources panel with file names
            if manifest and checkpoint:
                sources = manifest.get('sources', {})
                checkpoint_sources = checkpoint.get('sources', {})
                
                # Get all sources from config (including queued ones)
                all_source_names = set()
                if config:
                    all_source_names = {s.get('name') for s in config.get('sources', [])}
                all_source_names.update(sources.keys())
                all_source_names.update(checkpoint_sources.keys())
                
                if all_source_names:
                    sources_table = Table(title="[bold]Sources & Files[/bold]", box=box.ROUNDED, border_style="green")
                    sources_table.add_column("Source", style="cyan", no_wrap=True)
                    sources_table.add_column("File/Dataset", style="yellow", no_wrap=False, max_width=40)
                    sources_table.add_column("Processed", justify="right", style="magenta")
                    sources_table.add_column("Shards", justify="right", style="yellow")
                    sources_table.add_column("Status", style="green")
                    
                    for src_name in sorted(all_source_names):
                        src_info = checkpoint_sources.get(src_name, sources.get(src_name, {}))
                        processed = src_info.get('processed_docs', 0)
                        shards = src_info.get('shard_idx', 0)
                        
                        # Get file info from config
                        file_info = get_source_file_info(config, src_name) if config else {}
                        dataset_raw = file_info.get('dataset', 'N/A')
                        dataset = _format_dataset_display(dataset_raw, max_length=40)
                        
                        # Determine status
                        if src_name in queue_status:
                            if queue_status[src_name] == "queued":
                                status = "[yellow]⏳ Queued[/yellow]"
                            elif processed > 0:
                                # Check if complete (has output files)
                                output_files = get_output_files(out_dir, src_name)
                                if output_files and len(output_files) == shards:
                                    status = "[green]✓ Complete[/green]"
                                else:
                                    status = "[cyan]⚙ Processing[/cyan]"
                            else:
                                status = "[yellow]⏳ Queued[/yellow]"
                        elif processed > 0:
                            output_files = get_output_files(out_dir, src_name)
                            if output_files and len(output_files) == shards:
                                status = "[green]✓ Complete[/green]"
                            else:
                                status = "[cyan]⚙ Processing[/cyan]"
                        else:
                            status = "[yellow]⏳ Queued[/yellow]"
                        
                        sources_table.add_row(
                            src_name,
                            dataset,
                            f"{processed:,}",
                            f"{shards}",
                            status
                        )
                    
                    console.print(sources_table)
                    console.print()
                    
                    # Output files panel
                    if checkpoint_sources:
                        files_table = Table(title="[bold]Output Files[/bold]", box=box.ROUNDED, border_style="blue")
                        files_table.add_column("Source", style="cyan", no_wrap=True)
                        files_table.add_column("Output Files", style="green", no_wrap=False)
                        files_table.add_column("Size", justify="right", style="yellow")
                        
                        for src_name in sorted(checkpoint_sources.keys()):
                            output_files = get_output_files(out_dir, src_name)
                            if output_files:
                                file_names = [os.path.basename(f) for f in output_files[-5:]]  # Show last 5
                                if len(output_files) > 5:
                                    file_list = ", ".join(file_names) + f" ... (+{len(output_files)-5} more)"
                                else:
                                    file_list = ", ".join(file_names)
                                
                                total_size = sum(os.path.getsize(f) for f in output_files)
                                files_table.add_row(
                                    src_name,
                                    file_list,
                                    f"{total_size / 1024 / 1024:.2f} MB"
                                )
                        
                        if files_table.rows:
                            console.print(files_table)
                            console.print()
            
            # Analytics by stage
            if analytics_df is not None and not analytics_df.empty:
                stage_table = Table(title="[bold]Stage Analytics[/bold]", box=box.ROUNDED, border_style="yellow")
                stage_table.add_column("Stage", style="cyan", no_wrap=True)
                stage_table.add_column("Input", justify="right", style="magenta")
                stage_table.add_column("Accepted", justify="right", style="green")
                stage_table.add_column("Rejected", justify="right", style="red")
                stage_table.add_column("Rate", justify="right", style="yellow")
                
                # Group by stage
                if 'stage' in analytics_df.columns:
                    stage_summary = analytics_df.groupby('stage').agg({
                        'input_docs': 'sum',
                        'accepted_docs': 'sum',
                        'rejected_docs': 'sum'
                    }).reset_index()
                    
                    for _, row in stage_summary.iterrows():
                        stage_name = row['stage']
                        input_docs = int(row['input_docs'])
                        accepted = int(row['accepted_docs'])
                        rejected = int(row['rejected_docs'])
                        rate = (rejected / input_docs * 100) if input_docs > 0 else 0.0
                        
                        stage_table.add_row(
                            stage_name,
                            f"{input_docs:,}",
                            f"[green]{accepted:,}[/green]",
                            f"[red]{rejected:,}[/red]",
                            f"{rate:.1f}%"
                        )
                    
                    console.print(stage_table)
                    console.print()
            
            # Integrated analytics summary (from view_analytics.py)
            if analytics_df is not None and not analytics_df.empty:
                try:
                    import pandas as pd
                    # Rejection breakdown
                    if 'rejected_docs' in analytics_df.columns and 'stage' in analytics_df.columns:
                        rejection_summary = analytics_df.groupby('stage')['rejected_docs'].sum()
                        if rejection_summary.sum() > 0:
                            rejection_table = Table(title="[bold]Rejections by Stage[/bold]", box=box.SIMPLE, border_style="red")
                            rejection_table.add_column("Stage", style="cyan")
                            rejection_table.add_column("Rejected", justify="right", style="red")
                            for stage, count in rejection_summary.items():
                                if count > 0:
                                    rejection_table.add_row(stage, f"{int(count):,}")
                            console.print(rejection_table)
                            console.print()
                except:
                    pass
            
            # Recent log activity
            if log_lines:
                log_text = Text()
                log_text.append("[bold]Recent Activity:[/bold]\n", style="cyan")
                for line in log_lines[-5:]:
                    line = line.strip()
                    if line:
                        if "ERROR" in line:
                            log_text.append(f"  {line}\n", style="red")
                        elif "INFO" in line:
                            log_text.append(f"  {line}\n", style="green")
                        else:
                            log_text.append(f"  {line}\n", style="dim")
                
                console.print(Panel(log_text, title="[bold]Logs[/bold]", border_style="blue"))
                console.print()
            
            # Footer
            footer = Panel(
                f"[dim]Refreshing every {refresh_interval}s | Last update: {datetime.now().strftime('%H:%M:%S')} | Press Ctrl+C to exit[/dim]",
                style="dim",
                box=box.SIMPLE
            )
            console.print(footer)
            
            time.sleep(refresh_interval)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Dashboard stopped.[/yellow]\n")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]\n")
        import traceback
        # Print error details
        error_details = traceback.format_exc()
        console.print(f"[dim]{error_details}[/dim]")
        # Also print to stderr for debugging
        import sys
        print(f"Monitor error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

def _create_simple_dashboard(out_dir: str, refresh_interval: float = 5.0):
    """Simple text-based dashboard (fallback when Rich is not available)."""
    import sys
    
    # Force output to be flushed immediately
    sys.stdout.reconfigure(encoding='utf-8', errors='replace') if hasattr(sys.stdout, 'reconfigure') else None
    
    manifest = load_manifest(out_dir)
    if not manifest:
        print(f"Error: No run found in {out_dir}", flush=True)
        print("Run a pipeline first: python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml", flush=True)
        manifest_dir = os.path.join(out_dir, "manifests")
        if os.path.exists(manifest_dir):
            files = os.listdir(manifest_dir)
            print(f"Files in manifests directory: {files}", flush=True)
        return
    
    run_id = manifest.get('run_id', 'unknown')
    start_time = time.time()
    
    print(f"\n{'='*70}", flush=True)
    print(f"Clean Corpus Platform - Real-Time Dashboard (Simple Mode)", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"Monitoring: {out_dir}", flush=True)
    print(f"Run ID: {run_id}", flush=True)
    print(f"Press Ctrl+C to exit\n", flush=True)
    
    try:
        while True:
            manifest = load_manifest(out_dir)
            checkpoint = load_checkpoint(out_dir, run_id)
            
            # Clear screen (simple) - use os.system for Windows
            if sys.platform == 'win32':
                os.system('cls')
            else:
                os.system('clear')
            
            print(f"{'='*70}", flush=True)
            print(f"Run: {run_id} | Elapsed: {time.time() - start_time:.0f}s", flush=True)
            print(f"{'='*70}\n", flush=True)
            
            if manifest:
                total_written = manifest.get('total_written_docs', 0)
                total_rejected = manifest.get('total_rejected_docs', 0)
                total_processed = total_written + total_rejected
                success_rate = (total_written / total_processed * 100) if total_processed > 0 else 0
                
                print(f"Total Processed: {total_processed:,}", flush=True)
                print(f"Written: {total_written:,}", flush=True)
                print(f"Rejected: {total_rejected:,}", flush=True)
                print(f"Success Rate: {success_rate:.1f}%\n", flush=True)
                
                if checkpoint:
                    sources = checkpoint.get('sources', {})
                    if sources:
                        print("Sources:", flush=True)
                        for src_name, src_info in sources.items():
                            processed = src_info.get('processed_docs', 0)
                            shards = src_info.get('shard_idx', 0)
                            written = src_info.get('written_docs', 0)
                            rejected = src_info.get('rejected_docs', 0)
                            print(f"  {src_name}: {processed:,} processed, {written:,} written, {rejected:,} rejected, {shards} shards", flush=True)
                        print(flush=True)
            
            print(f"Last update: {datetime.now().strftime('%H:%M:%S')}", flush=True)
            print(f"Refreshing every {refresh_interval}s...", flush=True)
            print(f"{'='*70}", flush=True)
            
            time.sleep(refresh_interval)
            
    except KeyboardInterrupt:
        print("\n\nDashboard stopped.\n", flush=True)
    except Exception as e:
        print(f"\nError: {e}\n", flush=True)
        import traceback
        traceback.print_exc()

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Real-time dashboard for Clean Corpus Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s storage_example
  %(prog)s storage_example --refresh 10.0
  %(prog)s storage_example -r 2.5
  %(prog)s storage_example --debug
  %(prog)s storage_example --simple
        """
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="storage_example",
        help="Output directory to monitor (default: storage_example)"
    )
    parser.add_argument(
        "--refresh", "-r",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="Refresh interval in seconds (default: 5.0)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Use simple text mode (no Rich formatting)"
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        print(f"Error: Directory not found: {args.output_dir}")
        print("Run a pipeline first: python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml")
        sys.exit(1)
    
    if args.debug:
        # Debug mode: print diagnostic information
        print("=== Monitor Debug Mode ===")
        print(f"Output directory: {args.output_dir}")
        print(f"Directory exists: {os.path.exists(args.output_dir)}")
        
        manifest_dir = os.path.join(args.output_dir, "manifests")
        print(f"Manifests directory: {manifest_dir}")
        print(f"Manifests exists: {os.path.exists(manifest_dir)}")
        
        if os.path.exists(manifest_dir):
            manifest_files = glob.glob(os.path.join(manifest_dir, "*.json"))
            print(f"Manifest files found: {len(manifest_files)}")
            for mf in manifest_files:
                print(f"  - {mf}")
        
        checkpoint_dir = os.path.join(args.output_dir, "checkpoints")
        print(f"Checkpoints directory: {checkpoint_dir}")
        print(f"Checkpoints exists: {os.path.exists(checkpoint_dir)}")
        
        if os.path.exists(checkpoint_dir):
            checkpoint_files = glob.glob(os.path.join(checkpoint_dir, "*.json"))
            print(f"Checkpoint files found: {len(checkpoint_files)}")
            for cf in checkpoint_files:
                print(f"  - {cf}")
        
        analytics_dir = os.path.join(args.output_dir, "analytics")
        print(f"Analytics directory: {analytics_dir}")
        print(f"Analytics exists: {os.path.exists(analytics_dir)}")
        
        print("\nTesting Rich console...")
        try:
            test_console = Console()
            test_console.print("[green]✓ Rich console works![/green]")
            test_console.print("[yellow]Testing colors and formatting...[/yellow]")
        except Exception as e:
            print(f"✗ Rich console error: {e}")
        
        print("\nStarting dashboard...\n")
    
    create_dashboard(args.output_dir, args.refresh, use_simple_mode=args.simple, debug=args.debug)

if __name__ == "__main__":
    main()
