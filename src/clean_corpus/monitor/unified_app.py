"""Unified Monitor + Analytics Application.

Single application with two screens:
1. Monitor Screen - Real-time pipeline monitoring
2. Analytics Screen - Detailed analytics and reports

Switch between screens with:
- 'm' or '1' for Monitor
- 'a' or '2' for Analytics
- 'q' to quit
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
    from rich.prompt import Prompt
except ImportError:
    print("Error: 'rich' library not installed.")
    print("Install with: pip install rich")
    sys.exit(1)

# Import functions from dashboard and analytics
from .dashboard import (
    load_manifest, load_checkpoint, load_config_from_manifest,
    get_source_file_info, get_output_files, get_queue_status,
    get_latest_log_lines, load_analytics
)

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
    """Format dataset for display, handling lists and long paths.
    
    Returns a string suitable for display in tables/UI.
    """
    # Handle None
    if dataset is None:
        return "N/A"
    
    # Handle list of files
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
    
    # Convert to string and handle long paths
    dataset_str = str(dataset) if dataset else "N/A"
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
        import glob
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
        from pathlib import Path
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

def render_monitor_screen(console: Console, out_dir: str, manifest: Dict[str, Any], checkpoint: Dict[str, Any], 
                         analytics_df, log_lines: List[str], elapsed: float, refresh_interval: float):
    """Render the real-time monitoring screen."""
    try:
        layout = []
        
        # Main stats panel
        if not manifest:
            layout.append(Panel("[red]No manifest data available[/red]", border_style="red"))
        else:
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
            
            layout.append(Panel(stats_table, title="[bold]Pipeline Statistics[/bold]", border_style="cyan"))
            layout.append("")
            
            # Load config for source file info
            config = load_config_from_manifest(out_dir, manifest) if manifest else None
            queue_status = get_queue_status(config, checkpoint) if config and checkpoint else {}
            
            # Get processing time info
            time_info = get_processing_time_info(checkpoint, manifest) if checkpoint else {}
            processing_rate = time_info.get('processing_rate', 0.0)
            estimated_completion = estimate_completion_time(config, checkpoint, processing_rate) if config else None
            
            # Sources panel with file names
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
                    
                    # Calculate progress percentage and get dataset from config
                    progress_pct = ""
                    if config:
                        src_cfg = next((s for s in config.get('sources', []) if s.get('name') == src_name), None)
                        if src_cfg:
                            # Get dataset from config (may be list, string, etc.)
                            dataset_raw = src_cfg.get('dataset', 'N/A')
                            if src_cfg.get('kind') == 'local_jsonl':
                                total_lines = _count_jsonl_lines(dataset_raw)
                                if total_lines > 0:
                                    pct = (processed / total_lines * 100)
                                    progress_pct = f"{pct:.1f}%"
                    
                    # Format dataset for display (handles lists, strings, etc.)
                    dataset = _format_dataset_display(dataset_raw, max_length=35)
                    
                    # Determine status
                    if src_name in queue_status:
                        if queue_status[src_name] == "queued":
                            status = "[yellow]⏳ Queued[/yellow]"
                        elif processed > 0:
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
                        status,
                        progress_pct or "N/A"
                    )
                
                layout.append(sources_table)
                layout.append("")
                
                # Per-file statistics table
                file_stats = src_info.get('file_stats', {})
                if file_stats:
                    file_stats_table = Table(title="[bold]Per-File Statistics[/bold]", box=box.ROUNDED, border_style="cyan")
                    file_stats_table.add_column("File", style="yellow", no_wrap=False, max_width=50)
                    file_stats_table.add_column("Processed", justify="right", style="magenta")
                    file_stats_table.add_column("Written", justify="right", style="green")
                    file_stats_table.add_column("Rejected", justify="right", style="red")
                    file_stats_table.add_column("Success Rate", justify="right", style="yellow")
                    
                    for file_path, stats in sorted(file_stats.items()):
                        file_processed = stats.get('processed', 0)
                        file_written = stats.get('written', 0)
                        file_rejected = stats.get('rejected', 0)
                        success_rate = (file_written / file_processed * 100) if file_processed > 0 else 0.0
                        
                        # Format file path for display
                        display_path = _format_dataset_display(file_path, max_length=45)
                        
                        file_stats_table.add_row(
                            display_path,
                            f"{file_processed:,}",
                            f"[green]{file_written:,}[/green]",
                            f"[red]{file_rejected:,}[/red]",
                            f"{success_rate:.1f}%"
                        )
                    
                    layout.append(file_stats_table)
                    layout.append("")
            
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
                    import pandas as pd
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
                    
                    layout.append(stage_table)
                    layout.append("")
            
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
                
                layout.append(Panel(log_text, title="[bold]Logs[/bold]", border_style="blue"))
                layout.append("")
            
            # Processing time and estimates panel
            if checkpoint and time_info:
                time_table = Table.grid(padding=1)
                time_table.add_column(style="cyan", justify="right")
                time_table.add_column(style="yellow", justify="left")
                
                if time_info.get('start_time'):
                    time_table.add_row("[bold]Start Time:[/bold]", time_info['start_time'].strftime('%Y-%m-%d %H:%M:%S'))
                if time_info.get('last_update'):
                    time_table.add_row("[bold]Last Update:[/bold]", time_info['last_update'].strftime('%Y-%m-%d %H:%M:%S'))
                
                elapsed_sec = time_info.get('elapsed_seconds', elapsed)
                hours = int(elapsed_sec // 3600)
                minutes = int((elapsed_sec % 3600) // 60)
                seconds = int(elapsed_sec % 60)
                elapsed_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}" if hours > 0 else f"{minutes:02d}:{seconds:02d}"
                time_table.add_row("[bold]Elapsed Time:[/bold]", elapsed_str)
                
                if processing_rate > 0:
                    time_table.add_row("[bold]Processing Rate:[/bold]", f"{processing_rate:.1f} docs/sec")
                
                if estimated_completion:
                    time_table.add_row("[bold]Est. Time Remaining:[/bold]", f"[green]{estimated_completion}[/green]")
                
                layout.append(Panel(time_table, title="[bold]Processing Time[/bold]", border_style="yellow"))
                layout.append("")
            
            # Output directories and metadata location
            if manifest:
                outputs = manifest.get('outputs', {})
                if outputs:
                    paths_table = Table.grid(padding=1)
                    paths_table.add_column(style="cyan", justify="right")
                    paths_table.add_column(style="green", justify="left")
                    
                    paths_table.add_row("[bold]Output Directory:[/bold]", out_dir)
                    if outputs.get('docs_dir'):
                        paths_table.add_row("[bold]Documents:[/bold]", outputs['docs_dir'])
                    if outputs.get('metadata_dir'):
                        paths_table.add_row("[bold]Metadata:[/bold]", outputs['metadata_dir'])
                    if outputs.get('analytics_events'):
                        paths_table.add_row("[bold]Analytics:[/bold]", outputs['analytics_events'])
                    if outputs.get('checkpoint'):
                        paths_table.add_row("[bold]Checkpoint:[/bold]", outputs['checkpoint'])
                    
                    layout.append(Panel(paths_table, title="[bold]Output Locations[/bold]", border_style="blue"))
                    layout.append("")
            
            # Queue status
            if config and queue_status:
                queued_sources = [name for name, status in queue_status.items() if status == "queued"]
                if queued_sources:
                    queue_text = Text()
                    queue_text.append(f"[bold]Queued Sources ({len(queued_sources)}):[/bold]\n", style="yellow")
                    for q_name in queued_sources:
                        file_info = get_source_file_info(config, q_name) if config else {}
                        dataset_raw = file_info.get('dataset', 'N/A')
                        dataset = _format_dataset_display(dataset_raw, max_length=50)
                        queue_text.append(f"  • {q_name}: {dataset}\n", style="dim")
                    layout.append(Panel(queue_text, title="[bold]Queue Status[/bold]", border_style="yellow"))
                    layout.append("")
            
            # Note: Footer is now rendered in main loop, so we don't duplicate it here
            
            # Render all items directly to console
            if layout:
                for item in layout:
                    try:
                        console.print(item)
                    except Exception as item_error:
                        # If individual item fails, try to print as string
                        console.print(f"[yellow]Warning: Could not render item: {item_error}[/yellow]")
                        console.print(str(item)[:100] + "..." if len(str(item)) > 100 else str(item))
            else:
                # Fallback: show basic info if layout is empty
                console.print("[yellow]No data to display[/yellow]")
                if manifest:
                    console.print(f"Run ID: {manifest.get('run_id', 'unknown')}")
                    console.print(f"Written: {manifest.get('total_written_docs', 0):,}")
                    console.print(f"Rejected: {manifest.get('total_rejected_docs', 0):,}")
    except Exception as e:
        console.print(f"[red]Error in render_monitor_screen: {e}[/red]")
        import traceback
        console.print(traceback.format_exc())

def render_analytics_screen(console: Console, out_dir: str, manifest: Dict[str, Any], analytics_df):
    """Render the detailed analytics screen."""
    try:
        layout = []
        
        # Load checkpoint for per-file stats
        run_id = manifest.get('run_id', 'unknown') if manifest else 'unknown'
        checkpoint = load_checkpoint(out_dir, run_id)
        config = load_config_from_manifest(out_dir, manifest) if manifest else None
        
        # Run Summary
        if not manifest:
            layout.append(Panel("[red]No manifest data available[/red]", border_style="red"))
        else:
            total_written = manifest.get('total_written_docs', 0)
            total_rejected = manifest.get('total_rejected_docs', 0)
            total_processed = total_written + total_rejected
            
            summary_table = Table(title="[bold]Run Summary[/bold]", box=box.ROUNDED, border_style="cyan")
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", justify="right", style="magenta")
            summary_table.add_column("Percentage", justify="right", style="yellow")
            
            summary_table.add_row("Total Processed", f"{total_processed:,}", "100.0%")
            summary_table.add_row("[green]Written[/green]", f"[green]{total_written:,}[/green]", 
                                  f"{(total_written / total_processed * 100) if total_processed > 0 else 0:.1f}%")
            summary_table.add_row("[red]Rejected[/red]", f"[red]{total_rejected:,}[/red]",
                                  f"{(total_rejected / total_processed * 100) if total_processed > 0 else 0:.1f}%")
            
            layout.append(summary_table)
            layout.append("")
            
            # Input Files and Output Paths Section
            paths_info = []
            
            # Input files from config
            if config:
                sources_config = config.get('sources', [])
                if sources_config:
                    input_files_section = []
                    for src_cfg in sources_config:
                        src_name = src_cfg.get('name', 'unknown')
                        dataset = src_cfg.get('dataset', 'N/A')
                        
                        # Format dataset display
                        if isinstance(dataset, list):
                            dataset_display = f"[{len(dataset)} files]"
                            input_files_section.append(f"[bold]{src_name}:[/bold] {dataset_display}")
                            # Show individual files if not too many
                            if len(dataset) <= 5:
                                for f in dataset:
                                    input_files_section.append(f"  • {_format_dataset_display(f, max_length=60)}")
                            else:
                                for f in dataset[:3]:
                                    input_files_section.append(f"  • {_format_dataset_display(f, max_length=60)}")
                                input_files_section.append(f"  ... and {len(dataset) - 3} more files")
                        else:
                            dataset_display = _format_dataset_display(dataset, max_length=60)
                            input_files_section.append(f"[bold]{src_name}:[/bold] {dataset_display}")
                    
                    if input_files_section:
                        paths_info.append(("\n".join(input_files_section), "Input Files"))
            
            # Output paths from manifest
            outputs = manifest.get('outputs', {})
            if outputs:
                output_paths_section = []
                
                # Output directory
                output_paths_section.append(f"[bold]Output Directory:[/bold] {out_dir}")
                
                # Documents directory
                if outputs.get('docs_dir'):
                    docs_path = outputs['docs_dir']
                    output_paths_section.append(f"[bold]Documents:[/bold] {docs_path}")
                    # Try to get actual file count
                    try:
                        parquet_files = glob.glob(os.path.join(docs_path, "**", "*.parquet"), recursive=True)
                        if parquet_files:
                            output_paths_section.append(f"  └─ [green]{len(parquet_files)} Parquet file(s)[/green]")
                    except:
                        pass
                
                # Metadata directory
                if outputs.get('metadata_dir'):
                    metadata_path = outputs['metadata_dir']
                    output_paths_section.append(f"[bold]Metadata:[/bold] {metadata_path}")
                    # Try to get actual file count
                    try:
                        meta_files = glob.glob(os.path.join(metadata_path, "**", "*.parquet"), recursive=True)
                        if meta_files:
                            output_paths_section.append(f"  └─ [green]{len(meta_files)} Parquet file(s)[/green]")
                    except:
                        pass
                
                # Rejections file
                if outputs.get('rejections'):
                    rej_path = outputs['rejections']
                    output_paths_section.append(f"[bold]Rejections:[/bold] {rej_path}")
                    if os.path.exists(rej_path):
                        try:
                            with open(rej_path, 'r', encoding='utf-8') as f:
                                line_count = sum(1 for _ in f if _.strip())
                            output_paths_section.append(f"  └─ {line_count:,} rejection record(s)")
                        except:
                            pass
                
                # Analytics
                if outputs.get('analytics_events'):
                    analytics_path = outputs['analytics_events']
                    output_paths_section.append(f"[bold]Analytics Events:[/bold] {analytics_path}")
                
                if outputs.get('analytics_aggregates'):
                    agg_path = outputs['analytics_aggregates']
                    output_paths_section.append(f"[bold]Analytics Aggregates:[/bold] {agg_path}")
                
                # Checkpoint
                if outputs.get('checkpoint'):
                    ckpt_path = outputs['checkpoint']
                    output_paths_section.append(f"[bold]Checkpoint:[/bold] {ckpt_path}")
                    if os.path.exists(ckpt_path):
                        output_paths_section.append(f"  └─ [green]✓ Exists[/green]")
                
                if output_paths_section:
                    paths_info.append(("\n".join(output_paths_section), "Output Paths"))
            
            # Display paths information
            if paths_info:
                for paths_text, title in paths_info:
                    layout.append(Panel(paths_text, title=f"[bold]{title}[/bold]", border_style="blue"))
                    layout.append("")
            
            # Detailed Stage Analytics
            if analytics_df is not None and not analytics_df.empty:
                import pandas as pd
                
                # Stage breakdown
                if 'stage' in analytics_df.columns:
                    stage_table = Table(title="[bold]Stage-by-Stage Breakdown[/bold]", box=box.ROUNDED, border_style="yellow")
                    stage_table.add_column("Stage", style="cyan", no_wrap=True)
                    stage_table.add_column("Input", justify="right", style="magenta")
                    stage_table.add_column("Accepted", justify="right", style="green")
                    stage_table.add_column("Rejected", justify="right", style="red")
                    stage_table.add_column("Reject Rate", justify="right", style="yellow")
                    stage_table.add_column("Accept Rate", justify="right", style="green")
                    
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
                        reject_rate = (rejected / input_docs * 100) if input_docs > 0 else 0.0
                        accept_rate = (accepted / input_docs * 100) if input_docs > 0 else 0.0
                        
                        stage_table.add_row(
                            stage_name,
                            f"{input_docs:,}",
                            f"[green]{accepted:,}[/green]",
                            f"[red]{rejected:,}[/red]",
                            f"{reject_rate:.1f}%",
                            f"{accept_rate:.1f}%"
                        )
                    
                    layout.append(stage_table)
                    layout.append("")
                
                # Rejection breakdown
                if 'rejected_docs' in analytics_df.columns and 'stage' in analytics_df.columns:
                    rejection_summary = analytics_df.groupby('stage')['rejected_docs'].sum()
                    if rejection_summary.sum() > 0:
                        rejection_table = Table(title="[bold]Rejections by Stage[/bold]", box=box.SIMPLE, border_style="red")
                        rejection_table.add_column("Stage", style="cyan")
                        rejection_table.add_column("Rejected", justify="right", style="red")
                        rejection_table.add_column("Percentage", justify="right", style="yellow")
                        
                        total_rej = rejection_summary.sum()
                        for stage, count in rejection_summary.items():
                            if count > 0:
                                pct = (count / total_rej * 100) if total_rej > 0 else 0
                                rejection_table.add_row(stage, f"{int(count):,}", f"{pct:.1f}%")
                        
                        layout.append(rejection_table)
                        layout.append("")
            
            # Per-File Analytics Summary (from checkpoint)
            if checkpoint:
                checkpoint_sources = checkpoint.get('sources', {})
                all_file_stats = {}
                
                # Aggregate file stats from all sources
                for src_name, src_info in checkpoint_sources.items():
                    file_stats = src_info.get('file_stats', {})
                    if not file_stats:
                        # Debug: log if file_stats is empty
                        continue
                    
                    # Normalize file paths to ensure consistent keys
                    for file_path, stats in file_stats.items():
                        # Normalize path separators for consistent comparison
                        normalized_path = file_path.replace('\\', '/') if file_path else "unknown"
                        
                        if normalized_path not in all_file_stats:
                            all_file_stats[normalized_path] = {
                                'processed': 0,
                                'written': 0,
                                'rejected': 0,
                                'sources': []
                            }
                        all_file_stats[normalized_path]['processed'] += stats.get('processed', 0)
                        all_file_stats[normalized_path]['written'] += stats.get('written', 0)
                        all_file_stats[normalized_path]['rejected'] += stats.get('rejected', 0)
                        if src_name not in all_file_stats[normalized_path]['sources']:
                            all_file_stats[normalized_path]['sources'].append(src_name)
                
                if all_file_stats:
                    file_analytics_table = Table(
                        title=f"[bold]Per-File Analytics Summary ({len(all_file_stats)} file(s))[/bold]",
                        box=box.ROUNDED,
                        border_style="cyan"
                    )
                    file_analytics_table.add_column("File", style="yellow", no_wrap=False, max_width=50)
                    file_analytics_table.add_column("Source", style="cyan")
                    file_analytics_table.add_column("Processed", justify="right", style="magenta")
                    file_analytics_table.add_column("Written", justify="right", style="green")
                    file_analytics_table.add_column("Rejected", justify="right", style="red")
                    file_analytics_table.add_column("Success Rate", justify="right", style="yellow")
                    
                    # Sort by file path for consistent display
                    for file_path, stats in sorted(all_file_stats.items()):
                        file_processed = stats['processed']
                        file_written = stats['written']
                        file_rejected = stats['rejected']
                        success_rate = (file_written / file_processed * 100) if file_processed > 0 else 0.0
                        sources_str = ", ".join(stats['sources'])
                        
                        display_path = _format_dataset_display(file_path, max_length=45)
                        
                        file_analytics_table.add_row(
                            display_path,
                            sources_str,
                            f"{file_processed:,}",
                            f"[green]{file_written:,}[/green]",
                            f"[red]{file_rejected:,}[/red]",
                            f"{success_rate:.1f}%"
                        )
                    
                    layout.append(file_analytics_table)
                    layout.append("")
                else:
                    # Show debug info if no file stats found
                    layout.append(Panel(
                        "[yellow]No per-file statistics found in checkpoint[/yellow]\n"
                        f"[dim]Checkpoint sources: {list(checkpoint_sources.keys())}[/dim]",
                        title="[bold]Per-File Analytics Summary[/bold]",
                        border_style="yellow"
                    ))
                    layout.append("")
            
            # Rejections file summary
            rej_path = os.path.join(out_dir, "rejections", "rejections.jsonl")
            if os.path.exists(rej_path):
                try:
                    import pandas as pd
                    rejs = []
                    with open(rej_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                rejs.append(json.loads(line))
                    
                    if rejs:
                        df_rej = pd.DataFrame(rejs)
                        
                        # By stage
                        stage_counts = df_rej['stage'].value_counts()
                        stage_table = Table(title="[bold]Rejections by Stage[/bold]", box=box.SIMPLE, border_style="red")
                        stage_table.add_column("Stage", style="cyan")
                        stage_table.add_column("Count", justify="right", style="red")
                        for stage, count in stage_counts.items():
                            stage_table.add_row(stage, f"{count:,}")
                        layout.append(stage_table)
                        layout.append("")
                        
                        # By reason
                        if 'reason_code' in df_rej.columns:
                            reason_counts = df_rej['reason_code'].value_counts()
                            reason_table = Table(title="[bold]Rejections by Reason[/bold]", box=box.SIMPLE, border_style="red")
                            reason_table.add_column("Reason", style="cyan")
                            reason_table.add_column("Count", justify="right", style="red")
                            for reason, count in reason_counts.items():
                                reason_table.add_row(str(reason), f"{count:,}")
                            layout.append(reason_table)
                            layout.append("")
                        
                        # By source_file (per-file rejections)
                        if 'source_file' in df_rej.columns:
                            file_rej_counts = df_rej['source_file'].value_counts()
                            if len(file_rej_counts) > 0:
                                file_rej_table = Table(
                                    title=f"[bold]Rejections by File ({len(file_rej_counts)} file(s))[/bold]",
                                    box=box.SIMPLE,
                                    border_style="red"
                                )
                                file_rej_table.add_column("File", style="yellow", no_wrap=False, max_width=50)
                                file_rej_table.add_column("Rejected", justify="right", style="red")
                                file_rej_table.add_column("Percentage", justify="right", style="yellow")
                                
                                total_rej = file_rej_counts.sum()
                                # Sort by count (descending) then by file path
                                for file_path, count in sorted(file_rej_counts.items(), key=lambda x: (-x[1], str(x[0]))):
                                    pct = (count / total_rej * 100) if total_rej > 0 else 0
                                    # Normalize path for display
                                    normalized_path = str(file_path).replace('\\', '/') if file_path else "unknown"
                                    display_path = _format_dataset_display(normalized_path, max_length=45)
                                    file_rej_table.add_row(display_path, f"{count:,}", f"{pct:.1f}%")
                                
                                layout.append(file_rej_table)
                                layout.append("")
                        
                        # By source_file and stage (detailed breakdown)
                        if 'source_file' in df_rej.columns and 'stage' in df_rej.columns:
                            file_stage_counts = df_rej.groupby(['source_file', 'stage']).size().reset_index(name='count')
                            if len(file_stage_counts) > 0:
                                # Get unique file count
                                unique_files = file_stage_counts['source_file'].nunique()
                                file_stage_table = Table(
                                    title=f"[bold]Rejections by File and Stage ({unique_files} file(s))[/bold]",
                                    box=box.SIMPLE,
                                    border_style="red"
                                )
                                file_stage_table.add_column("File", style="yellow", no_wrap=False, max_width=40)
                                file_stage_table.add_column("Stage", style="cyan")
                                file_stage_table.add_column("Rejected", justify="right", style="red")
                                
                                # Sort by file path, then by stage
                                file_stage_counts_sorted = file_stage_counts.sort_values(['source_file', 'stage'])
                                for _, row in file_stage_counts_sorted.iterrows():
                                    # Normalize path for display
                                    normalized_path = str(row['source_file']).replace('\\', '/') if row['source_file'] else "unknown"
                                    display_path = _format_dataset_display(normalized_path, max_length=40)
                                    file_stage_table.add_row(
                                        display_path,
                                        row['stage'],
                                        f"{row['count']:,}"
                                    )
                                
                                layout.append(file_stage_table)
                                layout.append("")
                except Exception as e:
                    pass
            
            # Sources summary with per-file breakdown
            if manifest:
                sources = manifest.get('sources', {})
                checkpoint_sources = checkpoint.get('sources', {}) if checkpoint else {}
                
                if sources:
                    sources_table = Table(title="[bold]Sources Summary[/bold]", box=box.ROUNDED, border_style="green")
                    sources_table.add_column("Source", style="cyan")
                    sources_table.add_column("Processed", justify="right", style="magenta")
                    sources_table.add_column("Shards", justify="right", style="yellow")
                    
                    for src_name, src_info in sources.items():
                        processed = src_info.get('processed_docs', 0)
                        shards = src_info.get('shard_idx', 0)
                        sources_table.add_row(src_name, f"{processed:,}", f"{shards}")
                    
                    layout.append(sources_table)
                    layout.append("")
                    
                    # Per-file statistics for each source
                    for src_name in sources.keys():
                        src_info = checkpoint_sources.get(src_name, {})
                        file_stats = src_info.get('file_stats', {})
                        
                        if file_stats:
                            file_stats_table = Table(
                                title=f"[bold]Per-File Statistics: {src_name} ({len(file_stats)} file(s))[/bold]",
                                box=box.ROUNDED,
                                border_style="cyan"
                            )
                            file_stats_table.add_column("File", style="yellow", no_wrap=False, max_width=50)
                            file_stats_table.add_column("Processed", justify="right", style="magenta")
                            file_stats_table.add_column("Written", justify="right", style="green")
                            file_stats_table.add_column("Rejected", justify="right", style="red")
                            file_stats_table.add_column("Success Rate", justify="right", style="yellow")
                            
                            # Sort by file path for consistent display
                            for file_path, stats in sorted(file_stats.items()):
                                file_processed = stats.get('processed', 0)
                                file_written = stats.get('written', 0)
                                file_rejected = stats.get('rejected', 0)
                                success_rate = (file_written / file_processed * 100) if file_processed > 0 else 0.0
                                
                                # Normalize path for display
                                normalized_path = file_path.replace('\\', '/') if file_path else "unknown"
                                display_path = _format_dataset_display(normalized_path, max_length=45)
                                
                                file_stats_table.add_row(
                                    display_path,
                                    f"{file_processed:,}",
                                    f"[green]{file_written:,}[/green]",
                                    f"[red]{file_rejected:,}[/red]",
                                    f"{success_rate:.1f}%"
                                )
                            
                            layout.append(file_stats_table)
                            layout.append("")
                        else:
                            # Show message if no file stats for this source
                            layout.append(Panel(
                                f"[yellow]No per-file statistics available for source '{src_name}'[/yellow]",
                                title=f"[bold]Per-File Statistics: {src_name}[/bold]",
                                border_style="yellow"
                            ))
                            layout.append("")
            
            # Note: Footer is now rendered in main loop, so we don't duplicate it here
            
            # Render all items directly to console
            if layout:
                for item in layout:
                    try:
                        console.print(item)
                    except Exception as item_error:
                        # If individual item fails, try to print as string
                        console.print(f"[yellow]Warning: Could not render item: {item_error}[/yellow]")
                        console.print(str(item)[:100] + "..." if len(str(item)) > 100 else str(item))
            else:
                # Fallback: show basic info if layout is empty
                console.print("[yellow]No data to display[/yellow]")
                if manifest:
                    console.print(f"Run ID: {manifest.get('run_id', 'unknown')}")
                    console.print(f"Written: {manifest.get('total_written_docs', 0):,}")
                    console.print(f"Rejected: {manifest.get('total_rejected_docs', 0):,}")
    except Exception as e:
        console.print(f"[red]Error in render_analytics_screen: {e}[/red]")
        import traceback
        console.print(traceback.format_exc())

def create_unified_app(out_dir: str, refresh_interval: float = 5.0):
    """Create unified monitor + analytics application."""
    console = Console()
    
    # Find latest run
    manifest = load_manifest(out_dir)
    if not manifest:
        console.print(f"[red]No run found in {out_dir}[/red]")
        console.print("Run a pipeline first: python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml")
        console.print(f"\n[dim]Looking for manifest in: {os.path.join(out_dir, 'manifests')}[/dim]")
        return
    
    run_id = manifest.get('run_id', 'unknown')
    
    # Print initial header (this will be cleared but helps with startup)
    # Print initial message to ensure console works
    console.print(f"\n[bold cyan]Clean Corpus Platform - Unified Monitor & Analytics[/bold cyan]")
    console.print(f"[dim]Monitoring: {out_dir} | Run ID: {run_id}[/dim]")
    console.print("[dim]READ-ONLY mode - Safe to start/stop anytime without affecting pipeline[/dim]")
    console.print("[dim]Press 'm' for Monitor, 'a' for Analytics, 'q' to quit[/dim]\n")
    
    # Test that console is working - force output
    console.print("[green]✓ Dashboard starting...[/green]", end="\n\n")
    console.print()  # Extra newline
    
    # Wait a moment so user can see the initial message
    time.sleep(1.0)
    
    start_time = time.time()
    current_screen = "monitor"  # Start with monitor screen
    
    # Check if running on Windows
    is_windows = sys.platform == 'win32'
    
    try:
        while True:
            try:
                # Reload data
                manifest = load_manifest(out_dir)
                checkpoint = load_checkpoint(out_dir, run_id)
                analytics_df = load_analytics(out_dir)
                log_lines = get_latest_log_lines(out_dir, run_id, 5)
                
                # Ensure checkpoint is not None
                if checkpoint is None:
                    checkpoint = {"sources": {}}
                
                elapsed = time.time() - start_time
                
                # Clear screen (use print instead of clear on Windows if needed)
                try:
                    console.clear()
                except:
                    # Fallback: print newlines to clear
                    console.print("\n" * 50)
                
                # Always show header first (before any rendering)
                header_text = f"[bold]Clean Corpus Platform - {'Monitor' if current_screen == 'monitor' else 'Analytics'}[/bold]"
                if manifest:
                    header_text += f" | Run: [cyan]{manifest.get('run_id', 'unknown')}[/cyan]"
                header_text += f" | Elapsed: [yellow]{elapsed:.0f}s[/yellow]"
                
                # Ensure header is printed
                try:
                    console.print(Panel(header_text, style="bold blue" if current_screen == "monitor" else "bold yellow", box=box.DOUBLE))
                    console.print()
                except Exception as header_error:
                    # Fallback: simple text header
                    console.print(f"\n{'='*70}")
                    console.print(header_text)
                    console.print(f"{'='*70}\n")
                
                # Render current screen directly
                render_success = False
                try:
                    if current_screen == "monitor":
                        render_monitor_screen(console, out_dir, manifest, checkpoint, analytics_df, 
                                             log_lines, elapsed, refresh_interval)
                    else:  # analytics
                        render_analytics_screen(console, out_dir, manifest, analytics_df)
                    render_success = True
                except Exception as render_error:
                    # If rendering fails, show error and basic info
                    console.print(f"[red]Error rendering {current_screen} screen: {render_error}[/red]")
                    console.print(f"[yellow]Manifest: {manifest is not None}[/yellow]")
                    console.print(f"[yellow]Checkpoint: {checkpoint is not None}[/yellow]")
                    console.print(f"[yellow]Analytics: {analytics_df is not None}[/yellow]")
                    
                    # Show basic info even if rendering fails
                    if manifest:
                        console.print(f"\n[cyan]Run ID:[/cyan] {manifest.get('run_id', 'unknown')}")
                        console.print(f"[cyan]Written:[/cyan] {manifest.get('total_written_docs', 0):,}")
                        console.print(f"[cyan]Rejected:[/cyan] {manifest.get('total_rejected_docs', 0):,}")
                    
                    # Show per-file stats if available
                    if checkpoint:
                        checkpoint_sources = checkpoint.get('sources', {})
                        for src_name, src_info in checkpoint_sources.items():
                            file_stats = src_info.get('file_stats', {})
                            if file_stats:
                                console.print(f"\n[cyan]Source {src_name} per-file stats:[/cyan]")
                                for file_path, stats in sorted(file_stats.items()):
                                    console.print(f"  {file_path}: processed={stats.get('processed', 0)} written={stats.get('written', 0)} rejected={stats.get('rejected', 0)}")
                    
                    import traceback
                    console.print("\n[dim]Full error:[/dim]")
                    console.print("[dim]" + traceback.format_exc() + "[/dim]")
                    console.print(f"\n[yellow]Waiting {refresh_interval}s before retry...[/yellow]")
                    time.sleep(refresh_interval)
                    continue
                
                # Always show footer (only if rendering succeeded)
                if render_success:
                    footer = Panel(
                        f"[dim]Refreshing every {refresh_interval}s | Last update: {datetime.now().strftime('%H:%M:%S')}[/dim]\n"
                        f"[bold]Navigation:[/bold] [cyan]'m'[/cyan] Monitor | [yellow]'a'[/yellow] Analytics | [red]'q'[/red] Quit",
                        style="dim",
                        box=box.SIMPLE
                    )
                    console.print(footer)
            except Exception as e:
                console.print(f"[red]Error in main loop: {e}[/red]")
                import traceback
                console.print(traceback.format_exc())
                time.sleep(refresh_interval)
            
            # Simple input handling (works on both Windows and Unix)
            # For Windows, we'll use msvcrt for non-blocking input
            if is_windows:
                try:
                    import msvcrt
                    # Check for key press during refresh interval
                    end_time = time.time() + refresh_interval
                    key_pressed = None
                    while time.time() < end_time:
                        if msvcrt.kbhit():
                            try:
                                key_pressed = msvcrt.getch().decode('utf-8').lower()
                                break
                            except:
                                pass
                        time.sleep(0.1)
                    
                    if key_pressed:
                        if key_pressed == 'q':
                            break
                        elif key_pressed == 'm' or key_pressed == '1':
                            current_screen = "monitor"
                        elif key_pressed == 'a' or key_pressed == '2':
                            current_screen = "analytics"
                except ImportError:
                    # Fallback: just wait for refresh interval
                    time.sleep(refresh_interval)
            else:
                # Unix: use select for non-blocking input
                try:
                    import select
                    if select.select([sys.stdin], [], [], refresh_interval)[0]:
                        key = sys.stdin.read(1).lower()
                        if key == 'q':
                            break
                        elif key == 'm' or key == '1':
                            current_screen = "monitor"
                        elif key == 'a' or key == '2':
                            current_screen = "analytics"
                    else:
                        time.sleep(0.1)  # Small sleep to prevent busy loop
                except:
                    # Fallback: just wait for refresh interval
                    time.sleep(refresh_interval)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Application stopped.[/yellow]\n")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]\n")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Unified Monitor & Analytics for Clean Corpus Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s storage_example
  %(prog)s storage_example --refresh 10.0
  %(prog)s storage_example -r 2.5
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
    
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        print(f"Error: Directory not found: {args.output_dir}")
        print("Run a pipeline first: clean-corpus build --config examples/build_local_jsonl.yaml")
        sys.exit(1)
    
    create_unified_app(args.output_dir, args.refresh)

if __name__ == "__main__":
    main()
