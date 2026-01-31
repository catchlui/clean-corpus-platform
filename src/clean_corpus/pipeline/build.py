"""Pipeline build runners (local + Ray).

Local runner:
- simple and deterministic
- checkpoint/resume support
- emits analytics at every stage
- writes Parquet shards + rejection logs

Ray runner:
- placeholder hook for teams to upgrade to `ray.data` when ready
- still emits analytics and writes outputs, but local runner is the reference

This module is the 'entrypoint' for the Data Platform team.
"""

from __future__ import annotations
import collections
from typing import Dict, Any, List, Optional
import os, time, logging
from tqdm import tqdm

from ..sources.base import SourceSpec, RawDocument
from ..sources.registry import make_source
from ..utils.hashing import sha256_bytes
from ..pipeline.context import Document
from ..stages.registry import make_stages
from ..fingerprints.priority import (
    document_type_priority_rank,
    source_priority_rank,
)
# Import PII module to trigger auto-registration of detectors
import clean_corpus.pii  # noqa: F401
from ..storage.writer import append_jsonl, write_manifest, write_docs_shard
from ..writers.registry import get_corpus_writer, get_metadata_writer, register_corpus_writer, list_corpus_writers
from ..output_layout import (
    ensure_structured_dirs,
    get_rejection_category,
    rejection_path,
    get_document_subpath,
    write_stats_reports,
)
from ..analytics.sink import AnalyticsSink
from ..analytics.schemas import make_event
from ..checkpoints.store import CheckpointStore
from ..run_id import resolve_run_id, resolve_out_dir

log = logging.getLogger("clean_corpus.build")

def _register_format_writer(format_name: str, options: Dict[str, Any]) -> None:
    """Register a format-specific writer with custom options."""
    if format_name == "dolma" or format_name == "doml":
        from ..writers.dolma_writer import DolmaCorpusWriter
        from ..writers.registry import list_corpus_writers

        # Only register if not already registered or if we need custom options
        registered_formats = list_corpus_writers()
        if format_name not in registered_formats or options:
            include_all = options.get("include_all_metadata", True)
            custom_fields = options.get("custom_metadata_fields", {})
            default_data_tag = options.get("data_tag")
            writer = DolmaCorpusWriter(
                include_all_metadata=include_all,
                custom_metadata_fields=custom_fields,
                default_data_tag=default_data_tag,
            )
            try:
                register_corpus_writer(format_name, writer)
                log.info(f"Registered {format_name} writer with custom options")
            except ValueError:
                # Already registered, update if needed
                log.debug(f"{format_name} writer already registered")

def build_local(cfg: Dict[str, Any]) -> None:
    run = cfg["run"]
    run_id = resolve_run_id(cfg)
    out_dir = resolve_out_dir(cfg, run_id)
    run["run_id"] = run_id
    run["out_dir"] = out_dir
    shard_docs = int(run.get("shard_docs", 5000))
    log_every = int(run.get("log_every_docs", 1000))
    ckpt_every = int(run.get("checkpoint_every_docs", shard_docs))
    policy_version = run.get("policy_version", "policy_v0")
    
    # Record start time for dashboard
    start_time_ms = int(time.time() * 1000)

    # Storage directories (metadata and processed output)
    output_cfg_early = cfg.get("output", {})
    layout = output_cfg_early.get("layout", "flat")
    if layout == "structured":
        ensure_structured_dirs(out_dir)
    else:
        os.makedirs(os.path.join(out_dir, "docs"), exist_ok=True)
        os.makedirs(os.path.join(out_dir, "rejections"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "manifests"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "metadata"), exist_ok=True)

    # Global directories (checkpoints and logs)
    global_cfg = cfg.get("global", {})
    global_checkpoint_dir = global_cfg.get("checkpoint_dir", "checkpoints")
    global_log_dir = global_cfg.get("log_dir", "logs")
    
    os.makedirs(global_checkpoint_dir, exist_ok=True)
    os.makedirs(global_log_dir, exist_ok=True)

    # Checkpoint configuration
    checkpoint_cfg = cfg.get("checkpoint", {})
    resume_mode = checkpoint_cfg.get("resume_mode", "auto")
    checkpoint_id = checkpoint_cfg.get("checkpoint_id")

    tokenizer_name = cfg.get("tokenizer", {}).get("name", "custom_tok")
    
    # Setup logging with global log directory
    from ..logging_ import setup_logging
    setup_logging(out_dir=out_dir, run_id=run_id, log_dir=global_log_dir)
    
    # Build stages with unified configuration support
    stages = make_stages(
        cfg.get("stages", []), 
        cfg["policies"], 
        tokenizer_name=tokenizer_name,
        global_processing=cfg.get("global", {}).get("processing", {}),
        source_configs=cfg.get("sources", [])
    )
    
    # Initialize format-specific writers if format_options are provided
    output_cfg = cfg.get("output", {})
    format_options = output_cfg.get("format_options", {})
    corpus_format = output_cfg.get("corpus_format", "parquet")
    # Merge run-level data_tag into format options so writers can emit it
    default_data_tag = output_cfg.get("data_tag")

    # Register S3 Parquet writer when output is to S3 (run.storage type s3 + corpus_format s3_parquet)
    if corpus_format == "s3_parquet" and run.get("storage") and run["storage"].get("type") == "s3":
        if "s3_parquet" not in list_corpus_writers():
            from ..storage.base import get_storage_backend
            from ..writers.s3_parquet import S3ParquetCorpusWriter
            backend = get_storage_backend(run["storage"])
            register_corpus_writer("s3_parquet", S3ParquetCorpusWriter(backend))
        # out_dir for S3 is a key prefix (no s3://); backend joins bucket/prefix/out_dir
        # Resolved out_dir already has run_id substituted

    # Register format-specific writers if options provided or data_tag set
    if format_options and corpus_format in format_options:
        opts = dict(format_options[corpus_format])
        if default_data_tag is not None and "data_tag" not in opts:
            opts["data_tag"] = default_data_tag
        _register_format_writer(corpus_format, opts)
    elif default_data_tag is not None and corpus_format in ("dolma", "doml"):
        _register_format_writer(corpus_format, {"data_tag": default_data_tag})

    sink = AnalyticsSink(out_dir=out_dir, run_id=run_id)
    ckpt = CheckpointStore(
        out_dir=out_dir, 
        run_id=run_id,
        global_checkpoint_dir=global_checkpoint_dir
    )
    
    # Set global PDF configuration if provided
    if "pdf" in cfg:
        from ..sources.registry import set_global_pdf_config
        set_global_pdf_config(cfg["pdf"])

    # Load checkpoint with resume mode support
    state = ckpt.load(resume_mode=resume_mode, checkpoint_id=checkpoint_id)
    state.setdefault("run_id", run_id)
    state.setdefault("sources", {})
    state.setdefault("resume_mode", resume_mode)
    
    # Set start time if not already set (or if starting from beginning)
    if resume_mode == "beginning" or resume_mode == "ignore" or "start_time_ms" not in state:
        state["start_time_ms"] = start_time_ms
        if resume_mode == "ignore":
            # Clear sources for ignore mode
            state["sources"] = {}
        ckpt.save(state)

    total_written_docs = 0
    total_rejected_docs = 0
    # For structured layout: accumulate rejection counts by stage for stats reports
    rejection_counts_by_stage = {}

    # Order sources by dedup priority (type first, then optional source within type) so global_dedup sees high priority first
    gf = (cfg.get("global", {}).get("processing", {}) or {}).get("global_fingerprints") or {}
    document_type_priority = gf.get("document_type_priority") or []
    source_to_document_type = gf.get("source_to_document_type") or {}
    source_priority = gf.get("source_priority") or []
    if document_type_priority and source_to_document_type:
        def _sort_key(s):
            name = s.get("name", "")
            type_rank = document_type_priority_rank(source_to_document_type.get(name, "unknown"), document_type_priority)
            src_rank = source_priority_rank(name, source_priority) if source_priority else 0
            return (type_rank, src_rank)
        source_configs = sorted(cfg["sources"], key=_sort_key)
    elif source_priority:
        source_configs = sorted(
            cfg["sources"],
            key=lambda s: source_priority_rank(s.get("name", ""), source_priority),
        )
    else:
        source_configs = cfg["sources"]

    for s_cfg in source_configs:
        # Create a copy of source config without processing (SourceSpec handles it as optional)
        # Processing overrides are stored in SourceSpec but handled at stage level
        spec = SourceSpec(**s_cfg)
        
        # Check for PDF source dependencies before creating source
        if spec.kind == "pdf":
            extractor_name = spec.extractor or cfg.get("pdf", {}).get("extractor", "pymupdf")
            if extractor_name == "pymupdf":
                # Ensure user site-packages is in path (for cases where pymupdf is installed with --user)
                import site
                import sys
                user_site = site.getusersitepackages()
                if user_site and user_site not in sys.path:
                    sys.path.insert(0, user_site)
                
                try:
                    # Try importing pymupdf first (handles namespace conflicts with other 'fitz' packages)
                    import pymupdf
                    fitz = pymupdf  # pymupdf provides fitz interface
                    # Verify it's actually PyMuPDF (has 'open' method)
                    if not hasattr(fitz, 'open'):
                        raise ImportError("pymupdf module found but doesn't have 'open' method")
                except ImportError:
                    # Fallback to direct fitz import
                    try:
                        import fitz  # PyMuPDF
                        # Verify it's actually PyMuPDF (has 'open' method)
                        if not hasattr(fitz, 'open'):
                            raise ImportError("fitz module found but doesn't have 'open' method - may be wrong package")
                    except ImportError as e:
                        log.error(
                            f"Source {spec.name}: PyMuPDF not installed. "
                            f"Install with: pip install pymupdf\n"
                            f"Or change extractor to 'pdfplumber' and install: pip install pdfplumber\n"
                            f"Or remove/comment out this PDF source if not needed.\n"
                            f"Error: {e}"
                        )
                        raise ImportError(
                            f"PyMuPDF not installed for PDF source '{spec.name}'. "
                            f"Install with: pip install pymupdf"
                        )
            elif extractor_name == "pdfplumber":
                try:
                    import pdfplumber
                except ImportError:
                    log.error(
                        f"Source {spec.name}: pdfplumber not installed. "
                        f"Install with: pip install pdfplumber\n"
                        f"Or change extractor to 'pymupdf' and install: pip install pymupdf\n"
                        f"Or remove/comment out this PDF source if not needed."
                    )
                    raise ImportError(
                        f"pdfplumber not installed for PDF source '{spec.name}'. "
                        f"Install with: pip install pdfplumber"
                    )
        
        src = make_source(spec)
        
        # Get source metadata for logging
        src_metadata = src.metadata()
        dataset_info = src_metadata.get("dataset", src_metadata.get("path", src_metadata.get("files", "N/A")))
        
        # Verify source file/directory exists
        if spec.kind == "local_jsonl":
            # Handle multiple files (list) or single file (string)
            if isinstance(spec.dataset, list):
                files = spec.dataset
                log.info(f"Source {spec.name}: Processing {len(files)} files")
                for file_path in files:
                    if not os.path.exists(file_path):
                        log.warning(f"Source {spec.name}: File not found: {file_path}")
                    else:
                        file_size = os.path.getsize(file_path)
                        log.info(f"Source {spec.name}: {file_path} - {file_size:,} bytes")
            else:
                # Single file or directory/glob pattern
                if os.path.isfile(spec.dataset):
                    if not os.path.exists(spec.dataset):
                        log.error(f"Source {spec.name}: File not found: {spec.dataset}")
                        log.error(f"  Current working directory: {os.getcwd()}")
                        log.error(f"  Absolute path would be: {os.path.abspath(spec.dataset)}")
                        continue
                    file_size = os.path.getsize(spec.dataset)
                    log.info(f"Source {spec.name}: File size: {file_size:,} bytes")
                    if file_size == 0:
                        log.warning(f"Source {spec.name}: File is empty!")
                    else:
                        # Quick check: count lines in file
                        try:
                            with open(spec.dataset, 'r', encoding='utf-8') as f:
                                line_count = sum(1 for line in f if line.strip())
                            log.info(f"Source {spec.name}: Found {line_count} non-empty lines in file")
                        except Exception as e:
                            log.warning(f"Source {spec.name}: Could not count lines: {e}")
                elif os.path.isdir(spec.dataset):
                    log.info(f"Source {spec.name}: Processing directory: {spec.dataset}")
                else:
                    # Might be a glob pattern - will be resolved by source
                    log.info(f"Source {spec.name}: Dataset: {spec.dataset} (will be resolved by source)")
            
            # Log file count if available from metadata
            if "file_count" in src_metadata:
                log.info(f"Source {spec.name}: Will process {src_metadata['file_count']} file(s)")

        s_state = state["sources"].get(spec.name, {"processed_docs": 0, "shard_idx": 0})
        processed = int(s_state.get("processed_docs", 0))
        shard_idx = int(s_state.get("shard_idx", 0))

        log.info(f"Starting source={spec.name} dataset={dataset_info} kind={spec.kind} resume_processed={processed} resume_shard_idx={shard_idx}")

        # Data tag for filtering (training | sft | alignment): per-source override or output default
        data_tag_for_source = getattr(spec, "data_tag", None) or output_cfg.get("data_tag")

        shard: List[Document] = []
        rejs: List[dict] = []
        stage_counts = {st.name: {"in": 0, "acc": 0, "rej": 0, "rej_reasons": {}} for st in stages}
        
        # Track per-file statistics
        file_stats: Dict[str, Dict[str, int]] = {}  # file_path -> {processed, written, rejected}

        # best-effort resume: skip N records
        it = src.stream()
        skipped_count = 0
        for _ in range(processed):
            try:
                next(it)
                skipped_count += 1
            except StopIteration:
                log.warning(f"Source {spec.name}: Tried to skip {processed} docs but iterator ended early at {skipped_count}")
                break

        # Track if any documents were processed
        doc_count = 0
        first_doc = True
        for i, raw in enumerate(it, start=processed):
            doc_count += 1
            if first_doc:
                log.info(f"Source {spec.name}: First document received - raw_id={raw.raw_id[:20] if raw.raw_id else 'N/A'} text_length={len(raw.text)}")
                first_doc = False
            try:
                doc = _raw_to_doc(raw, policy_version=policy_version, data_tag=data_tag_for_source)
                accepted = True
                
                # Track per-file statistics
                source_file = doc.source_file or "unknown"
                if source_file not in file_stats:
                    file_stats[source_file] = {"processed": 0, "written": 0, "rejected": 0}
                file_stats[source_file]["processed"] += 1

                # run stages sequentially; emit analytics per stage on batch boundaries (we do per-doc counters, flush periodic)
                for st in stages:
                    stage_counts[st.name]["in"] += 1
                    d = st.apply(doc)
                    if not d.accepted:
                        accepted = False
                        stage_counts[st.name]["rej"] += 1
                        rc = d.reason_code or "REJECT"
                        stage_counts[st.name]["rej_reasons"][rc] = stage_counts[st.name]["rej_reasons"].get(rc, 0) + 1
                        rc = d.reason_code or "REJECT"
                        rejection_counts_by_stage.setdefault(st.name, {})
                        rejection_counts_by_stage[st.name][rc] = rejection_counts_by_stage[st.name].get(rc, 0) + 1
                        rejs.append({
                            "doc_id": doc.doc_id.hex(),
                            "source": doc.source,
                            "source_file": source_file,
                            "stage": st.name,
                            "decision": "reject",
                            "reason_code": rc,
                            "reason_detail": d.reason_detail,
                            "ts_ms": int(time.time()*1000),
                        })
                        total_rejected_docs += 1
                        file_stats[source_file]["rejected"] += 1
                        break
                    else:
                        stage_counts[st.name]["acc"] += 1

                if not accepted:
                    # Log rejection immediately for visibility
                    log.debug(f"Rejected doc {i+1}: stage={st.name} reason={d.reason_code} detail={d.reason_detail}")
                    # periodically flush analytics + rejections + checkpoint
                    if (i + 1) % log_every == 0:
                        _flush_stage_analytics(run_id, doc.source, stages, stage_counts, sink)
                        sink.flush_aggregates()
                        if rejs:
                            if layout == "structured":
                                by_cat = collections.defaultdict(list)
                                for r in rejs:
                                    by_cat[get_rejection_category(r.get("reason_code", ""))].append(r)
                                for cat, items in by_cat.items():
                                    append_jsonl(rejection_path(out_dir, cat), items)
                            else:
                                append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
                            rejs.clear()
                    if (i + 1) % ckpt_every == 0:
                        state["sources"][spec.name] = {
                            "processed_docs": i + 1, 
                            "shard_idx": shard_idx,
                            "file_stats": file_stats
                        }
                        ckpt.save(state)
                    continue

                shard.append(doc)
                total_written_docs += 1
                file_stats[source_file]["written"] += 1

                # shard flush
                if len(shard) >= shard_docs:
                    output_cfg = cfg.get("output", {})
                    corpus_format = output_cfg.get("corpus_format", "parquet")
                    metadata_format = output_cfg.get("metadata_format", "parquet_v1")
                    format_options = output_cfg.get("format_options", {})
                    document_subpath = None
                    if layout == "structured" and shard:
                        first = shard[0]
                        document_subpath = get_document_subpath(
                            spec.name,
                            first.lang,
                            first.extra,
                            source_to_namespace=output_cfg.get("source_to_namespace"),
                        )
                    cw = get_corpus_writer(corpus_format)
                    cw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx, document_subpath=document_subpath)
                    # Only write metadata if format is specified (skip for JSONL-only workflows)
                    if metadata_format:
                        mw = get_metadata_writer(metadata_format)
                        mw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
                    shard.clear()
                    shard_idx += 1

                    # flush analytics, rejections, checkpoint after shard
                    _flush_stage_analytics(run_id, spec.name, stages, stage_counts, sink)
                    sink.flush_aggregates()
                    if rejs:
                        if layout == "structured":
                            by_cat = collections.defaultdict(list)
                            for r in rejs:
                                by_cat[get_rejection_category(r.get("reason_code", ""))].append(r)
                            for cat, items in by_cat.items():
                                append_jsonl(rejection_path(out_dir, cat), items)
                        else:
                            append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
                        rejs.clear()

                    state["sources"][spec.name] = {
                        "processed_docs": i + 1,
                        "shard_idx": shard_idx,
                        "file_stats": file_stats
                    }
                    ckpt.save(state)

                # periodic logs/analytics
                if (i + 1) % log_every == 0:
                    log.info(f"source={spec.name} processed={i+1} written={total_written_docs} rejected={total_rejected_docs}")
                    _flush_stage_analytics(run_id, spec.name, stages, stage_counts, sink)
                    sink.flush_aggregates()
                    if rejs:
                        if layout == "structured":
                            by_cat = collections.defaultdict(list)
                            for r in rejs:
                                by_cat[get_rejection_category(r.get("reason_code", ""))].append(r)
                            for cat, items in by_cat.items():
                                append_jsonl(rejection_path(out_dir, cat), items)
                        else:
                            append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
                        rejs.clear()

            except Exception as e:
                # Hard error handling: reject doc but continue.
                log.exception(f"Unhandled error in doc processing source={spec.name}: {e}")
                total_rejected_docs += 1
                rejection_counts_by_stage.setdefault("runtime_error", {})
                rejection_counts_by_stage["runtime_error"]["RUNTIME_ERROR"] = rejection_counts_by_stage["runtime_error"].get("RUNTIME_ERROR", 0) + 1
                rejs.append({
                    "doc_id": getattr(raw, "raw_id", ""),
                    "source": spec.name,
                    "stage": "runtime_error",
                    "decision": "reject",
                    "reason_code": "RUNTIME_ERROR",
                    "reason_detail": str(e),
                    "ts_ms": int(time.time()*1000),
                })

        # flush remaining shard
        if shard:
            output_cfg = cfg.get("output", {})
            corpus_format = output_cfg.get("corpus_format", "parquet")
            metadata_format = output_cfg.get("metadata_format", "parquet_v1")
            document_subpath = None
            if layout == "structured" and shard:
                first = shard[0]
                document_subpath = get_document_subpath(
                    spec.name,
                    first.lang,
                    first.extra,
                    source_to_namespace=output_cfg.get("source_to_namespace"),
                )
            cw = get_corpus_writer(corpus_format)
            cw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx, document_subpath=document_subpath)
            # Only write metadata if format is specified (skip for JSONL-only workflows)
            if metadata_format:
                mw = get_metadata_writer(metadata_format)
                mw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
            shard.clear()
            shard_idx += 1

        # final flush for this source
        _flush_stage_analytics(run_id, spec.name, stages, stage_counts, sink)
        sink.flush_aggregates()
        if rejs:
            if layout == "structured":
                by_cat = collections.defaultdict(list)
                for r in rejs:
                    by_cat[get_rejection_category(r.get("reason_code", ""))].append(r)
                for cat, items in by_cat.items():
                    append_jsonl(rejection_path(out_dir, cat), items)
            else:
                append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
            rejs.clear()

        # Check if any documents were processed
        final_processed = i + 1 if 'i' in locals() else processed
        if doc_count == 0:
            log.warning(f"Source {spec.name}: No documents were yielded from source stream(). Check if source file exists and contains data.")
            log.warning(f"  Source path: {dataset_info}")
            log.warning(f"  Source kind: {spec.kind}")
        
        # Save per-file statistics to checkpoint (already done above, but ensure it's saved)
        state["sources"][spec.name] = {
            "processed_docs": final_processed,
            "shard_idx": shard_idx,
            "file_stats": file_stats
        }
        ckpt.save(state)
        
        # Log summary for this source with per-file breakdown
        log.info(f"Source {spec.name} complete: processed={final_processed} written={total_written_docs} rejected={total_rejected_docs} (doc_count={doc_count})")
        if file_stats:
            log.info(f"Source {spec.name} per-file statistics:")
            for file_path, stats in sorted(file_stats.items()):
                log.info(f"  {file_path}: processed={stats['processed']} written={stats['written']} rejected={stats['rejected']}")

    # write run manifest
    # Get config path from environment (set by CLI)
    config_path = os.environ.get('CLEAN_CORPUS_CONFIG_PATH')
    
    # Calculate total processed from sources (more accurate than counters)
    total_processed_from_sources = sum(
        s.get("processed_docs", 0) for s in state.get("sources", {}).values()
    )
    
    # If counters are 0 but sources show processing, use source counts
    if total_written_docs == 0 and total_rejected_docs == 0 and total_processed_from_sources > 0:
        log.warning(f"Document counters are 0 but sources show {total_processed_from_sources} processed. "
                   f"This may indicate all documents were filtered before counting.")
        # Analytics counts will be handled by summary report
    
    manifest = {
        "run_id": run_id,
        "policy_version": policy_version,
        "start_time_ms": state.get("start_time_ms", start_time_ms),  # Store start time
        "total_written_docs": total_written_docs,
        "total_rejected_docs": total_rejected_docs,
        "total_processed_docs": total_written_docs + total_rejected_docs,
        "sources": state.get("sources", {}),
        "config_path": config_path,  # Store config path for dashboard
        "resume_mode": state.get("resume_mode", "auto"),
        "outputs": {
            "docs_dir": os.path.join(out_dir, "documents" if layout == "structured" else "docs"),
            "rejections": os.path.join(out_dir, "rejections", "rejections.jsonl") if layout != "structured" else os.path.join(out_dir, "rejected"),
            "rejected_dir": os.path.join(out_dir, "rejected") if layout == "structured" else None,
            "stats_dir": os.path.join(out_dir, "stats") if layout == "structured" else None,
            "analytics_events": os.path.join(out_dir, "analytics", "events"),
            "analytics_aggregates": os.path.join(out_dir, "analytics", "aggregates"),
            "metadata_dir": os.path.join(out_dir, "metadata"),
            "checkpoint": os.path.join(global_checkpoint_dir, f"{run_id}.json"),
            "log_dir": global_log_dir,
        }
    }
    write_manifest(os.path.join(out_dir, "manifests", f"{run_id}.json"), manifest)
    
    # Log final summary
    log.info(f"Pipeline complete: written={total_written_docs} rejected={total_rejected_docs} "
             f"total_processed={total_written_docs + total_rejected_docs}")
    
    # Generate summary report
    try:
        from ..tools.summary_report import generate_summary_report
        report_path = generate_summary_report(out_dir, run_id, manifest, config=cfg)
        log.info(f"Summary report: {report_path}")
    except Exception as e:
        log.debug(f"Could not generate summary report: {e}")

    # Structured layout: write stats reports (pii_report.json, dedup_report.json, quality_report.json)
    if layout == "structured":
        try:
            total_processed = total_written_docs + total_rejected_docs
            write_stats_reports(
                out_dir,
                rejection_counts_by_stage,
                total_processed,
                total_written_docs,
                total_rejected_docs,
            )
            log.info(f"Stats reports written to {os.path.join(out_dir, 'stats')}")
        except Exception as e:
            log.debug(f"Could not write stats reports: {e}")

    log.info(f"Build complete. manifest={os.path.join(out_dir,'manifests',f'{run_id}.json')}")

def build_ray(cfg: Dict[str, Any], ray_cfg: Dict[str, Any]) -> None:
    # Placeholder: local runner is reference; Ray upgrade is straightforward.
    # Teams can evolve this into a ray.data pipeline with stage-level analytics hooks.
    import ray
    addr = ray_cfg.get("ray", {}).get("address", "auto")
    ray.init(address=addr, ignore_reinit_error=True)
    log.info("Ray initialized. For now, using local build logic (safe baseline).")
    build_local(cfg)

def _raw_to_doc(
    raw: RawDocument,
    policy_version: str,
    data_tag: Optional[str] = None,
) -> Document:
    # doc_id placeholder is hash of prefix to keep something stable even before dedup stage
    seed = sha256_bytes((raw.text or "")[:512])
    # Extract source_file and language from extra if available
    source_file = None
    language = "en"  # Default to English
    extra_metadata = {}
    
    if raw.extra and isinstance(raw.extra, dict):
        source_file = raw.extra.get("source_file")
        # Extract language from metadata (for web PDFs and multi-language sources)
        language = raw.extra.get("language") or raw.extra.get("lang") or "en"
        # Ensure language is a valid ISO 639-1 code (2 characters)
        if language and len(language) >= 2:
            language = language[:2].lower()
        else:
            language = "en"
        
        # Preserve all extra metadata (folder-level metadata, PDF metadata, etc.)
        # This includes: book_name, author, certificate_type, pdf_metadata, etc.
        extra_metadata = dict(raw.extra)
    
    return Document(
        doc_id=seed,
        source=raw.source,
        text=raw.text or "",
        url=raw.url,
        license=raw.license if raw.license is not None else "Unknown",
        policy_version=policy_version,
        source_file=source_file,
        lang=language,
        data_tag=data_tag,
        extra=extra_metadata,  # Preserve custom metadata (folder-level, PDF metadata, etc.)
    )

def _flush_stage_analytics(run_id: str, source: str, stages, stage_counts, sink: AnalyticsSink) -> None:
    # Emit one event per stage with cumulative counters since last flush.
    for st in stages:
        c = stage_counts[st.name]
        if c["in"] == 0 and c["acc"] == 0 and c["rej"] == 0:
            continue
        ev = make_event(
            run_id=run_id,
            stage=st.name,
            source=source,
            layer=getattr(st, "layer", "preprocessing"),
            counts={"input_docs": c["in"], "accepted_docs": c["acc"], "rejected_docs": c["rej"]},
            metrics={},
            rejection_breakdown=c["rej_reasons"],
        )
        sink.emit(ev)
        # reset counters after flush
        stage_counts[st.name] = {"in": 0, "acc": 0, "rej": 0, "rej_reasons": {}}
