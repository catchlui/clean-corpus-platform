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
from typing import Dict, Any, List
import os, time, logging
from tqdm import tqdm

from ..sources.base import SourceSpec, RawDocument
from ..sources.registry import make_source
from ..utils.hashing import sha256_bytes
from ..pipeline.context import Document
from ..stages.registry import make_stages
# Import PII module to trigger auto-registration of detectors
import clean_corpus.pii  # noqa: F401
from ..storage.writer import append_jsonl, write_manifest, write_docs_shard
from ..writers.registry import get_corpus_writer, get_metadata_writer
from ..analytics.sink import AnalyticsSink
from ..analytics.schemas import make_event
from ..checkpoints.store import CheckpointStore

log = logging.getLogger("clean_corpus.build")

def build_local(cfg: Dict[str, Any]) -> None:
    run = cfg["run"]
    run_id = run["run_id"]
    out_dir = run["out_dir"]
    shard_docs = int(run.get("shard_docs", 5000))
    log_every = int(run.get("log_every_docs", 1000))
    ckpt_every = int(run.get("checkpoint_every_docs", shard_docs))
    policy_version = run.get("policy_version", "policy_v0")
    
    # Record start time for dashboard
    start_time_ms = int(time.time() * 1000)

    os.makedirs(os.path.join(out_dir, "docs"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "rejections"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "manifests"), exist_ok=True)

    tokenizer_name = cfg.get("tokenizer", {}).get("name", "custom_tok")
    stages = make_stages(cfg.get("stages", []), cfg["policies"], tokenizer_name=tokenizer_name)
    sink = AnalyticsSink(out_dir=out_dir, run_id=run_id)
    ckpt = CheckpointStore(out_dir=out_dir, run_id=run_id)
    
    # Set global PDF configuration if provided
    if "pdf" in cfg:
        from ..sources.registry import set_global_pdf_config
        set_global_pdf_config(cfg["pdf"])

    state = ckpt.load()
    state.setdefault("run_id", run_id)
    state.setdefault("sources", {})
    # Set start time if not already set
    if "start_time_ms" not in state:
        state["start_time_ms"] = start_time_ms
        ckpt.save(state)

    total_written_docs = 0
    total_rejected_docs = 0

    for s_cfg in cfg["sources"]:
        spec = SourceSpec(**s_cfg)
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
                doc = _raw_to_doc(raw, policy_version=policy_version)
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
                        rejs.append({
                            "doc_id": doc.doc_id.hex(),
                            "source": doc.source,
                            "source_file": source_file,
                            "stage": st.name,
                            "decision": "reject",
                            "reason_code": d.reason_code,
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
                    cw = get_corpus_writer(cfg.get("output", {}).get("corpus_format", "parquet"))
                    mw = get_metadata_writer(cfg.get("output", {}).get("metadata_format", "parquet_v1"))
                    cw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
                    mw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
                    shard.clear()
                    shard_idx += 1

                    # flush analytics, rejections, checkpoint after shard
                    _flush_stage_analytics(run_id, spec.name, stages, stage_counts, sink)
                    sink.flush_aggregates()
                    if rejs:
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
                        append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
                        rejs.clear()

            except Exception as e:
                # Hard error handling: reject doc but continue.
                log.exception(f"Unhandled error in doc processing source={spec.name}: {e}")
                total_rejected_docs += 1
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
            cw = get_corpus_writer(cfg.get("output", {}).get("corpus_format", "parquet"))
            mw = get_metadata_writer(cfg.get("output", {}).get("metadata_format", "parquet_v1"))
            cw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
            mw.write_shard(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
            shard.clear()
            shard_idx += 1

        # final flush for this source
        _flush_stage_analytics(run_id, spec.name, stages, stage_counts, sink)
        sink.flush_aggregates()
        if rejs:
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
        "outputs": {
            "docs_dir": os.path.join(out_dir, "docs"),
            "rejections": os.path.join(out_dir, "rejections", "rejections.jsonl"),
            "analytics_events": os.path.join(out_dir, "analytics", "events"),
            "analytics_aggregates": os.path.join(out_dir, "analytics", "aggregates"),
            "metadata_dir": os.path.join(out_dir, "metadata"),
            "checkpoint": os.path.join(out_dir, "checkpoints", f"{run_id}.json"),
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
    
    log.info(f"Build complete. manifest={os.path.join(out_dir,'manifests',f'{run_id}.json')}")

def build_ray(cfg: Dict[str, Any], ray_cfg: Dict[str, Any]) -> None:
    # Placeholder: local runner is reference; Ray upgrade is straightforward.
    # Teams can evolve this into a ray.data pipeline with stage-level analytics hooks.
    import ray
    addr = ray_cfg.get("ray", {}).get("address", "auto")
    ray.init(address=addr, ignore_reinit_error=True)
    log.info("Ray initialized. For now, using local build logic (safe baseline).")
    build_local(cfg)

def _raw_to_doc(raw: RawDocument, policy_version: str) -> Document:
    # doc_id placeholder is hash of prefix to keep something stable even before dedup stage
    seed = sha256_bytes((raw.text or "")[:512])
    # Extract source_file and language from extra if available
    source_file = None
    language = "en"  # Default to English
    if raw.extra and isinstance(raw.extra, dict):
        source_file = raw.extra.get("source_file")
        # Extract language from metadata (for web PDFs and multi-language sources)
        language = raw.extra.get("language") or raw.extra.get("lang") or "en"
        # Ensure language is a valid ISO 639-1 code (2 characters)
        if language and len(language) >= 2:
            language = language[:2].lower()
        else:
            language = "en"
    
    return Document(
        doc_id=seed,
        source=raw.source,
        text=raw.text or "",
        url=raw.url,
        license=raw.license if raw.license is not None else "Unknown",
        policy_version=policy_version,
        source_file=source_file,
        lang=language,
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
