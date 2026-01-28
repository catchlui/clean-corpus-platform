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
from ..storage.writer import append_jsonl, write_manifest
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

    os.makedirs(os.path.join(out_dir, "docs"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "rejections"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "manifests"), exist_ok=True)

    tokenizer_name = cfg.get("tokenizer", {}).get("name", "custom_tok")
    stages = make_stages(cfg.get("stages", []), cfg["policies"], tokenizer_name=tokenizer_name)
    sink = AnalyticsSink(out_dir=out_dir, run_id=run_id)
    ckpt = CheckpointStore(out_dir=out_dir, run_id=run_id)

    state = ckpt.load()
    state.setdefault("run_id", run_id)
    state.setdefault("sources", {})

    total_written_docs = 0
    total_rejected_docs = 0

    for s_cfg in cfg["sources"]:
        spec = SourceSpec(**s_cfg)
        src = make_source(spec)

        s_state = state["sources"].get(spec.name, {"processed_docs": 0, "shard_idx": 0})
        processed = int(s_state.get("processed_docs", 0))
        shard_idx = int(s_state.get("shard_idx", 0))

        log.info(f"Starting source={spec.name} resume_processed={processed} resume_shard_idx={shard_idx}")

        shard: List[Document] = []
        rejs: List[dict] = []
        stage_counts = {st.name: {"in": 0, "acc": 0, "rej": 0, "rej_reasons": {}} for st in stages}

        # best-effort resume: skip N records
        it = src.stream()
        for _ in range(processed):
            try:
                next(it)
            except StopIteration:
                break

        for i, raw in enumerate(it, start=processed):
            try:
                doc = _raw_to_doc(raw, policy_version=policy_version)
                accepted = True

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
                            "stage": st.name,
                            "decision": "reject",
                            "reason_code": d.reason_code,
                            "reason_detail": d.reason_detail,
                            "ts_ms": int(time.time()*1000),
                        })
                        total_rejected_docs += 1
                        break
                    else:
                        stage_counts[st.name]["acc"] += 1

                if not accepted:
                    # periodically flush analytics + rejections + checkpoint
                    if (i + 1) % log_every == 0:
                        _flush_stage_analytics(run_id, doc.source, stages, stage_counts, sink)
                        sink.flush_aggregates()
                        if rejs:
                            append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
                            rejs.clear()
                    if (i + 1) % ckpt_every == 0:
                        state["sources"][spec.name] = {"processed_docs": i + 1, "shard_idx": shard_idx}
                        ckpt.save(state)
                    continue

                shard.append(doc)
                total_written_docs += 1

                # shard flush
                if len(shard) >= shard_docs:
                    cw = get_corpus_writer(cfg.get("output", {}).get("corpus_format", "parquet"))
                    mw = get_metadata_writer(cfg.get("output", {}).get("metadata_format", "parquet_v1"))
                    cw.write(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
                    mw.write(shard, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)
                    shard.clear()
                    shard_idx += 1

                    # flush analytics, rejections, checkpoint after shard
                    _flush_stage_analytics(run_id, spec.name, stages, stage_counts, sink)
                    sink.flush_aggregates()
                    if rejs:
                        append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
                        rejs.clear()

                    state["sources"][spec.name] = {"processed_docs": i + 1, "shard_idx": shard_idx}
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
            path = os.path.join(out_dir, "docs", f"source={spec.name}", f"shard_{shard_idx:06d}.parquet")
            write_docs_shard(path, shard)
            shard.clear()
            shard_idx += 1

        # final flush for this source
        _flush_stage_analytics(run_id, spec.name, stages, stage_counts, sink)
        sink.flush_aggregates()
        if rejs:
            append_jsonl(os.path.join(out_dir, "rejections", "rejections.jsonl"), rejs)
            rejs.clear()

        state["sources"][spec.name] = {"processed_docs": i + 1 if 'i' in locals() else processed, "shard_idx": shard_idx}
        ckpt.save(state)

    # write run manifest
    manifest = {
        "run_id": run_id,
        "policy_version": policy_version,
        "total_written_docs": total_written_docs,
        "total_rejected_docs": total_rejected_docs,
        "sources": state.get("sources", {}),
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
    return Document(
        doc_id=seed,
        source=raw.source,
        text=raw.text or "",
        url=raw.url,
        license=raw.license if raw.license is not None else "Unknown",
        policy_version=policy_version,
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
