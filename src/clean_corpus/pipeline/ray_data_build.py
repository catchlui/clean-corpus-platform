"""Ray Data pipeline runner with **true per-stage analytics**.

Pipeline:
ray.data.from_items (or read_*) ->
  map_batches(stage1) -> emit analytics
  map_batches(stage2) -> emit analytics
  ...
-> write corpus shards in desired format (parquet/jsonl)
-> write metadata shards (parquet schema versioned)

Analytics:
- Emitted per stage via AnalyticsSink (Parquet events + daily aggregates)
- Supports metric_samples percentiles (entropy/tokens/ppl) when stages provide samples

Note:
- For simplicity, we still chunk streaming ingestion on the driver in this reference implementation.
  For production scale, use batch/snapshot sources and ray.data.read_parquet/read_text.
"""

from __future__ import annotations
from typing import Dict, Any, List, Callable
import os, logging
import ray
import ray.data
import pyarrow as pa

from ..sources.base import SourceSpec
from ..sources.registry import make_source
from ..analytics.sink import AnalyticsSink
from ..analytics.schemas import make_event
from ..checkpoints.store import CheckpointStore
from ..run_id import resolve_run_id, resolve_out_dir
from ..utils.hashing import sha256_bytes
from ..writers.registry import get_corpus_writer, get_metadata_writer

log = logging.getLogger("clean_corpus.ray_data")

def build_ray_data(cfg: Dict[str, Any], ray_cfg: Dict[str, Any]) -> None:
    run = cfg["run"]
    run_id = resolve_run_id(cfg)
    out_dir = resolve_out_dir(cfg, run_id)
    run["run_id"] = run_id
    run["out_dir"] = out_dir
    ckpt_every = int(run.get("checkpoint_every_docs", 10_000))
    policy_version = run.get("policy_version", "policy_v0")

    addr = ray_cfg.get("ray", {}).get("address", "auto")
    ray.init(address=addr, ignore_reinit_error=True)

    os.makedirs(out_dir, exist_ok=True)
    sink = AnalyticsSink(out_dir=out_dir, run_id=run_id)
    ckpt = CheckpointStore(out_dir=out_dir, run_id=run_id)
    state = ckpt.load()
    state.setdefault("sources", {})

    tokenizer_name = cfg.get("tokenizer", {}).get("name", "custom_tok")

    # Set global PDF configuration if provided
    if "pdf" in cfg:
        from ..sources.registry import set_global_pdf_config
        set_global_pdf_config(cfg["pdf"])

    # stages (doc-level stages; we execute per row inside map_batches)
    from ..stages.registry import make_stages
    stages = make_stages(cfg.get("stages", []), cfg["policies"], tokenizer_name=tokenizer_name)

    # writers (register s3_parquet when output is to S3)
    out_cfg = cfg.get("output", {}) or {}
    corpus_format = out_cfg.get("corpus_format", "parquet")
    if corpus_format == "s3_parquet" and run.get("storage") and run["storage"].get("type") == "s3":
        from ..writers.registry import list_corpus_writers, register_corpus_writer
        if "s3_parquet" not in list_corpus_writers():
            from ..storage.base import get_storage_backend
            from ..writers.s3_parquet import S3ParquetCorpusWriter
            backend = get_storage_backend(run["storage"])
            register_corpus_writer("s3_parquet", S3ParquetCorpusWriter(backend))
    corpus_writer = get_corpus_writer(corpus_format)
    meta_writer = get_metadata_writer(out_cfg.get("metadata_format", "parquet_v1"))

    for s_cfg in cfg["sources"]:
        spec = SourceSpec(**s_cfg)
        src = make_source(spec)
        s_state = state["sources"].get(spec.name, {"processed_docs": 0, "shard_idx": 0})
        processed = int(s_state.get("processed_docs", 0))
        shard_idx = int(s_state.get("shard_idx", 0))

        log.info(f"[ray.data] source={spec.name} resume_processed={processed} resume_shard_idx={shard_idx}")

        it = src.stream()
        # best-effort resume: skip N
        for _ in range(processed):
            try: next(it)
            except StopIteration: break

        chunk = 20_000
        processed_local = processed

        while True:
            buf = []
            try:
                for _ in range(chunk):
                    raw = next(it)
                    buf.append({
                        "raw_id": raw.raw_id,
                        "text": raw.text,
                        "source": raw.source,
                        "url": raw.url,
                        "license": raw.license if raw.license is not None else "Unknown",
                        "policy_version": policy_version,
                    })
            except StopIteration:
                pass

            if not buf:
                break

            ds = ray.data.from_items(buf)

            # Apply each stage as its own map_batches for per-stage analytics
            for st in stages:
                ds = ds.map_batches(
                    lambda b, _st=st: _run_stage_batch(b, _st, run_id, spec.name, sink),
                    batch_size=512,
                    batch_format="pyarrow",
                )

            # Write outputs
            # Ray writes Parquet natively; for JSONL or custom we collect blocks and write ourselves.
            if corpus_writer.name == "parquet":
                out_path = os.path.join(out_dir, "docs", f"source={spec.name}", f"ray_shard_{shard_idx:06d}")
                ds.write_parquet(out_path)
            else:
                # Collect to driver in manageable blocks and write via writer
                docs = _collect_docs(ds)
                corpus_writer.write_shard(docs, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)

            # Metadata always written via MetadataWriter (parquet schema versioned)
            docs_meta = _collect_docs(ds)
            meta_writer.write_shard(docs_meta, out_dir=out_dir, source=spec.name, shard_idx=shard_idx)

            shard_idx += 1
            processed_local += len(buf)

            # checkpoint + flush
            if processed_local % ckpt_every < chunk:
                state["sources"][spec.name] = {"processed_docs": processed_local, "shard_idx": shard_idx}
                ckpt.save(state)
                sink.flush_aggregates()

        state["sources"][spec.name] = {"processed_docs": processed_local, "shard_idx": shard_idx}
        ckpt.save(state)
        sink.flush_aggregates()

def _run_stage_batch(batch: pa.Table, st, run_id: str, source: str, sink: AnalyticsSink) -> pa.Table:
    rows = batch.to_pylist()
    accepted = []
    rejected = 0
    rej_breakdown: Dict[str,int] = {}
    # metric samples collected opportunistically
    entropy_s = []
    tokens_s = []
    ppl_s = []

    from ..pipeline.context import Document

    for r in rows:
        doc = _row_to_doc(r)
        d = st.apply(doc)
        if not d.accepted:
            rejected += 1
            rc = d.reason_code or "REJECT"
            rej_breakdown[rc] = rej_breakdown.get(rc, 0) + 1
            continue
        # sample metrics from doc after stage (if present)
        if doc.entropy is not None: entropy_s.append(float(doc.entropy))
        if doc.tokens is not None: tokens_s.append(float(doc.tokens))
        if doc.ppl is not None: ppl_s.append(float(doc.ppl))
        accepted.append(_doc_to_row(doc))

    ev = make_event(
        run_id=run_id,
        stage=st.name,
        source=source,
        layer=getattr(st, "layer", "preprocessing"),
        counts={"input_docs": len(rows), "accepted_docs": len(accepted), "rejected_docs": rejected},
        metrics={},
        rejection_breakdown=rej_breakdown,
    )
    ev["metric_samples"] = {}
    if entropy_s: ev["metric_samples"]["entropy"] = entropy_s
    if tokens_s: ev["metric_samples"]["tokens"] = tokens_s
    if ppl_s: ev["metric_samples"]["ppl"] = ppl_s
    sink.emit(ev)

    return pa.Table.from_pylist(accepted)

def _row_to_doc(row: dict):
    from ..pipeline.context import Document
    seed = sha256_bytes((row.get("text","") or "")[:512])
    return Document(
        doc_id=seed,
        source=row.get("source",""),
        text=row.get("text","") or "",
        url=row.get("url"),
        license=row.get("license"),
        policy_version=row.get("policy_version","policy_v0"),
    )

def _doc_to_row(doc):
    return {
        "doc_id": doc.doc_id,
        "source": doc.source,
        "lang": doc.lang,
        "text": doc.text,
        "url": doc.url,
        "license": doc.license,
        "license_version": doc.license_version,
        "tokens": doc.tokens,
        "chars": doc.chars,
        "bytes_utf8": doc.bytes_utf8,
        "entropy": doc.entropy,
        "ppl": doc.ppl,
        "quality_score": doc.quality_score,
        "dup_group_id": doc.dup_group_id,
        "pii_flag": doc.pii_flag,
        "pii_types": doc.pii_types,
        "policy_version": doc.policy_version,
        "transform_chain": doc.transform_chain,
        "created_at_ms": doc.created_at_ms,
    }

def _collect_docs(ds: ray.data.Dataset):
    # Convert dataset rows to Document objects on driver (for non-parquet writers / metadata writer).
    # For very large scale, replace with distributed writer per block.
    from ..pipeline.context import Document
    rows = ds.take_all()
    docs = []
    for r in rows:
        docs.append(Document(
            doc_id=r["doc_id"],
            source=r["source"],
            text=r.get("text",""),
            url=r.get("url"),
            license=r.get("license"),
            license_version=r.get("license_version"),
            lang=r.get("lang","en"),
            tokens=r.get("tokens"),
            chars=r.get("chars"),
            bytes_utf8=r.get("bytes_utf8"),
            entropy=r.get("entropy"),
            ppl=r.get("ppl"),
            quality_score=r.get("quality_score"),
            dup_group_id=r.get("dup_group_id"),
            pii_flag=bool(r.get("pii_flag", False)),
            pii_types=r.get("pii_types") or [],
            policy_version=r.get("policy_version","policy_v0"),
            transform_chain=r.get("transform_chain") or [],
            created_at_ms=int(r.get("created_at_ms", 0)),
        ))
    return docs
