from __future__ import annotations
import os, json
from typing import Iterable, Optional
from .base import CorpusWriter
from ..pipeline.context import Document

class JSONLCorpusWriter(CorpusWriter):
    name = "jsonl"

    def write_shard(
        self,
        docs: Iterable[Document],
        *,
        out_dir: str,
        source: str,
        shard_idx: int,
        document_subpath: Optional[str] = None,
    ) -> str:
        if document_subpath:
            base = os.path.join(out_dir, "documents", document_subpath)
        else:
            base = os.path.join(out_dir, "docs", f"source={source}")
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, f"shard_{shard_idx:06d}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for d in docs:
                doc_dict = {
                    "doc_id": d.doc_id.hex(),
                    "text": d.text,
                    "source": d.source,
                    "lang": d.lang,
                    "url": d.url,
                    "license": d.license,
                    "license_version": d.license_version,
                    "source_file": d.source_file,
                    "data_tag": getattr(d, "data_tag", None),
                    "tokens": d.tokens,
                    "chars": d.chars,
                    "bytes_utf8": d.bytes_utf8,
                    "entropy": d.entropy,
                    "ppl": d.ppl,
                    "quality_score": d.quality_score,
                    "dup_group_id": d.dup_group_id,
                    "pii_flag": d.pii_flag,
                    "pii_types": d.pii_types,
                    "policy_version": d.policy_version,
                    "transform_chain": d.transform_chain,
                    "created_at_ms": d.created_at_ms,
                }
                # Add extra metadata (folder-level metadata, PDF metadata, etc.)
                # This includes: book_name, author, certificate_type, pdf_metadata, etc.
                if d.extra:
                    doc_dict.update(d.extra)
                
                f.write(json.dumps(doc_dict, ensure_ascii=False) + "\n")
        return path
