"""Dolma format writer.

Dolma is a corpus format used by AI2 (Allen Institute for AI) for language model training.
Format specification:
- id: unique document identifier
- text: document text content
- metadata: nested object containing source, license, and other metadata fields
"""

from __future__ import annotations
import os
import json
from typing import Iterable, Optional, Dict, Any
from .base import CorpusWriter
from ..pipeline.context import Document


class DolmaCorpusWriter(CorpusWriter):
    """Writer for Dolma format (AI2 corpus format)."""
    name = "dolma"
    
    def __init__(
        self,
        include_all_metadata: bool = True,
        custom_metadata_fields: Optional[Dict[str, Any]] = None,
        default_data_tag: Optional[str] = None,
    ):
        """
        Initialize Dolma writer.

        Args:
            include_all_metadata: If True, include all available metadata fields
            custom_metadata_fields: Optional dict of custom metadata to add to all documents
            default_data_tag: Default data use tag (e.g. training | sft | alignment) when doc.data_tag is not set
        """
        super().__init__()
        self.include_all_metadata = include_all_metadata
        self.custom_metadata_fields = custom_metadata_fields or {}
        self.default_data_tag = default_data_tag
    
    def write_shard(
        self,
        docs: Iterable[Document],
        *,
        out_dir: str,
        source: str,
        shard_idx: int,
        document_subpath: Optional[str] = None,
    ) -> str:
        """
        Write documents in Dolma format.
        
        Dolma format:
        {
            "id": "doc_id",
            "text": "document text",
            "metadata": {
                "source": "source_name",
                "license": "license_type",
                "url": "source_url",
                "language": "lang_code",
                ... other metadata fields
            }
        }
        """
        if document_subpath:
            base = os.path.join(out_dir, "documents", document_subpath)
        else:
            base = os.path.join(out_dir, "docs", f"source={source}")
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, f"shard_{shard_idx:06d}.jsonl")
        
        with open(path, "w", encoding="utf-8") as f:
            for d in docs:
                # Build metadata object
                metadata: Dict[str, Any] = {}
                
                # Core metadata fields
                if d.source:
                    metadata["source"] = d.source
                if d.license:
                    metadata["license"] = d.license
                if d.license_version:
                    metadata["license_version"] = d.license_version
                if d.url:
                    metadata["url"] = d.url
                if d.lang:
                    metadata["language"] = d.lang
                if d.source_file:
                    metadata["source_file"] = d.source_file
                
                # Include all metadata if requested
                if self.include_all_metadata:
                    if d.tokens is not None:
                        metadata["tokens"] = d.tokens
                    if d.chars is not None:
                        metadata["chars"] = d.chars
                    if d.bytes_utf8 is not None:
                        metadata["bytes_utf8"] = d.bytes_utf8
                    if d.entropy is not None:
                        metadata["entropy"] = d.entropy
                    if d.ppl is not None:
                        metadata["ppl"] = d.ppl
                    if d.quality_score is not None:
                        metadata["quality_score"] = d.quality_score
                    if d.dup_group_id is not None:
                        metadata["dup_group_id"] = d.dup_group_id
                    if d.pii_flag:
                        metadata["pii_flag"] = d.pii_flag
                    if d.pii_types:
                        metadata["pii_types"] = d.pii_types
                    if d.policy_version:
                        metadata["policy_version"] = d.policy_version
                    if d.transform_chain:
                        metadata["transform_chain"] = d.transform_chain
                    if d.created_at_ms:
                        metadata["created_at_ms"] = d.created_at_ms
                
                # Data use tag for filtering (training | sft | alignment)
                tag = d.data_tag if getattr(d, "data_tag", None) else self.default_data_tag
                if tag is not None:
                    metadata["data_tag"] = tag

                # Add custom metadata fields from configuration
                metadata.update(self.custom_metadata_fields)

                # Add extra metadata (folder-level metadata, PDF metadata, etc.)
                # This includes: book_name, author, certificate_type, pdf_metadata, etc.
                if d.extra:
                    metadata.update(d.extra)
                
                # Build Dolma format document
                dolma_doc = {
                    "id": d.doc_id.hex(),  # Use hex representation of doc_id
                    "text": d.text,
                    "metadata": metadata
                }
                
                # Write as JSONL
                f.write(json.dumps(dolma_doc, ensure_ascii=False) + "\n")
        
        return path
