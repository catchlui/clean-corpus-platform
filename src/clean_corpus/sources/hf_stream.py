"""Hugging Face streaming source.

This source is suitable for large corpora that you do not want to download fully.
Resume behavior: best-effort (skip N processed docs on restart).

Supports both streaming from HuggingFace Hub and loading from locally downloaded datasets.
For local datasets downloaded via `huggingface-cli download`, set the `data_dir` field
in the source spec, or ensure the dataset is in the HuggingFace cache directory.
"""

from __future__ import annotations
from datasets import load_dataset
from typing import Iterable, Optional
import os
from .base import DataSource, DataSourceType, RawDocument, SourceSpec

class HFStreamingSource(DataSource):
    source_type = DataSourceType.STREAMING

    def __init__(self, spec: SourceSpec):
        self.spec = spec
        self.name = spec.name
        # Note: For local directories downloaded via huggingface-cli download --local-dir,
        # the datasets library will automatically use the HuggingFace cache.
        # If you need to use a custom directory, convert to JSONL first (see convert_hf_to_jsonl.py)

    def metadata(self):
        return {
            "kind": "hf_stream",
            "dataset": self.spec.dataset,
            "split": self.spec.split,
        }

    def stream(self) -> Iterable[RawDocument]:
        # Load dataset - will automatically use HuggingFace cache if available
        # If you downloaded via huggingface-cli download, it goes to the cache
        # and will be used automatically here
        config = getattr(self.spec, "config", None)
        if config:
            ds = load_dataset(
                self.spec.dataset,
                config,
                split=self.spec.split,
                streaming=True,
            )
        else:
            ds = load_dataset(
                self.spec.dataset,
                split=self.spec.split,
                streaming=True,
            )
        
        for ex in ds:
            yield RawDocument(
                raw_id=str(ex.get("id", "")),
                text=ex.get(self.spec.text_field, "") or "",
                source=self.spec.name,
                url=ex.get(self.spec.url_field),
                license=ex.get(self.spec.license_field),
                created_at=None,
                extra=ex,
            )
