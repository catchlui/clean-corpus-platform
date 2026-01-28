"""Hugging Face streaming source.

This source is suitable for large corpora that you do not want to download fully.
Resume behavior: best-effort (skip N processed docs on restart).
"""

from __future__ import annotations
from datasets import load_dataset
from typing import Iterable
from .base import DataSource, DataSourceType, RawDocument, SourceSpec

class HFStreamingSource(DataSource):
    source_type = DataSourceType.STREAMING

    def __init__(self, spec: SourceSpec):
        self.spec = spec
        self.name = spec.name

    def metadata(self):
        return {
            "kind": "hf_stream",
            "dataset": self.spec.dataset,
            "split": self.spec.split,
        }

    def stream(self) -> Iterable[RawDocument]:
        ds = load_dataset(self.spec.dataset, split=self.spec.split, streaming=True)
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
