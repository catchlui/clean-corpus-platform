"""Local JSONL batch source (example).

Each line should be JSON with at least:
- text
Optional:
- id, url, license

This is a practical template for adding internal corpora exports.
"""

from __future__ import annotations
import json
from typing import Iterable
from .base import DataSource, DataSourceType, RawDocument, SourceSpec

class LocalJSONLSource(DataSource):
    source_type = DataSourceType.BATCH

    def __init__(self, spec: SourceSpec):
        self.spec = spec
        self.name = spec.name

    def metadata(self):
        return {"kind": "local_jsonl", "path": self.spec.dataset}

    def stream(self) -> Iterable[RawDocument]:
        with open(self.spec.dataset, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ex = json.loads(line)
                yield RawDocument(
                    raw_id=str(ex.get("id", "")),
                    text=ex.get(self.spec.text_field, "") or "",
                    source=self.spec.name,
                    url=ex.get(self.spec.url_field),
                    license=ex.get(self.spec.license_field),
                    created_at=ex.get("created_at"),
                    extra=ex,
                )
