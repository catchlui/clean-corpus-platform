"""Source registry.

Adding a new source:
1) implement a DataSource subclass in `clean_corpus.sources.*`
2) register it here under a new `kind` key
3) reference it in build.yaml

"""

from __future__ import annotations
from .base import SourceSpec
from .hf_stream import HFStreamingSource
from .local_jsonl import LocalJSONLSource

def make_source(spec: SourceSpec):
    if spec.kind == "hf_stream":
        return HFStreamingSource(spec)
    if spec.kind == "local_jsonl":
        return LocalJSONLSource(spec)
    raise ValueError(f"Unknown source kind: {spec.kind}")
