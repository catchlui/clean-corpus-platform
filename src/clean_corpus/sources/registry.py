"""Source registry.

Adding a new source:
1) implement a DataSource subclass in `clean_corpus.sources.*`
2) register it here under a new `kind` key (static) OR use register_source() (dynamic)
3) reference it in build.yaml

Dynamic registration allows adding sources at runtime without modifying this file.
Each source processes independently - adding new sources has no impact on existing ones.

"""

from __future__ import annotations
from typing import Dict, Callable, Optional, Any
from .base import SourceSpec, DataSource
from .local_jsonl import LocalJSONLSource

# Lazy import for HF streaming source (optional dependency)
def _make_hf_stream_source(spec: SourceSpec) -> DataSource:
    """Lazy import HF streaming source."""
    try:
        from .hf_stream import HFStreamingSource
        return HFStreamingSource(spec)
    except ImportError as e:
        raise ImportError(
            f"HuggingFace datasets library not available. "
            f"Install with: pip install datasets. "
            f"Original error: {e}"
        )

# Lazy import for Web PDF source (optional dependency)
def _make_web_pdf_source(spec: SourceSpec) -> DataSource:
    """Lazy import Web PDF source."""
    try:
        from .web_pdf import WebPDFSource
        return WebPDFSource(spec, global_pdf_config=_global_pdf_config)
    except ImportError as e:
        raise ImportError(
            f"Web PDF downloader requires additional dependencies. "
            f"Install with: pip install requests beautifulsoup4 langdetect. "
            f"Original error: {e}"
        )

# Static registry (built-in sources)
_STATIC_REGISTRY: Dict[str, Callable[[SourceSpec], DataSource]] = {
    "hf_stream": _make_hf_stream_source,
    "local_jsonl": lambda spec: LocalJSONLSource(spec),
    "web_pdf": _make_web_pdf_source,
}

# PDF source (optional dependency)
# Store global PDF config for PDF sources
_global_pdf_config: Optional[Dict[str, Any]] = None

def set_global_pdf_config(config: Optional[Dict[str, Any]]) -> None:
    """Set global PDF configuration that applies to all PDF sources."""
    global _global_pdf_config
    _global_pdf_config = config

def get_global_pdf_config() -> Optional[Dict[str, Any]]:
    """Get global PDF configuration."""
    return _global_pdf_config

def _make_pdf_source(spec: SourceSpec) -> DataSource:
    """Factory function for PDF source that uses current global config."""
    from .pdf_source import PDFSource
    return PDFSource(spec, global_pdf_config=_global_pdf_config)

try:
    from .pdf_source import PDFSource
    _STATIC_REGISTRY["pdf"] = _make_pdf_source
except ImportError:
    # PDF libraries not installed, skip registration
    pass

# Dynamic registry (plugins/extensions)
_DYNAMIC_REGISTRY: Dict[str, Callable[[SourceSpec], DataSource]] = {}

def register_source(kind: str, factory: Callable[[SourceSpec], DataSource]) -> None:
    """Register a new source type dynamically.
    
    This allows adding sources at runtime without modifying registry.py.
    Useful for plugins, extensions, or team-specific sources.
    
    Args:
        kind: Source kind identifier (e.g., "s3_jsonl", "kafka_stream")
        factory: Function that takes SourceSpec and returns DataSource instance
    
    Example:
        from clean_corpus.sources.registry import register_source
        from clean_corpus.sources.base import SourceSpec
        
        def make_my_source(spec: SourceSpec):
            return MyCustomSource(spec)
        
        register_source("my_custom", make_my_source)
    """
    if kind in _STATIC_REGISTRY:
        raise ValueError(f"Source kind '{kind}' is already registered statically. Use a different name.")
    _DYNAMIC_REGISTRY[kind] = factory

def unregister_source(kind: str) -> None:
    """Unregister a dynamically registered source."""
    if kind in _DYNAMIC_REGISTRY:
        del _DYNAMIC_REGISTRY[kind]

def list_sources() -> Dict[str, str]:
    """List all registered sources (static + dynamic)."""
    all_sources = {}
    for kind in _STATIC_REGISTRY:
        all_sources[kind] = "static"
    for kind in _DYNAMIC_REGISTRY:
        all_sources[kind] = "dynamic"
    return all_sources

def make_source(spec: SourceSpec) -> DataSource:
    """Create a source instance from spec.
    
    Checks static registry first, then dynamic registry.
    Each source processes independently - no impact on other sources.
    """
    # Check static registry first
    if spec.kind in _STATIC_REGISTRY:
        return _STATIC_REGISTRY[spec.kind](spec)
    
    # Check dynamic registry
    if spec.kind in _DYNAMIC_REGISTRY:
        return _DYNAMIC_REGISTRY[spec.kind](spec)
    
    available = list(_STATIC_REGISTRY.keys()) + list(_DYNAMIC_REGISTRY.keys())
    raise ValueError(
        f"Unknown source kind: {spec.kind}. "
        f"Available: {available}. "
        f"Register dynamically with register_source() or add to registry.py"
    )
