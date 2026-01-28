"""clean_corpus

Policy-driven, restartable, analytics-first corpus preprocessing platform.

Public API surface:
- clean_corpus.cli.main : CLI entrypoint
- clean_corpus.pipeline.build.build_local / build_ray : run pipeline
- clean_corpus.sources : add/extend data sources
- clean_corpus.stages : add/extend pipeline stages
- clean_corpus.analytics : analytics sinks and schemas

This package is intentionally modular so multiple teams can own different pieces.
"""
__all__ = ["__version__"]
__version__ = "0.6.0"
