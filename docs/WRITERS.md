# Writers: corpus vs metadata

This platform supports configurable output formats for corpus and metadata.

- Analytics: always Parquet (events + aggregates)
- Corpus: selectable (parquet or jsonl in v0.5)
- Metadata: Parquet with schema versioning (meta_v1)

## Config
```yaml
output:
  corpus_format: parquet        # parquet | jsonl
  metadata_format: parquet_v1   # currently only parquet_v1
```

## Why split corpus and metadata?
- Curriculum and governance teams often need metadata without access to raw text
- Metadata schema must be versioned for long-lived pipelines
- Training can consume corpus shards while dashboards consume analytics

## Metadata schema versioning
Metadata shards are stored under:
`storage/metadata/schema=meta_v1/source=<source>/shard_*.parquet`

Add new metadata fields by:
- creating `ParquetMetadataWriterV2` with `schema_version=meta_v2`
- updating registry to include `parquet_v2`
