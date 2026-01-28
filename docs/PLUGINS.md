# Plugins

## Stages
Enable in `configs/build.yaml`:

```yaml
stages:
  - license_gate
  - sanitize
  - exact_dedup
  - near_dup_minhash
  - semantic_simhash
  - quality_gate
  - pii_gate
  - tokenize
```

### near_dup_minhash
- Uses datasketch MinHashLSH
- Default: soft gate (clusters/marks, does not hard reject)
- Can be configured to hard reject for strict corpora

### semantic_simhash
- Fast SimHash signature for semantic-ish similarity
- Designed for clustering/analytics (hard reject only with conservative thresholds)

## Tokenizer Adapter
See `docs/TOKENIZER_ADAPTER.md`.
