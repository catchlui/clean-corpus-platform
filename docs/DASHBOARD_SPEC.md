# Dashboard Spec (Analytics at every layer)

This package emits analytics events at every stage:
- Stored as Parquet in: `storage/analytics/events/`
- Aggregated daily in: `storage/analytics/aggregates/daily_aggregates.parquet`

## Recommended dashboards (Grafana + DuckDB)

### 1) Pipeline Health (real-time / near-real-time)
Panels:
- input_docs/sec by source (from events counts)
- accepted_docs/sec by source
- rejection rate (%) by stage
- top rejection reasons (bar chart)

### 2) Governance / Legal
Panels:
- license gate reject counts
- unknown license count (should be zero)
- PII rejects by source

### 3) Quality
Panels:
- quality_gate rejects over time
- entropy distribution (if you add entropy histograms)
- too-short rejects

### 4) Dedup
Panels:
- exact dedup rejects over time
- (future) near-dup and semantic-dup rates

## DuckDB quick query
```sql
INSTALL parquet;
LOAD parquet;

SELECT
  date,
  stage,
  source,
  input_docs,
  accepted_docs,
  rejected_docs,
  rejected_docs::DOUBLE / NULLIF(input_docs,0) AS reject_rate
FROM read_parquet('storage/analytics/aggregates/daily_aggregates.parquet')
ORDER BY date, stage, source;
```

## Notes
- This MVP stores only counts + rejection reasons per stage.
- You can extend `AnalyticsSink` to store histograms (p50/p90) for entropy/perplexity/token stats.
