# Ray Data Pipeline Mode

Set:
```yaml
execution:
  mode: ray_data
```

Then run:
```bash
ray start --head
clean-corpus build --config configs/build.yaml --ray-config configs/ray.yaml
```

Current implementation uses `ray.data.from_items` fed by streaming chunks on the driver.
For production scale, replace ingestion with:
- `ray.data.read_parquet` (batch sources on S3/HDFS)
- `ray.data.read_text` / `read_datasource` for custom readers

Stages are applied inside `map_batches` and Parquet written via `write_parquet`.
Analytics are emitted per batch and stored to `storage/analytics/`.
