# End-to-end example: add a new data source + new PII detector

## 1) Add a new data source
- Implement in `src/clean_corpus/sources/<your_source>.py`
- Register in `src/clean_corpus/sources/registry.py`
- Add to config under `sources:`

This repo includes `LocalJSONLSource` as an example.

Run:
```bash
python scripts/bootstrap_pii.py
clean-corpus build --config examples/build_local_jsonl.yaml
```

Outputs appear in `storage_example/`.

## 2) Add a new PII detector
- Implement `PIIDetector` in `src/clean_corpus/pii/detectors/`
- Register using `register_detector()` in your bootstrap or orchestration layer

Example included:
- `EmailDetector`, `PhoneDetector`, `AadhaarDetector`

## 3) Policy diff
```bash
clean-corpus policy-diff --a src/clean_corpus/policies/defaults/pii.yaml --b src/clean_corpus/policies/defaults/pii.yaml
```

Use policy diff in CI to enforce review of policy changes.
