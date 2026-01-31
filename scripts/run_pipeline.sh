#!/usr/bin/env bash
# All-in-one pipeline runner (bash version)

set -euo pipefail

CONFIG="${1:-examples/build_local_jsonl.yaml}"
MONITOR="${2:-}"

echo "=========================================="
echo "Clean Corpus Platform - Pipeline Runner"
echo "=========================================="
echo "Config: $CONFIG"
echo "Started: $(date)"
echo "=========================================="

# Bootstrap PII detectors
echo ""
echo "Bootstrapping PII detectors..."
python scripts/bootstrap_pii.py

# Verify config
echo ""
echo "Verifying configuration..."
python scripts/show_sources.py "$CONFIG" || true

# Run pipeline
echo ""
echo "Running pipeline..."
clean-corpus build --config "$CONFIG"

# Show results
echo ""
echo "=========================================="
echo "Results"
echo "=========================================="
python scripts/show_run_info.py storage_example 2>/dev/null || echo "Run completed"

# Launch monitor if requested
if [ "$MONITOR" = "--monitor" ] || [ "$MONITOR" = "-m" ]; then
    echo ""
    echo "Launching monitoring dashboard..."
    clean-corpus monitor storage_example
fi

echo ""
echo "=========================================="
echo "Complete: $(date)"
echo "=========================================="
