#!/usr/bin/env bash
set -euo pipefail
pip install -e .
clean-corpus build --config configs/build.yaml
echo "Done. See storage/ for outputs."
