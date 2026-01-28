"""Policy loader.

Policies are YAML files with simple keys used by stages.
Keeping policies in YAML allows:
- easy review by legal/compliance
- versioned configuration across runs
- non-engineers to propose changes safely
"""

from __future__ import annotations
from typing import Any, Dict
import yaml

def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
