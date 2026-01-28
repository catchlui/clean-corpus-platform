import json, hashlib
from typing import Any

def stable_fingerprint(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()
