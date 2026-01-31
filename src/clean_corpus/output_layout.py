"""Structured output layout: /processed/v1/ documents | rejected | stats.

Target structure:
  /processed/v1/
   ├── documents/
   │    ├── ncert/
   │    │    ├── en/
   │    │    │    └── physics/class11/chapter_01.jsonl
   │    │    └── hi/
   │    │         └── hindi/class10/
   │    └── dolma/
   │         ├── wikipedia/
   │         ├── books/
   │         └── web/
   ├── rejected/
   │    ├── pii/
   │    ├── duplicates/
   │    ├── corrupt/
   │    └── low_quality/
   └── stats/
      ├── pii_report.json
      ├── dedup_report.json
      └── quality_report.json
"""

from __future__ import annotations
import os
import json
import re
from typing import Any, Dict, List, Optional

# Directory names
DOCUMENTS_DIR = "documents"
REJECTED_DIR = "rejected"
STATS_DIR = "stats"

REJECTED_PII = "pii"
REJECTED_DUPLICATES = "duplicates"
REJECTED_CORRUPT = "corrupt"
REJECTED_LOW_QUALITY = "low_quality"

REJECTION_CATEGORIES = (REJECTED_PII, REJECTED_DUPLICATES, REJECTED_CORRUPT, REJECTED_LOW_QUALITY)


def get_rejection_category(reason_code: str) -> str:
    """Map stage reason_code to rejected/ subfolder."""
    if not reason_code:
        return REJECTED_LOW_QUALITY
    code = (reason_code or "").upper()
    if "DUP" in code or "DUPLICATE" in code:
        return REJECTED_DUPLICATES
    if "PII" in code or "LICENSE" in code:
        return REJECTED_PII
    if "RUNTIME" in code or "CORRUPT" in code or "ERROR" in code:
        return REJECTED_CORRUPT
    return REJECTED_LOW_QUALITY


def _normalize_for_path(s: str) -> str:
    """Safe path segment: lowercase, replace spaces/slashes with underscore."""
    if not s:
        return "unknown"
    s = re.sub(r"[\s/\\]+", "_", s.strip().lower())
    return s or "unknown"


def get_document_subpath(
    source: str,
    lang: str,
    extra: Optional[Dict[str, Any]] = None,
    source_to_namespace: Optional[Dict[str, str]] = None,
    include_domain_grade: bool = True,
) -> str:
    """
    Build relative path under documents/: namespace/lang/domain/grade.
    Example: ncert/en/physics/class11
    """
    namespace = (source_to_namespace or {}).get(source, source)
    namespace = _normalize_for_path(namespace)
    lang = _normalize_for_path(lang or "en")
    parts = [namespace, lang]
    extra = extra or {}
    if include_domain_grade:
        domain = extra.get("subject") or extra.get("book_name") or extra.get("subject_name")
        grade = extra.get("grade") or extra.get("class")
        if domain:
            parts.append(_normalize_for_path(str(domain)))
        if grade:
            parts.append(_normalize_for_path(str(grade)))
    return os.path.join(*parts)


def ensure_structured_dirs(out_dir: str) -> None:
    """Create documents/, rejected/{pii,duplicates,corrupt,low_quality}/, stats/."""
    os.makedirs(os.path.join(out_dir, DOCUMENTS_DIR), exist_ok=True)
    for cat in REJECTION_CATEGORIES:
        os.makedirs(os.path.join(out_dir, REJECTED_DIR, cat), exist_ok=True)
    os.makedirs(os.path.join(out_dir, STATS_DIR), exist_ok=True)


def rejection_path(out_dir: str, category: str, filename: str = "rejections.jsonl") -> str:
    """Full path for rejected file: out_dir/rejected/{category}/rejections.jsonl."""
    return os.path.join(out_dir, REJECTED_DIR, category, filename)


def documents_base(out_dir: str) -> str:
    """Base path for documents: out_dir/documents/."""
    return os.path.join(out_dir, DOCUMENTS_DIR)


def document_shard_path(
    out_dir: str,
    subpath: str,
    shard_idx: int,
    extension: str = "jsonl",
) -> str:
    """Full path for a shard: out_dir/documents/{subpath}/shard_000001.jsonl."""
    base = os.path.join(out_dir, DOCUMENTS_DIR, subpath)
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, f"shard_{shard_idx:06d}.{extension}")


def write_stats_reports(
    out_dir: str,
    rejection_counts_by_stage: Dict[str, Dict[str, int]],
    total_processed: int,
    total_written: int,
    total_rejected: int,
) -> None:
    """Write stats/pii_report.json, dedup_report.json, quality_report.json."""
    stats_dir = os.path.join(out_dir, STATS_DIR)
    os.makedirs(stats_dir, exist_ok=True)

    def _rejection_counts(stage_name: str) -> Dict[str, int]:
        return dict(rejection_counts_by_stage.get(stage_name, {}))

    # PII report: from pii_policy_gate
    pii_report = {
        "stage": "pii_policy_gate",
        "rejection_counts": _rejection_counts_by_stage("pii_policy_gate"),
        "total_processed": total_processed,
        "total_rejected": total_rejected,
    }
    with open(os.path.join(stats_dir, "pii_report.json"), "w", encoding="utf-8") as f:
        json.dump(pii_report, f, indent=2)

    # Dedup report: from exact_dedup, near_dup_minhash, global_dedup
    dedup_report = {
        "stages": ["exact_dedup", "near_dup_minhash", "global_dedup"],
        "rejection_counts": {
            "exact_dedup": _rejection_counts("exact_dedup"),
            "near_dup_minhash": _rejection_counts("near_dup_minhash"),
            "global_dedup": _rejection_counts("global_dedup"),
        },
        "total_processed": total_processed,
        "total_written": total_written,
        "total_rejected": total_rejected,
    }
    with open(os.path.join(stats_dir, "dedup_report.json"), "w", encoding="utf-8") as f:
        json.dump(dedup_report, f, indent=2)

    # Quality report: from quality_gate and overall
    quality_report = {
        "stage": "quality_gate",
        "rejection_counts": _rejection_counts("quality_gate"),
        "total_processed": total_processed,
        "total_written": total_written,
        "total_rejected": total_rejected,
    }
    with open(os.path.join(stats_dir, "quality_report.json"), "w", encoding="utf-8") as f:
        json.dump(quality_report, f, indent=2)
