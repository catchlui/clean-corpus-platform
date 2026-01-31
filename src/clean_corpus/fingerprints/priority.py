"""Priority-order-aware deduplication.

Priority is by document type (family) first: e.g. books > wiki > commoncrawl.
Within the same type, optionally use source-level priority; otherwise keep existing (drop incoming = random within family).
"""

from __future__ import annotations
from typing import Dict, List, Optional


def document_type_priority_rank(doc_type: str, type_order: List[str]) -> int:
    """
    Return rank of document type (lower = higher priority).
    First element in type_order has rank 0 (highest). Unknown types get worst rank.
    """
    if not type_order:
        return 0
    t = (doc_type or "").strip().lower()
    for i, label in enumerate(type_order):
        L = (label or "").strip().lower()
        if not L:
            continue
        if t == L or t.startswith(L) or L in t:
            return i
    return len(type_order)


def source_priority_rank(source: str, priority_order: List[str]) -> int:
    """
    Return rank of source (lower = higher priority).
    First element in priority_order has rank 0 (highest priority).
    Match: source name equals or starts with a priority label (case-insensitive),
    or priority label appears in source name.
    """
    if not priority_order:
        return 0
    s = (source or "").strip().lower()
    for i, label in enumerate(priority_order):
        L = (label or "").strip().lower()
        if not L:
            continue
        if s == L or s.startswith(L) or L in s:
            return i
    return len(priority_order)


def should_keep_incoming_by_priority(
    incoming_source: str,
    existing_sources: List[str],
    priority_order: List[str],
) -> bool:
    """
    When a duplicate is found: keep incoming iff it has strictly higher priority
    than all existing. Otherwise drop incoming (keep existing).
    If priority_order is empty, returns False (drop incoming = default behavior).
    """
    if not priority_order:
        return False
    incoming_rank = source_priority_rank(incoming_source, priority_order)
    existing_ranks = [source_priority_rank(s, priority_order) for s in (existing_sources or [])]
    if not existing_ranks:
        return True
    return incoming_rank < min(existing_ranks)


def should_keep_incoming_by_type_and_source(
    incoming_source: str,
    existing_sources: List[str],
    document_type_priority: List[str],
    source_to_document_type: Dict[str, str],
    source_priority: Optional[List[str]] = None,
) -> bool:
    """
    Priority by document type (family) first; within same type, optional source-level priority.

    - If incoming type ranks higher than all existing types → keep incoming.
    - If incoming type ranks lower than any existing type → drop incoming.
    - Same family (same type rank): if source_priority is set, use it to decide;
      otherwise drop incoming (keep existing = random within family).
    """
    if not document_type_priority or not source_to_document_type:
        return False
    incoming_type = source_to_document_type.get(incoming_source) or "unknown"
    existing_types = [source_to_document_type.get(s) or "unknown" for s in (existing_sources or [])]
    incoming_rank = document_type_priority_rank(incoming_type, document_type_priority)
    existing_ranks = [document_type_priority_rank(t, document_type_priority) for t in existing_types]
    if not existing_ranks:
        return True
    best_existing = min(existing_ranks)
    if incoming_rank < best_existing:
        return True
    if incoming_rank > best_existing:
        return False
    # Same family: use document-level (source) priority if configured; else drop incoming
    if source_priority:
        return should_keep_incoming_by_priority(incoming_source, existing_sources, source_priority)
    return False
