"""Unicode NFC normalization stage.

Applies Unicode NFC (Canonical Composition) normalization at record level
to ensure Indic script consistency and fix broken encodings.

This stage should run early in the pipeline, typically after sanitize.
"""

from __future__ import annotations
from ..pipeline.context import Document, Decision
from ..utils.text import normalize_unicode_nfc
from .base import Stage

class UnicodeNormalize(Stage):
    name = "unicode_normalize"
    layer = "preprocessing"

    def __init__(self, enabled: bool = True):
        self.enabled = bool(enabled)

    def apply(self, doc: Document) -> Decision:
        if not self.enabled:
            return Decision(True, self.name)
        
        # Normalize text to NFC form
        original_text = doc.text
        normalized_text = normalize_unicode_nfc(original_text)
        
        # Update document text
        doc.text = normalized_text
        
        # Track if normalization changed the text
        if original_text != normalized_text:
            doc.transform_chain.append("unicode_nfc_normalized_v1")
        else:
            doc.transform_chain.append("unicode_nfc_unchanged_v1")
        
        return Decision(True, self.name)
