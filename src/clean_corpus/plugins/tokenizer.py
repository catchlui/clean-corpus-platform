"""Tokenizer adapter plugin.

Your tokenizer team should implement this interface in a separate package or in this repo,
and register it via `clean_corpus.plugins.registry`.

Design goals:
- Data preprocessing can run without knowing tokenizer internals
- Tokenizer can be swapped/versioned without touching preprocessing stages
- Supports long-context window analytics (4K -> 256K) later

Minimal contract:
- encode(text) -> list[int]
- optional: encode_batch(list[str]) -> list[list[int]] for speed

We store only counts/statistics, not token lists, to keep storage small.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Sequence

@dataclass(frozen=True)
class TokenizerInfo:
    tokenizer_id: int
    name: str
    vocab_size: int
    type: str  # bpe | unigram | byte | hybrid
    max_context: int

class TokenizerAdapter(ABC):
    """Base tokenizer adapter."""
    info: TokenizerInfo

    @abstractmethod
    def encode(self, text: str) -> List[int]:
        """Tokenize a string into token IDs."""
        raise NotImplementedError

    def encode_batch(self, texts: Sequence[str]) -> List[List[int]]:
        """Optional fast path; default falls back to single encode."""
        return [self.encode(t) for t in texts]
