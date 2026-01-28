"""Plugin registry.

This enables new add-ons without modifying core pipeline code.

Plugins supported:
- Tokenizer adapters
- Batch stages (Ray Data pipeline)
- Curriculum 'views' (filters + eligibility + sampling metadata)

For now, we keep a simple in-process registry.
In a larger org, you can back this with entry-points or service discovery.
"""

from __future__ import annotations
from typing import Dict, Type, Optional
from .tokenizer import TokenizerAdapter

_TOKENIZERS: Dict[str, TokenizerAdapter] = {}

def register_tokenizer(name: str, tok: TokenizerAdapter) -> None:
    _TOKENIZERS[name] = tok

def get_tokenizer(name: str) -> Optional[TokenizerAdapter]:
    return _TOKENIZERS.get(name)
