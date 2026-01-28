# Tokenizer Adapter

Your tokenizer team can plug a custom tokenizer without changing preprocessing.

## Implement
Create a module (can be separate repo/package) that implements `TokenizerAdapter`:

```python
from clean_corpus.plugins.tokenizer import TokenizerAdapter, TokenizerInfo
from clean_corpus.plugins.registry import register_tokenizer

class MyTokenizer(TokenizerAdapter):
    info = TokenizerInfo(tokenizer_id=1, name="custom_tok_v1", vocab_size=64000, type="bpe", max_context=262144)

    def __init__(self, model_path: str):
        self.model_path = model_path
        # load tokenizer artifacts here

    def encode(self, text: str):
        # return list[int]
        return ...

register_tokenizer("custom_tok", MyTokenizer(model_path="..."))
```

## Run
- Ensure the registration code runs before you call `clean-corpus build`.
  The simplest way is to create a small `bootstrap.py` that imports your tokenizer module.

Future: we can support entrypoints so tokenizers auto-discover.
