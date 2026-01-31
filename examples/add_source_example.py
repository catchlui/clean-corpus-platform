"""Example: Adding a new source dynamically without modifying registry.py.

This demonstrates how to add a new source type at runtime,
ensuring no impact on existing sources or processes.
"""

from clean_corpus.sources.base import DataSource, RawDocument, SourceSpec
from clean_corpus.sources.registry import register_source, list_sources
from typing import Iterable
import json

# Example: Custom source that reads from a custom API
class CustomAPISource(DataSource):
    """Example custom source."""
    
    def __init__(self, spec: SourceSpec):
        self.spec = spec
        self.name = spec.name
    
    def stream(self) -> Iterable[RawDocument]:
        """Stream documents from custom API."""
        # Your implementation here
        # This is just an example
        api_data = [
            {"id": "1", "text": "Sample text", "url": "https://example.com/1"},
            {"id": "2", "text": "More text", "url": "https://example.com/2"},
        ]
        
        for item in api_data:
            yield RawDocument(
                raw_id=item["id"].encode(),
                text=item["text"],
                source=self.spec.name,
                url=item.get("url"),
                license=item.get("license"),
                extra=item.get("extra", {})
            )

# Register the new source dynamically
def make_custom_api_source(spec: SourceSpec) -> DataSource:
    """Factory function for CustomAPISource."""
    return CustomAPISource(spec)

# Register it
register_source("custom_api", make_custom_api_source)

# Verify registration
print("Registered sources:")
for kind, source_type in list_sources().items():
    print(f"  {kind}: {source_type}")

# Now you can use it in config:
# sources:
#   - name: "my_api_data"
#     kind: "custom_api"
#     # ... your config
