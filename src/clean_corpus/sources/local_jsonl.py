"""Local JSONL batch source (example).

Each line should be JSON with at least:
- text
Optional:
- id, url, license

Supports multiple input formats:
- Single file: "path/to/file.jsonl"
- Multiple files: ["path/to/file1.jsonl", "path/to/file2.jsonl"]
- Directory: "path/to/directory/" (processes all .jsonl files)
- Glob pattern: "path/to/*.jsonl" or "path/to/**/*.jsonl"

This is a practical template for adding internal corpora exports.
"""

from __future__ import annotations
import json
import os
import glob
from pathlib import Path
from typing import Iterable, List, Union, Any, Dict
from .base import DataSource, DataSourceType, RawDocument, SourceSpec

class LocalJSONLSource(DataSource):
    source_type = DataSourceType.BATCH

    def __init__(self, spec: SourceSpec):
        self.spec = spec
        self.name = spec.name
        self.files = self._resolve_files(spec.dataset)

    def _resolve_files(self, dataset: Union[str, List[str]]) -> List[str]:
        """Resolve dataset specification to list of file paths.
        
        Supports:
        - Single file path (string)
        - List of file paths
        - Directory path (processes all .jsonl files)
        - Glob pattern
        """
        files = []
        
        # Handle list of files
        if isinstance(dataset, list):
            for item in dataset:
                files.extend(self._resolve_files(item))
            return files
        
        # Handle string path
        if isinstance(dataset, str):
            path = Path(dataset)
            
            # Check if it's a glob pattern
            if '*' in dataset or '?' in dataset or '[' in dataset:
                matched_files = glob.glob(dataset, recursive=True)
                files.extend([f for f in matched_files if os.path.isfile(f) and f.endswith('.jsonl')])
                return sorted(files)
            
            # Check if it's a directory
            if path.is_dir():
                jsonl_files = list(path.glob("*.jsonl"))
                jsonl_files.extend(path.glob("**/*.jsonl"))  # Recursive
                files.extend([str(f) for f in jsonl_files if f.is_file()])
                return sorted(files)
            
            # Single file
            if path.is_file():
                return [str(path)]
            
            # File doesn't exist - return as-is (will error later if needed)
            return [dataset]
        
        # Fallback: treat as single file
        return [str(dataset)]

    def metadata(self) -> Dict[str, Any]:
        return {
            "kind": "local_jsonl",
            "files": self.files,
            "file_count": len(self.files),
            "total_size_bytes": sum(os.path.getsize(f) for f in self.files if os.path.exists(f))
        }

    def stream(self) -> Iterable[RawDocument]:
        """Stream documents from all configured JSONL files."""
        for file_path in self.files:
            if not os.path.exists(file_path):
                import logging
                logging.getLogger("clean_corpus.sources.local_jsonl").warning(
                    f"File not found: {file_path}, skipping"
                )
                continue
            
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, start=1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            ex = json.loads(line)
                            yield RawDocument(
                                raw_id=str(ex.get("id", f"{Path(file_path).stem}_{line_num}")),
                                text=ex.get(self.spec.text_field, "") or "",
                                source=self.spec.name,
                                url=ex.get(self.spec.url_field),
                                license=ex.get(self.spec.license_field),
                                created_at=ex.get("created_at"),
                                extra={
                                    **ex,
                                    "source_file": file_path,
                                    "source_line": line_num
                                },
                            )
                        except json.JSONDecodeError as e:
                            import logging
                            logging.getLogger("clean_corpus.sources.local_jsonl").warning(
                                f"Invalid JSON in {file_path}:{line_num}: {e}"
                            )
                            continue
            except Exception as e:
                import logging
                logging.getLogger("clean_corpus.sources.local_jsonl").error(
                    f"Error reading file {file_path}: {e}"
                )
                continue
