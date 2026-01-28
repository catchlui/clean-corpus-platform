from __future__ import annotations
from abc import ABC, abstractmethod
import pyarrow as pa

class RayBatchStage(ABC):
    name: str
    layer: str

    @abstractmethod
    def run(
        self,
        batch: pa.Table,
        *,
        run_id: str,
        source: str,
        analytics
    ) -> pa.Table:
        ...
