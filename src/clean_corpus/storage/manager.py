"""Storage manager for handling different storage backends.

Supports:
- Global storage configuration
- Per-output storage configuration
- Automatic backend selection
"""

from __future__ import annotations
from typing import Dict, Any, Optional
from .base import StorageBackend, LocalStorageBackend, get_storage_backend

class StorageManager:
    """Manages storage backends for different output types."""
    
    def __init__(self, global_storage_config: Optional[Dict[str, Any]] = None):
        self.global_config = global_storage_config or {}
        self._backends: Dict[str, StorageBackend] = {}
        self._default_backend: Optional[StorageBackend] = None
        
        # Initialize default backend
        if self.global_config:
            self._default_backend = get_storage_backend(self.global_config)
        else:
            self._default_backend = LocalStorageBackend()
    
    def get_backend(self, output_type: str = "default", storage_config: Optional[Dict[str, Any]] = None) -> StorageBackend:
        """Get storage backend for output type.
        
        Args:
            output_type: Type of output (docs, metadata, analytics, etc.)
            storage_config: Optional per-output storage config (overrides global)
        """
        # Check cache
        cache_key = f"{output_type}_{id(storage_config) if storage_config else 'default'}"
        if cache_key in self._backends:
            return self._backends[cache_key]
        
        # Use per-output config if provided, else global
        config = storage_config or self.global_config.get(output_type) or self.global_config
        
        if config:
            backend = get_storage_backend(config)
        else:
            backend = self._default_backend
        
        self._backends[cache_key] = backend
        return backend
    
    def get_path(self, output_type: str, *parts: str, storage_config: Optional[Dict[str, Any]] = None) -> str:
        """Get storage path for output type."""
        backend = self.get_backend(output_type, storage_config)
        return backend.join(*parts)

# Global storage manager instance
_global_storage_manager: Optional[StorageManager] = None

def get_storage_manager(config: Optional[Dict[str, Any]] = None) -> StorageManager:
    """Get or create global storage manager."""
    global _global_storage_manager
    if _global_storage_manager is None or config is not None:
        storage_config = config.get('storage', {}) if config else {}
        _global_storage_manager = StorageManager(storage_config)
    return _global_storage_manager
