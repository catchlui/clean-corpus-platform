from __future__ import annotations
import fnmatch
import glob
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class StorageBackend(ABC):
    """Storage abstraction for filesystems and cloud buckets."""

    @abstractmethod
    def join(self, *parts: str) -> str:
        raise NotImplementedError()

    @abstractmethod
    def exists(self, path: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        raise NotImplementedError()

    @abstractmethod
    def list_files(self, path: str, pattern: Optional[str] = None) -> List[str]:
        raise NotImplementedError()

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def write_file(self, path: str, data: bytes) -> None:
        raise NotImplementedError()


class LocalStorageBackend(StorageBackend):
    """Filesystem-backed storage backend."""

    def join(self, *parts: str) -> str:
        return os.path.join(*parts) if parts else ""

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        if not path:
            return
        os.makedirs(path, exist_ok=exist_ok)

    def list_files(self, path: str, pattern: Optional[str] = None) -> List[str]:
        if not path:
            return []
        if not os.path.exists(path):
            return []
        if pattern:
            return glob.glob(os.path.join(path, pattern))
        return [
            os.path.join(path, entry)
            for entry in os.listdir(path)
            if os.path.isfile(os.path.join(path, entry))
        ]

    def read_file(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def write_file(self, path: str, data: bytes) -> None:
        dirpath = os.path.dirname(path)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)


class S3StorageBackend(StorageBackend):
    """Minimal S3 backend for fingerprints and optional outputs."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        try:
            import boto3
        except ImportError as exc:
            raise ImportError("boto3 required for S3 storage backend") from exc

        client_kwargs: Dict[str, Any] = {}
        if region:
            client_kwargs["region_name"] = region
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if aws_access_key_id:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            client_kwargs["aws_session_token"] = aws_session_token

        self.bucket = bucket
        self.prefix = prefix.rstrip("/").lstrip("/")
        self.s3_client = boto3.client("s3", **client_kwargs)

    def _normalize_path(self, path: str) -> str:
        key = path.replace("\\", "/").lstrip("/")
        if self.prefix and not key.startswith(self.prefix):
            key = f"{self.prefix}/{key}"
        return key.strip("/")

    def join(self, *parts: str) -> str:
        clean_parts = [p.strip("/") for p in parts if p]
        return "/".join(clean_parts)

    def exists(self, path: str) -> bool:
        if not path:
            return False
        try:
            from botocore.exceptions import ClientError
        except ImportError:  # pragma: no cover
            ClientError = Exception
        key = self._normalize_path(path)
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code") if hasattr(exc, "response") else None
            if code in ("404", "NoSuchKey"):
                return False
            raise

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        # S3 is flat; directories are logical. No action required.
        return

    def list_files(self, path: str, pattern: Optional[str] = None) -> List[str]:
        key_prefix = self._normalize_path(path)
        if path and not key_prefix.endswith("/"):
            key_prefix += "/"
        paginator = self.s3_client.get_paginator("list_objects_v2")
        results: List[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if pattern:
                    if fnmatch.fnmatch(os.path.basename(key), pattern):
                        results.append(key)
                else:
                    results.append(key)
        return results

    def read_file(self, path: str) -> bytes:
        key = self._normalize_path(path)
        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def write_file(self, path: str, data: bytes) -> None:
        key = self._normalize_path(path)
        self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=data)


def get_storage_backend(storage_config: Optional[Dict[str, Any]]) -> StorageBackend:
    """Create a storage backend from configuration."""
    if not storage_config:
        return LocalStorageBackend()
    storage_type = storage_config.get("type", "local").lower()
    if storage_type == "local":
        return LocalStorageBackend()
    if storage_type == "s3":
        bucket = storage_config.get("bucket")
        if not bucket:
            raise ValueError("S3 storage requires a 'bucket' value")
        return S3StorageBackend(
            bucket=bucket,
            prefix=storage_config.get("prefix", ""),
            region=storage_config.get("region"),
            endpoint_url=storage_config.get("endpoint_url"),
            aws_access_key_id=storage_config.get("aws_access_key_id"),
            aws_secret_access_key=storage_config.get("aws_secret_access_key"),
            aws_session_token=storage_config.get("aws_session_token"),
        )
    raise ValueError(f"Unknown storage type: {storage_type}")
