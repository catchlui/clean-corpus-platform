#!/usr/bin/env python3
"""Clean pipeline storage for a fresh test run.

Removes:
- Global checkpoints (checkpoints/*.json)
- Global fingerprint store (fingerprints_global/)
- Run output dirs (storage/, storage_*, and any dir with docs/ or documents/ or manifests/)
- Logs (logs/*.log)

Usage:
    python scripts/clean_storage.py [--dry-run]
"""

from __future__ import annotations
import argparse
import os
import shutil
import glob

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> None:
    ap = argparse.ArgumentParser(description="Clean pipeline storage for fresh test")
    ap.add_argument("--dry-run", action="store_true", help="Print what would be removed, do not delete")
    args = ap.parse_args()
    dry = args.dry_run

    removed: list[str] = []
    # Global checkpoints
    ckpt_dir = os.path.join(ROOT, "checkpoints")
    if os.path.isdir(ckpt_dir):
        for f in glob.glob(os.path.join(ckpt_dir, "*.json")):
            if dry:
                print(f"[dry-run] would remove: {f}")
            else:
                os.remove(f)
                removed.append(f)

    # Global fingerprint store
    fp_dir = os.path.join(ROOT, "fingerprints_global")
    if os.path.isdir(fp_dir):
        if dry:
            print(f"[dry-run] would remove dir: {fp_dir}")
        else:
            shutil.rmtree(fp_dir)
            removed.append(fp_dir)

    # Common run output dirs (by name)
    for name in ["storage", "storage_class4_hindi_veena", "storage_example"]:
        path = os.path.join(ROOT, name)
        if os.path.isdir(path):
            if dry:
                print(f"[dry-run] would remove dir: {path}")
            else:
                shutil.rmtree(path)
                removed.append(path)

    # Any storage_* dir in project root
    for path in glob.glob(os.path.join(ROOT, "storage_*")):
        if os.path.isdir(path):
            if dry:
                print(f"[dry-run] would remove dir: {path}")
            else:
                shutil.rmtree(path)
                removed.append(path)

    # Logs
    log_dir = os.path.join(ROOT, "logs")
    if os.path.isdir(log_dir):
        for f in glob.glob(os.path.join(log_dir, "*.log")):
            if dry:
                print(f"[dry-run] would remove: {f}")
            else:
                os.remove(f)
                removed.append(f)

    if dry:
        print("[dry-run] Done (no files removed)")
    else:
        print(f"Cleaned {len(removed)} item(s). Fresh test ready.")


if __name__ == "__main__":
    main()
