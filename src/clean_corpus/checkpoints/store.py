"""Checkpoint store.

Purpose:
- allow long-running builds to resume after crashes/restarts
- preserve progress counters per source
- preserve shard indices
- support different resume modes: start from beginning, specific checkpoint, or ignore data

This is a *best-effort* approach for streaming sources.
For strict reproducibility, use snapshot sources and deterministic sharding.

Checkpoint schema (JSON):
{
  "run_id": "...",
  "updated_at_ms": 123,
  "start_time_ms": 123,
  "resume_mode": "auto" | "beginning" | "checkpoint" | "ignore",
  "checkpoint_id": "optional_checkpoint_id",
  "sources": {
    "common_pile": {
      "processed_docs": 100000,
      "shard_idx": 12,
      "file_stats": {"file1.jsonl": {"processed": 1000, "written": 950, "rejected": 50}}
    }
  }
}
"""

from __future__ import annotations
from typing import Dict, Any, Optional
import os, json, time
import glob

class CheckpointStore:
    """Enhanced checkpoint store with resume mode support."""
    
    RESUME_MODES = {"auto", "beginning", "checkpoint", "ignore"}
    
    def __init__(self, out_dir: str, run_id: str, global_checkpoint_dir: Optional[str] = None):
        """
        Initialize checkpoint store.
        
        Args:
            out_dir: Output directory for storage (metadata/docs go here)
            run_id: Run identifier
            global_checkpoint_dir: Global directory for checkpoints (if None, uses out_dir/checkpoints)
        """
        self.run_id = run_id
        self.out_dir = out_dir
        
        # Use global checkpoint directory if provided, otherwise use storage-level
        if global_checkpoint_dir:
            self.checkpoint_dir = global_checkpoint_dir
        else:
            self.checkpoint_dir = os.path.join(out_dir, "checkpoints")
        
        self.path = os.path.join(self.checkpoint_dir, f"{run_id}.json")
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def load(self, resume_mode: str = "auto", checkpoint_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Load checkpoint state.
        
        Args:
            resume_mode: How to resume:
                - "auto": Use existing checkpoint if available, otherwise start from beginning
                - "beginning": Ignore existing checkpoint, start from beginning
                - "checkpoint": Load specific checkpoint by ID
                - "ignore": Ignore all checkpoints and data, start fresh
            checkpoint_id: Specific checkpoint ID to load (only used if resume_mode="checkpoint")
        
        Returns:
            Checkpoint state dictionary
        """
        if resume_mode == "beginning" or resume_mode == "ignore":
            return {"sources": {}, "resume_mode": resume_mode}
        
        if resume_mode == "checkpoint" and checkpoint_id:
            # Load specific checkpoint
            checkpoint_path = self._find_checkpoint(checkpoint_id)
            if checkpoint_path and os.path.exists(checkpoint_path):
                with open(checkpoint_path, "r", encoding="utf-8") as f:
                    state = json.load(f)
                    state["resume_mode"] = resume_mode
                    state["checkpoint_id"] = checkpoint_id
                    return state
            else:
                # Fallback to current checkpoint if specific one not found
                if os.path.exists(self.path):
                    with open(self.path, "r", encoding="utf-8") as f:
                        state = json.load(f)
                        state["resume_mode"] = resume_mode
                        return state
        
        # Auto mode: load current checkpoint if exists
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                state = json.load(f)
                state["resume_mode"] = resume_mode
                return state
        
        return {"sources": {}, "resume_mode": resume_mode}

    def _find_checkpoint(self, checkpoint_id: str) -> Optional[str]:
        """Find checkpoint file by ID (supports partial matches)."""
        # Try exact match first
        exact_path = os.path.join(self.checkpoint_dir, f"{checkpoint_id}.json")
        if os.path.exists(exact_path):
            return exact_path
        
        # Try partial match (checkpoint_id as prefix)
        pattern = os.path.join(self.checkpoint_dir, f"{checkpoint_id}*.json")
        matches = glob.glob(pattern)
        if matches:
            return matches[0]  # Return first match
        
        return None

    def save(self, state: Dict[str, Any]) -> None:
        """Save checkpoint state."""
        state["updated_at_ms"] = int(time.time() * 1000)
        if "run_id" not in state:
            state["run_id"] = self.run_id
        
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.path)
    
    def list_checkpoints(self) -> list[Dict[str, Any]]:
        """List all available checkpoints for this run."""
        checkpoints = []
        pattern = os.path.join(self.checkpoint_dir, f"{self.run_id}*.json")
        for path in glob.glob(pattern):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    ckpt = json.load(f)
                    checkpoints.append({
                        "path": path,
                        "run_id": ckpt.get("run_id", self.run_id),
                        "updated_at_ms": ckpt.get("updated_at_ms", 0),
                        "sources": list(ckpt.get("sources", {}).keys()),
                    })
            except Exception:
                continue
        
        return sorted(checkpoints, key=lambda x: x["updated_at_ms"], reverse=True)