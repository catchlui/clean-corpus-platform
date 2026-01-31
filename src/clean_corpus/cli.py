"""CLI entrypoint.

Commands:
- `clean-corpus build --config configs/build.yaml [--ray-config configs/ray.yaml]`
- `clean-corpus policy-diff --a <file.yaml> --b <file.yaml>`

Execution modes (config: execution.mode):
- local: reference implementation with checkpoints + per-stage analytics
- ray: initializes Ray then runs local (safe baseline)
- ray_data: Ray Data pipeline (ray.data.from_items -> map_batches -> write_parquet)

"""

from __future__ import annotations
import argparse
import os
import yaml
from .logging_ import setup_logging
from .pipeline.build import build_local, build_ray
from .pipeline.ray_data_build import build_ray_data
from .tools.policy_diff import main as policy_diff_main

def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def main() -> None:
    p = argparse.ArgumentParser(prog="clean-corpus")
    sub = p.add_subparsers(dest="cmd", required=True)

    pb = sub.add_parser("build")
    pb.add_argument("--config", required=True)
    pb.add_argument("--ray-config", default="configs/ray.yaml")

    pd = sub.add_parser("policy-diff")
    pd.add_argument("--a", required=True)
    pd.add_argument("--b", required=True)

    pm = sub.add_parser("monitor")
    pm.add_argument("output_dir", nargs="?", default="storage_example", help="Output directory to monitor")
    pm.add_argument("--refresh", "-r", type=float, default=5.0, metavar="SECONDS", help="Refresh interval in seconds (default: 5.0)")
    pm.add_argument("--unified", action="store_true", help="Use unified Monitor+Analytics app (default: legacy dashboard)")
    pm.add_argument("--debug", action="store_true", help="Enable debug output")
    pm.add_argument("--simple", action="store_true", help="Use simple text mode (no Rich formatting)")

    args = p.parse_args()

    if args.cmd == "policy-diff":
        print(policy_diff_main(args.a, args.b))
        return

    if args.cmd == "monitor":
        if args.unified:
            from .monitor.unified_app import create_unified_app
            create_unified_app(args.output_dir, args.refresh)
        else:
            from .monitor.dashboard import create_dashboard
            create_dashboard(args.output_dir, args.refresh, use_simple_mode=args.simple, debug=args.debug)
        return

    cfg = _load_yaml(args.config)
    from .run_id import resolve_run_id, resolve_out_dir
    run_id = resolve_run_id(cfg)
    out_dir = resolve_out_dir(cfg, run_id)
    setup_logging(out_dir=out_dir, run_id=run_id)
    
    # Store config path in environment for manifest
    os.environ['CLEAN_CORPUS_CONFIG_PATH'] = os.path.abspath(args.config)

    mode = cfg.get("execution", {}).get("mode", "local").lower()
    if mode == "ray_data":
        ray_cfg = _load_yaml(args.ray_config)
        build_ray_data(cfg, ray_cfg)
    elif mode == "ray":
        ray_cfg = _load_yaml(args.ray_config)
        build_ray(cfg, ray_cfg)
    else:
        build_local(cfg)
