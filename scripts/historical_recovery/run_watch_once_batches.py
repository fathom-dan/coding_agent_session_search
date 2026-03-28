#!/usr/bin/env python3

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run native 'cass index --watch-once' in resumable batches so large raw "
            "session trees can be reconciled without whole-root OOM failures."
        )
    )
    parser.add_argument(
        "--cass-binary",
        default="/data/projects/.cargo-target-cass-release/release/cass",
        help="Path to the cass binary to invoke.",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="cass data dir that contains agent_search.db.",
    )
    parser.add_argument(
        "--root",
        action="append",
        required=True,
        help="Root to scan for raw session files. Repeatable.",
    )
    parser.add_argument(
        "--pattern",
        action="append",
        required=True,
        help="Glob pattern relative to each root, e.g. '**/*.jsonl'. Repeatable.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Initial number of files to pass to each watch-once invocation.",
    )
    parser.add_argument(
        "--min-batch-size",
        type=int,
        default=1,
        help="Smallest batch size allowed when shrinking after failures.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Optional cap on successful batches for a single run.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=None,
        help="Override the resume position instead of using the saved state file.",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help="JSON file that stores resume state. Defaults under <data-dir>/recovery_state/.",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="JSONL log file for per-batch results. Defaults alongside the state file.",
    )
    parser.add_argument(
        "--serial-chunk-size",
        type=int,
        default=32,
        help="Sets CASS_INDEXER_SERIAL_CHUNK_SIZE for the cass subprocess.",
    )
    parser.add_argument(
        "--defer-lexical-updates",
        action="store_true",
        default=True,
        help="Set CASS_DEFER_LEXICAL_UPDATES=1 for DB-only reconciliation passes.",
    )
    parser.add_argument(
        "--no-defer-lexical-updates",
        dest="defer_lexical_updates",
        action="store_false",
        help="Do not set CASS_DEFER_LEXICAL_UPDATES.",
    )
    return parser.parse_args()


def collect_paths(roots: List[str], patterns: List[str]) -> List[Path]:
    seen: Dict[str, Path] = {}
    for root_text in roots:
        root = Path(root_text).expanduser().resolve()
        if not root.exists():
            continue
        for pattern in patterns:
            for path in root.glob(pattern):
                if path.is_file():
                    seen[str(path)] = path
    return [seen[key] for key in sorted(seen.keys())]


def state_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    data_dir = Path(args.data_dir).expanduser().resolve()
    recovery_dir = data_dir / "recovery_state"
    recovery_dir.mkdir(parents=True, exist_ok=True)
    if args.state_file is not None:
        state_file = Path(args.state_file).expanduser().resolve()
    else:
        state_file = recovery_dir / "watch_once_batches_state.json"
    if args.log_file is not None:
        log_file = Path(args.log_file).expanduser().resolve()
    else:
        log_file = recovery_dir / "watch_once_batches_log.jsonl"
    return state_file, log_file


def load_state(state_file: Path) -> Dict[str, object]:
    if not state_file.exists():
        return {}
    return json.loads(state_file.read_text())


def save_state(state_file: Path, state: Dict[str, object]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def append_log(log_file: Path, payload: Dict[str, object]) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def db_counts(data_dir: Path) -> Dict[str, int]:
    db_path = data_dir / "agent_search.db"
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        conversations = cur.execute("SELECT count(*) FROM conversations").fetchone()[0]
        messages = cur.execute("SELECT count(*) FROM messages").fetchone()[0]
        return {
            "conversations": int(conversations),
            "messages": int(messages),
        }
    finally:
        conn.close()


def run_batch(
    cass_binary: Path,
    data_dir: Path,
    batch_paths: List[Path],
    defer_lexical_updates: bool,
    serial_chunk_size: int,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        str(cass_binary),
        "--color=never",
        "index",
        "--watch-once",
        *[str(path) for path in batch_paths],
        "--data-dir",
        str(data_dir),
        "--json",
    ]
    env = os.environ.copy()
    env["CASS_INDEXER_SERIAL_CHUNK_SIZE"] = str(serial_chunk_size)
    if defer_lexical_updates:
        env["CASS_DEFER_LEXICAL_UPDATES"] = "1"
    else:
        env.pop("CASS_DEFER_LEXICAL_UPDATES", None)
    return subprocess.run(
        cmd,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def failure_text(proc: subprocess.CompletedProcess[str]) -> str:
    return "\n".join(part for part in [proc.stdout, proc.stderr] if part).lower()


def main() -> int:
    args = parse_args()
    cass_binary = Path(args.cass_binary).expanduser().resolve()
    data_dir = Path(args.data_dir).expanduser().resolve()
    state_file, log_file = state_paths(args)

    paths = collect_paths(args.root, args.pattern)
    if not paths:
        print(json.dumps({"status": "no_paths", "roots": args.root, "patterns": args.pattern}))
        return 0

    state = load_state(state_file)
    next_index = int(state.get("next_index", 0))
    current_batch_size = int(state.get("current_batch_size", args.batch_size))
    if args.start_index is not None:
        next_index = args.start_index
        current_batch_size = args.batch_size

    baseline_counts = db_counts(data_dir)
    run_started_at = int(time.time())
    successful_batches = 0

    while next_index < len(paths):
        if args.max_batches is not None and successful_batches >= args.max_batches:
            break

        batch_size = max(args.min_batch_size, current_batch_size)
        batch = paths[next_index : next_index + batch_size]
        started_at = time.time()
        proc = run_batch(
            cass_binary=cass_binary,
            data_dir=data_dir,
            batch_paths=batch,
            defer_lexical_updates=args.defer_lexical_updates,
            serial_chunk_size=args.serial_chunk_size,
        )
        elapsed_ms = int((time.time() - started_at) * 1000)
        combined_failure = failure_text(proc)

        log_entry: Dict[str, object] = {
            "ts": int(time.time()),
            "start_index": next_index,
            "end_index": next_index + len(batch),
            "batch_size": len(batch),
            "first_path": str(batch[0]),
            "last_path": str(batch[-1]),
            "exit_code": proc.returncode,
            "elapsed_ms": elapsed_ms,
            "stdout_tail": proc.stdout[-2000:],
            "stderr_tail": proc.stderr[-2000:],
        }

        if proc.returncode == 0:
            counts = db_counts(data_dir)
            log_entry["db_counts"] = counts
            append_log(log_file, log_entry)
            next_index += len(batch)
            successful_batches += 1
            state = {
                "roots": args.root,
                "patterns": args.pattern,
                "total_paths": len(paths),
                "next_index": next_index,
                "current_batch_size": current_batch_size,
                "successful_batches_this_run": successful_batches,
                "run_started_at": run_started_at,
                "updated_at": int(time.time()),
                "baseline_counts": baseline_counts,
                "latest_counts": counts,
                "last_batch": {
                    "size": len(batch),
                    "first_path": str(batch[0]),
                    "last_path": str(batch[-1]),
                    "elapsed_ms": elapsed_ms,
                },
            }
            save_state(state_file, state)
            print(
                json.dumps(
                    {
                        "status": "batch_ok",
                        "next_index": next_index,
                        "total_paths": len(paths),
                        "batch_size": len(batch),
                        "elapsed_ms": elapsed_ms,
                        "db_counts": counts,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            continue

        append_log(log_file, log_entry)
        if "out of memory" in combined_failure and batch_size > args.min_batch_size:
            current_batch_size = max(args.min_batch_size, batch_size // 2)
            state = {
                "roots": args.root,
                "patterns": args.pattern,
                "total_paths": len(paths),
                "next_index": next_index,
                "current_batch_size": current_batch_size,
                "successful_batches_this_run": successful_batches,
                "run_started_at": run_started_at,
                "updated_at": int(time.time()),
                "baseline_counts": baseline_counts,
                "latest_counts": db_counts(data_dir),
                "last_failure": {
                    "reason": "out_of_memory",
                    "failed_batch_size": batch_size,
                    "retry_batch_size": current_batch_size,
                    "first_path": str(batch[0]),
                    "last_path": str(batch[-1]),
                },
            }
            save_state(state_file, state)
            print(
                json.dumps(
                    {
                        "status": "shrinking_batch_after_oom",
                        "failed_batch_size": batch_size,
                        "retry_batch_size": current_batch_size,
                        "next_index": next_index,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            continue

        print(proc.stdout, end="", file=sys.stdout)
        print(proc.stderr, end="", file=sys.stderr)
        state = {
            "roots": args.root,
            "patterns": args.pattern,
            "total_paths": len(paths),
            "next_index": next_index,
            "current_batch_size": current_batch_size,
            "successful_batches_this_run": successful_batches,
            "run_started_at": run_started_at,
            "updated_at": int(time.time()),
            "baseline_counts": baseline_counts,
            "latest_counts": db_counts(data_dir),
            "last_failure": {
                "reason": "subprocess_failed",
                "exit_code": proc.returncode,
                "batch_size": batch_size,
                "first_path": str(batch[0]),
                "last_path": str(batch[-1]),
            },
        }
        save_state(state_file, state)
        return proc.returncode

    final_counts = db_counts(data_dir)
    summary = {
        "status": "done",
        "successful_batches_this_run": successful_batches,
        "next_index": next_index,
        "total_paths": len(paths),
        "baseline_counts": baseline_counts,
        "final_counts": final_counts,
        "state_file": str(state_file),
        "log_file": str(log_file),
    }
    save_state(state_file, {**state, **summary, "updated_at": int(time.time())})
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
