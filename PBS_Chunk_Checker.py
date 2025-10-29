#!/usr/bin/env python3

import argparse
import concurrent.futures as futures
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence, Set, Tuple
from collections import Counter 

# =============================================================================
# CLI helpers and shared utilities
# =============================================================================

def human_readable_size(num_bytes: int) -> str:
    """Format bytes using IEC units with 'B' suffix (e.g., '1.0KiB')."""
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}B"
            return f"{size:.1f}{unit}"
        size /= 1024.0


def run_cmd(cmd: Sequence[str], check: bool = True, capture: bool = True, text: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the CompletedProcess. Raises on failure if check=True."""
    try:
        return subprocess.run(
            cmd,
            check=check,
            capture_output=capture,
            text=text,
        )
    except FileNotFoundError as e:
        sys.stderr.write(f"❌ Error: required command not found: {cmd[0]}\n")
        raise


def get_datastore_path(datastore_name: str) -> str:
    """Resolve the filesystem path of a PBS datastore via proxmox-backup-manager."""
    try:
        cp = run_cmd(["proxmox-backup-manager", "datastore", "show", datastore_name, "--output-format", "json"])
        data = json.loads(cp.stdout) if cp.stdout else {}
        path = data.get("path")
        if path:
            return path
    except subprocess.CalledProcessError:
        pass

    cp = run_cmd(["proxmox-backup-manager", "datastore", "show", datastore_name])
    m = re.search(r'"path"\s*:\s*"([^"]+)"', cp.stdout)
    if m:
        return m.group(1)

    sys.stderr.write(f"❌ Error: Datastore '{datastore_name}' not found or path not resolvable.\n")
    sys.exit(1)


def find_index_files(search_path: str) -> list[str]:
    """Recursively find *.fidx and *.didx under search_path."""
    matches: list[str] = []
    sp = Path(search_path)
    if not sp.is_dir():
        sys.stderr.write(f"❌ Error: Folder does not exist → {search_path}\n")
        sys.exit(1)
    for root, dirs, files in os.walk(sp):
        for name in files:
            if name.endswith(".fidx") or name.endswith(".didx"):
                matches.append(str(Path(root) / name))
    return matches


# =============================================================================
# Chunk extraction from index files
# =============================================================================

_HEX64 = re.compile(r'"?([a-f0-9]{64})"?')

def _parse_chunks_from_text(output: str) -> Set[str]:
    chunks: Set[str] = set()
    in_chunks = False
    for line in output.splitlines():
        if not in_chunks:
            if line.strip().startswith("chunks:"):
                in_chunks = True
            continue
        m = _HEX64.search(line)
        if m:
            chunks.add(m.group(1))
    return chunks


def _parse_chunks_from_json(output: str) -> Optional[Set[str]]:
    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return None

    chunks = set()
    if isinstance(data, dict) and "chunks" in data:
        for item in data.get("chunks", []):
            digest = item.get("digest")
            if isinstance(digest, str) and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest):
                chunks.add(digest)
        return chunks
    return None


def extract_chunks_from_file(index_file: str) -> Set[str]:
    try:
        cp = run_cmd(["proxmox-backup-debug", "inspect", "file", "--output-format", "json", index_file])
        if cp.stdout:
            parsed = _parse_chunks_from_json(cp.stdout)
            if parsed is not None:
                return parsed
    except subprocess.CalledProcessError:
        pass

    cp = run_cmd(["proxmox-backup-debug", "inspect", "file", "--output-format", "text", index_file])
    return _parse_chunks_from_text(cp.stdout)


# =============================================================================
# Chunk size lookup and aggregation
# =============================================================================

def chunk_path_for_digest(chunks_root: str, digest: str) -> Path:
    return Path(chunks_root) / digest[:4] / digest


def stat_size_if_exists(path: Path) -> int:
    try:
        st = path.stat()
        return int(st.st_size)
    except FileNotFoundError:
        return 0


# =============================================================================
# Progress rendering utilities
# =============================================================================

def _progress_line(prefix: str, i: int, total: int, extra: str = "") -> None:
    msg = f"\r\033[K{prefix} {i}/{total} {extra}"
    print(msg, end="", flush=True)


# =============================================================================
# Main routine: CLI parsing, data evaluation and reporting
# =============================================================================

def main(argv: Optional[Sequence[str]] = None) -> int:
    # ----- Parse CLI arguments and determine worker count -----
    parser = argparse.ArgumentParser(
        description="Sum actual used chunk sizes for a given PBS datastore object (namespace/VM/CT)."
    )
    parser.add_argument("datastore", help='Name of the PBS datastore (e.g., "MyDatastore")')
    parser.add_argument("search_subpath", help='Subpath (e.g., "/ns/MyNamespace" or "/ns/MyNamespace/vm/100")')
    parser.add_argument("--workers", type=int, default=min(32, (os.cpu_count() or 4) * 2),
                        help="Number of parallel workers for parsing/stat (default: 2×CPU, capped at 32).")
    args = parser.parse_args(argv)

    start_ts = time.time()

    # ----- Resolve datastore and chunk directory paths -----
    datastore_path = get_datastore_path(args.datastore)
    search_path = str(Path(datastore_path) / args.search_subpath.lstrip("/"))
    chunks_root = str(Path(datastore_path) / ".chunks")

    print(f"📁 Path to datastore: {datastore_path}")
    print(f"📁 Search path: {search_path}")
    print(f"📁 Chunk path: {chunks_root}")

    if not Path(search_path).is_dir():
        sys.stderr.write(f"❌ Error: Folder does not exist → {search_path}\n")
        return 1

    # ----- Locate index files (didx/fidx) -----
    index_files = find_index_files(search_path)
    total_files = len(index_files)
    if total_files == 0:
        print("ℹ️ No index files (*.fidx/*.didx) found.")
        return 0
         
    print("\n💾 Saving all used chunks")

    # ----- Extract referenced chunks from index files -----
    digest_counter = Counter()
    processed = 0
    with futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(extract_chunks_from_file, f): f for f in index_files}
        for fut in futures.as_completed(futs):
            try:
                digests = fut.result()
                digest_counter.update(digests)
            except Exception as e:
                sys.stderr.write(f"\n⚠️ Failed to parse {futs[fut]}: {e}\n")
            processed += 1
            _progress_line("📄 Index", processed, total_files)

    print()

    chunk_counter_total = sum(digest_counter.values())
    total_unique = len(digest_counter)
    duplicate_ratio = (1 - total_unique / chunk_counter_total) * 100 if chunk_counter_total else 0

    if total_unique == 0:
        print("ℹ️ No chunks referenced. Nothing to sum.")
        return 0

    # ----- Sum chunk files under .chunks -----
    print("➕ Summing up chunks")

    missing_count = 0
    summed = 0
    total_bytes = 0

    def _stat_one(digest: str) -> Tuple[int, bool]:
        path = chunk_path_for_digest(chunks_root, digest)
        size = stat_size_if_exists(path)
        missing = (size == 0 and not path.exists())
        return size, missing

    with futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs2 = {pool.submit(_stat_one, d): d for d in digest_counter.keys()}
        for fut in futures.as_completed(futs2):
            try:
                size, missing = fut.result()
                total_bytes += size
                if missing:
                    missing_count += 1
                    print(f"\r\033[K❌ Missing: {chunk_path_for_digest(chunks_root, futs2[fut])}", flush=True)
            except Exception as e:
                sys.stderr.write(f"\n⚠️ Failed to stat chunk {futs2[fut]}: {e}\n")
            summed += 1
            _progress_line("📦 Chunk", summed, total_unique, f"| 🧮 Size so far: {human_readable_size(total_bytes)}")

    print()
    print("\033[2K", end="")

    print(f"🧮 Total size: {total_bytes} Bytes ({human_readable_size(total_bytes)})")

    end_ts = time.time()
    duration = int(end_ts - start_ts)
    hours = duration // 3600
    minutes = (duration % 3600) // 60
    seconds = duration % 60
    print(f"⏱️ Evaluation duration: {hours} hours, {minutes} minutes, and {seconds} seconds")

    print(f"🧩 Unique chunks: {total_unique} ({100 - duplicate_ratio:.2f}% unique, {duplicate_ratio:.2f}% duplicates)")

    if missing_count:
        print(f"⚠️ Missing chunk files: {missing_count}")

    print(f"📁 Searched object: {search_path}")
    return 0


# =============================================================================
# Program entrypoint
# =============================================================================

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(130))
    sys.exit(main())
