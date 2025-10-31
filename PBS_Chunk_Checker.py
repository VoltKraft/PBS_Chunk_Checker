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
from typing import Iterable, Iterator, Optional, Sequence, Set, Tuple, List, Dict
from collections import Counter 

__version__ = "2.2.0"

# =============================================================================
# CLI helpers and shared utilities
# =============================================================================

ICONS: Dict[str, str] = {
    "error": "❌",
    "warning": "⚠️",
    "info": "ℹ️",
    "folder": "📁",
    "folder_current": "📂",
    "save": "💾",
    "index": "📄",
    "sum": "➕",
    "chunk": "📦",
    "total": "🧮",
    "timer": "⏱️",
    "puzzle": "🧩",
    "missing": "❌",
}

ASCII_ICONS: Dict[str, str] = {
    "error": "[ERROR]",
    "warning": "[WARN]",
    "info": "[INFO]",
    "folder": "[DIR]",
    "folder_current": "[DIR]",
    "save": "[SAVE]",
    "index": "[INDEX]",
    "sum": "[SUM]",
    "chunk": "[CHUNK]",
    "total": "[TOTAL]",
    "timer": "[TIME]",
    "puzzle": "[DETAIL]",
    "missing": "[MISSING]",
}

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
        sys.stderr.write(f"{ICONS['error']} Error: required command not found: {cmd[0]}\n")
        raise


def get_datastore_path(datastore_name: str) -> str:
    """Resolve the filesystem path of a PBS datastore via proxmox-backup-manager."""
    last_error = ""
    try:
        cp = run_cmd(
            ["proxmox-backup-manager", "datastore", "show", datastore_name, "--output-format", "json"],
            check=False,
        )
        if cp.returncode == 0 and cp.stdout:
            try:
                data = json.loads(cp.stdout)
            except json.JSONDecodeError:
                data = {}
            path = data.get("path")
            if path:
                return path
        if cp.returncode != 0:
            err_msg = ""
            if cp.stderr:
                err_msg = cp.stderr.strip()
            elif cp.stdout:
                err_msg = cp.stdout.strip()
            if err_msg:
                last_error = err_msg
    except FileNotFoundError:
        raise

    try:
        cp = run_cmd(
            ["proxmox-backup-manager", "datastore", "show", datastore_name],
            check=False,
        )
        m = re.search(r'"path"\s*:\s*"([^"]+)"', cp.stdout or "")
        if m:
            return m.group(1)
        if cp.returncode != 0:
            err_msg = ""
            if cp.stderr:
                err_msg = cp.stderr.strip()
            elif cp.stdout:
                err_msg = cp.stdout.strip()
            if err_msg:
                last_error = err_msg
    except FileNotFoundError:
        raise

    if not last_error:
        last_error = f"Datastore '{datastore_name}' not found or path not resolvable."

    sys.stderr.write(f"{ICONS['error']} Error: {last_error}\n")
    sys.exit(1)


def list_datastores() -> List[str]:
    """Return a list of available PBS datastores (best-effort).

    Tries JSON first, falls back to parsing text output. If the command is not
    available or fails, returns an empty list so the caller can ask for manual input.
    """
    try:
        cp = run_cmd(["proxmox-backup-manager", "datastore", "list", "--output-format", "json"])
        if cp.stdout:
            try:
                data = json.loads(cp.stdout)
                if isinstance(data, list):
                    names: List[str] = []
                    for item in data:
                        if isinstance(item, dict):
                            n = item.get("name") or item.get("datastore") or item.get("id")
                            if isinstance(n, str):
                                names.append(n)
                    return sorted(set(names))
            except json.JSONDecodeError:
                pass
    except subprocess.CalledProcessError:
        pass
    except FileNotFoundError:
        return []


def ensure_required_tools() -> None:
    """Verify that required PBS CLI tools are available before proceeding."""
    required = ["proxmox-backup-manager", "proxmox-backup-debug"]
    missing = [cmd for cmd in required if shutil.which(cmd) is None]
    if missing:
        missing_str = ", ".join(sorted(set(missing)))
        sys.stderr.write(
            f"{ICONS['error']} Error: required command(s) missing: {missing_str}\n"
        )
        sys.stderr.write("Please install the Proxmox Backup Server CLI tools and retry.\n")
        sys.exit(1)

    # Fallback: parse plain text
    try:
        cp = run_cmd(["proxmox-backup-manager", "datastore", "list"])  # plain text output
        names2: List[str] = []
        for line in (cp.stdout or "").splitlines():
            line = line.strip()
            m = re.match(r"^([A-Za-z0-9_.-]+)\b", line)
            if m:
                names2.append(m.group(1))
        return sorted(set(names2))
    except Exception:
        return []


def find_index_files(search_path: str) -> list[str]:
    """Recursively find *.fidx and *.didx under search_path."""
    matches: list[str] = []
    sp = Path(search_path)
    if not sp.is_dir():
        sys.stderr.write(f"{ICONS['error']} Error: folder does not exist → {search_path}\n")
        sys.exit(1)
    for root, dirs, files in os.walk(sp):
        for name in files:
            if name.endswith(".fidx") or name.endswith(".didx"):
                matches.append(str(Path(root) / name))
    return matches


# =============================================================================
# Interactive helpers (menu-driven mode)
# =============================================================================

def _prompt_select(prompt: str, options: List[str], allow_manual: bool = True) -> Optional[str]:
    """Simple numeric selection menu. Returns the chosen string or None if aborted.

    - Shows options enumerated 1..N
    - 'm' to enter manually (if allowed)
    - 'q' to abort
    """
    while True:
        print(prompt)
        for i, opt in enumerate(options, 1):
            print(f"  {i}) {opt}")
        extra = []
        if allow_manual:
            extra.append("m = enter manually")
        extra.append("q = quit")
        print("  (" + ", ".join(extra) + ")")
        choice = input("> ").strip()
        if choice.lower() == "q":
            return None
        if allow_manual and choice.lower() == "m":
            manual = input("Enter value: ").strip()
            if manual:
                return manual
            continue
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        print("Invalid input, please try again.\n")


def _choose_directory(base_path: str) -> Optional[str]:
    """Interactive directory browser inside base_path.

    Returns absolute path of the chosen directory, or None if aborted.
    """
    base = Path(base_path)
    if not base.is_dir():
        return None

    current = base
    while True:
        rel = "/" if current == base else "/" + str(current.relative_to(base))
        print(f"\n{ICONS['folder_current']} Current path: {rel}")
        # List subdirs (skip hidden and .chunks by default)
        subs = []
        try:
            for p in sorted([p for p in current.iterdir() if p.is_dir()], key=lambda x: x.name.lower()):
                if p.name.startswith('.'):
                    continue
                if p.name == ".chunks":
                    continue
                subs.append(p)
        except PermissionError:
            subs = []

        print("Select a directory:")
        print("  0) Use current path")
        for i, p in enumerate(subs, 1):
            print(f"  {i}) {p.name}")
        print("  (u = go up one level, m = enter path manually, q = quit)")

        choice = input("> ").strip().lower()
        if choice == "q":
            return None
        if choice == "u":
            if current != base:
                current = current.parent
            continue
        if choice == "m":
            manual = input("Enter relative path from datastore (e.g. /ns/MyNamespace): ").strip()
            if manual:
                manual = manual.lstrip("/")
                abs_path = base / manual
                if abs_path.is_dir():
                    return str(abs_path)
                print("Path does not exist. Please try again.")
            continue
        if choice.isdigit():
            idx = int(choice)
            if idx == 0:
                return str(current)
            if 1 <= idx <= len(subs):
                current = subs[idx - 1]
                continue
        print("Invalid input, please try again.")


# =============================================================================
# Chunk extraction from index files
# =============================================================================

_HEX64 = re.compile(r'"?([A-Fa-f0-9]{64})"?')

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
            chunks.add(m.group(1).lower())
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
            if (
                isinstance(digest, str)
                and len(digest) == 64
                and all(c in "0123456789abcdefABCDEF" for c in digest)
            ):
                chunks.add(digest.lower())
        return chunks
    return None


def extract_chunks_from_file(index_file: str) -> Set[str]:
    try:
        cp = run_cmd(
            ["proxmox-backup-debug", "inspect", "file", "--output-format", "json", index_file],
            check=False,
        )
        if cp.returncode == 0 and cp.stdout:
            parsed = _parse_chunks_from_json(cp.stdout)
            if parsed is not None:
                return parsed
        elif cp.returncode != 0 and cp.stderr:
            sys.stderr.write(
                f"{ICONS['warning']} Warning: failed to inspect {index_file} (json): {cp.stderr.strip()}\n"
            )
    except FileNotFoundError:
        raise RuntimeError("Required command 'proxmox-backup-debug' not available.") from None

    try:
        cp_text = run_cmd(
            ["proxmox-backup-debug", "inspect", "file", "--output-format", "text", index_file],
            check=False,
        )
    except FileNotFoundError:
        raise RuntimeError("Required command 'proxmox-backup-debug' not available.") from None

    if cp_text.returncode != 0:
        if cp_text.stderr:
            sys.stderr.write(
                f"{ICONS['warning']} Warning: failed to inspect {index_file} (text): {cp_text.stderr.strip()}\n"
            )
        return set()

    return _parse_chunks_from_text(cp_text.stdout or "")


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
    except OSError as exc:
        sys.stderr.write(f"{ICONS['warning']} Warning: unable to access chunk file {path}: {exc}\n")
        return 0


# =============================================================================
# Progress rendering utilities
# =============================================================================

def _progress_line(prefix: str, i: int, total: int, extra: str = "") -> None:
    msg = f"\r\033[K{prefix} {i}/{total} {extra}"
    print(msg, end="", flush=True)


# =============================================================================
# Main routine: CLI parsing, chunk processing and reporting
# =============================================================================

def main(argv: Optional[Sequence[str]] = None) -> int:
    # ----- Parse CLI arguments and compute runtime defaults -----
    parser = argparse.ArgumentParser(
        description=(
            "Sum actual used chunk sizes for a given PBS datastore object (namespace/VM/CT). "
            "When started without parameters, an interactive menu is shown."
        )
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show program version and exit.",
    )
    parser.add_argument(
        "--datastore",
        help="Name of the PBS datastore where the object resides (e.g. MyDatastore).",
    )
    parser.add_argument(
        "--searchpath",
        dest="searchpath",
        help="Object path inside the datastore (e.g. /ns/MyNamespace or /ns/MyNamespace/vm/100).",
    )
    default_workers = min(32, (os.cpu_count() or 4) * 2)
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help="Number of parallel workers to use when parsing indexes and statting chunk files. "
             "Defaults to 2× available CPUs (capped at 32).",
    )
    parser.add_argument(
        "--no-emoji",
        action="store_true",
        help="Disable emoji characters in console output.",
    )
    args = parser.parse_args(argv)

    if args.no_emoji:
        ICONS.update(ASCII_ICONS)

    ensure_required_tools()

    if args.workers <= 0:
        sys.stderr.write(
            f"{ICONS['error']} Error: invalid worker count ({args.workers}). Using default value {default_workers}.\n"
        )
        args.workers = default_workers

    # ----- Interactive mode detection -----
    interactive = not args.datastore and not args.searchpath
    if (args.datastore and not args.searchpath) or (args.searchpath and not args.datastore):
        parser.error("Either provide both --datastore and --searchpath, or none for interactive mode.")

    if interactive:
        print("PBS Chunk Checker - Interactive Mode\n")
        # 1) Select or enter datastore
        stores = list_datastores()
        if stores:
            ds = _prompt_select("Select datastore:", stores, allow_manual=True)
            if ds is None:
                print("Aborted.")
                return 130
            args.datastore = ds
        else:
            # Fallback: ask for manual input
            ds = input("Enter datastore name: ").strip()
            if not ds:
                print("No datastore provided. Aborting.")
                return 130
            args.datastore = ds

        # Resolve datastore path
        try:
            datastore_path = get_datastore_path(args.datastore)
        except SystemExit:
            return 1
        except FileNotFoundError:
            sys.stderr.write(
                f"{ICONS['error']} Error: required command 'proxmox-backup-manager' not available.\n"
            )
            return 1

        if not Path(datastore_path).is_dir():
            sys.stderr.write(
                f"{ICONS['error']} Error: datastore path is not a directory → {datastore_path}\n"
            )
            return 1

        # 2) Choose search path via browser or manual entry
        abs_selected = _choose_directory(datastore_path)
        if abs_selected is None:
            print("Aborted.")
            return 130
        # Convert to datastore-relative path (prefix with '/')
        rel = "/" + str(Path(abs_selected).relative_to(datastore_path)) if abs_selected != datastore_path else "/"
        args.searchpath = rel

    # ----- Start measuring total execution time -----
    start_ts = time.time()

    # ----- Resolve datastore and chunk directory paths -----
    try:
        datastore_path = get_datastore_path(args.datastore)
    except FileNotFoundError:
        sys.stderr.write(
            f"{ICONS['error']} Error: required command 'proxmox-backup-manager' not available.\n"
        )
        return 1

    if not Path(datastore_path).is_dir():
        sys.stderr.write(
            f"{ICONS['error']} Error: datastore path is not a directory → {datastore_path}\n"
        )
        return 1

    search_path = str(Path(datastore_path) / args.searchpath.lstrip("/"))
    chunks_root = str(Path(datastore_path) / ".chunks")

    print(f"{ICONS['folder']} Path to datastore: {datastore_path}")
    print(f"{ICONS['folder']} Search path: {search_path}")
    print(f"{ICONS['chunk']} Chunk path: {chunks_root}")

    if not Path(search_path).is_dir():
        sys.stderr.write(f"{ICONS['error']} Error: folder does not exist → {search_path}\n")
        return 1

    # ----- Locate index files (didx/fidx) -----
    index_files = find_index_files(search_path)
    total_files = len(index_files)
    if total_files == 0:
        print(f"{ICONS['info']} No index files (*.fidx/*.didx) found.")
        return 0
         
    print(f"\n{ICONS['save']} Saving all used chunks")

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
                sys.stderr.write(f"\n{ICONS['warning']} Warning: failed to parse {futs[fut]}: {e}\n")
            processed += 1
            _progress_line(f"{ICONS['index']} Index", processed, total_files)

    print()

    chunk_counter_total = sum(digest_counter.values())
    total_unique = len(digest_counter)
    duplicate_ratio = (1 - total_unique / chunk_counter_total) * 100 if chunk_counter_total else 0

    if total_unique == 0:
        print(f"{ICONS['info']} No chunks referenced. Nothing to sum.")
        return 0

    # ----- Sum chunk files under .chunks -----
    print(f"{ICONS['sum']} Summing up chunks")

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
                    print(
                        f"\r\033[K{ICONS['missing']} Missing: {chunk_path_for_digest(chunks_root, futs2[fut])}",
                        flush=True,
                    )
            except Exception as e:
                sys.stderr.write(f"\n{ICONS['warning']} Warning: failed to stat chunk {futs2[fut]}: {e}\n")
            summed += 1
            _progress_line(
                f"{ICONS['chunk']} Chunk",
                summed,
                total_unique,
                f"| {ICONS['total']} Size so far: {human_readable_size(total_bytes)}",
            )

    print()
    print("\033[2K", end="")

    print(f"{ICONS['total']} Total size: {total_bytes} Bytes ({human_readable_size(total_bytes)})")

    end_ts = time.time()
    duration = int(end_ts - start_ts)
    hours = duration // 3600
    minutes = (duration % 3600) // 60
    seconds = duration % 60
    print(f"{ICONS['timer']} Evaluation duration: {hours} hours, {minutes} minutes, and {seconds} seconds")

    print(
        f"{ICONS['puzzle']} Unique chunks: {total_unique} ({100 - duplicate_ratio:.2f}% unique, {duplicate_ratio:.2f}% duplicates)"
    )

    if missing_count:
        print(f"{ICONS['warning']} Missing chunk files: {missing_count}")

    print(f"{ICONS['folder']} Searched object: {search_path}")
    return 0


# =============================================================================
# Program entrypoint
# =============================================================================

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(130))
    sys.exit(main())
