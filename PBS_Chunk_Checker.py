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
from typing import Iterable, Iterator, Optional, Sequence, Set, Tuple, List, Dict, Callable
from collections import Counter
try:
    import curses  # type: ignore
except Exception:
    curses = None  # type: ignore

__version__ = "2.5.0"

COMMAND_PATHS: Dict[str, str] = {}
DEFAULT_PATH_SEGMENTS = [
    str(p)
    for p in (
        Path("/usr/sbin"),
        Path("/usr/bin"),
        Path("/sbin"),
        Path("/bin"),
    )
    if p.exists()
]
COMMAND_ENV = os.environ.copy()
if DEFAULT_PATH_SEGMENTS:
    COMMAND_ENV["PATH"] = os.pathsep.join(list(dict.fromkeys(DEFAULT_PATH_SEGMENTS)))
else:
    COMMAND_ENV["PATH"] = COMMAND_ENV.get("PATH", "")
COMMAND_ENV.setdefault("LC_ALL", "C")
COMMAND_ENV.setdefault("LANG", "C")
COMMAND_TIMEOUTS = {
    "manager_list": 10,
    "manager_show": 10,
    "debug_inspect": 60,
}
DATASTORE_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def _format_command(cmd: object) -> str:
    if isinstance(cmd, (list, tuple)):
        return " ".join(str(part) for part in cmd)
    return str(cmd)

# =============================================================================
# CLI helpers and shared utilities
# =============================================================================

EMOJI_ICONS: Dict[str, str] = {
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
    "threads": "🧵",
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
    "threads": "[THREADS]",
}

ICONS: Dict[str, str] = EMOJI_ICONS.copy()
_EMOJI_ENABLED = True


def _set_emoji_mode(enabled: bool) -> None:
    """Switch between emoji and ASCII icon sets."""
    global ICONS, _EMOJI_ENABLED
    _EMOJI_ENABLED = enabled
    ICONS.clear()
    ICONS.update(EMOJI_ICONS if enabled else ASCII_ICONS)

def clear_console() -> None:
    """Clear the terminal similar to the POSIX 'clear' command."""
    # ANSI full reset works for Linux terminal emulators
    print("\033c", end="", flush=True)

def format_elapsed(seconds: float) -> str:
    """Return a compact runtime string like '1h 02m 03s'."""
    seconds_int = int(seconds)
    hours, remainder = divmod(seconds_int, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02}m {secs:02}s"
    return f"{minutes}m {secs:02}s"

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


def run_cmd(
    cmd: Sequence[str],
    check: bool = True,
    capture: bool = True,
    text: bool = True,
    timeout: Optional[float] = None,
) -> subprocess.CompletedProcess:
    """Run a command and return the CompletedProcess. Raises on failure if check=True."""
    try:
        if not cmd:
            raise ValueError("Command must not be empty.")
        command = [str(part) for part in cmd]
        resolved = COMMAND_PATHS.get(command[0])
        if resolved:
            command[0] = resolved
        return subprocess.run(
            command,
            check=check,
            capture_output=capture,
            text=text,
            timeout=timeout,
            env=COMMAND_ENV,
        )
    except FileNotFoundError as e:
        sys.stderr.write(f"{ICONS['error']} Error: required command not found: {cmd[0]}\n")
        raise
    except subprocess.TimeoutExpired as e:
        executed = _format_command(command)
        timeout_val = timeout if timeout is not None else e.timeout
        timeout_desc = (
            f"{int(timeout_val)}s"
            if isinstance(timeout_val, (int, float))
            else "configured limit"
        )
        sys.stderr.write(
            f"{ICONS['error']} Error: command timed out after {timeout_desc}: {executed}\n"
        )
        raise


def get_datastore_path(datastore_name: str) -> str:
    """Resolve the filesystem path of a PBS datastore via proxmox-backup-manager."""
    last_error = ""
    try:
        cp = run_cmd(
            ["proxmox-backup-manager", "datastore", "show", datastore_name, "--output-format", "json"],
            check=False,
            timeout=COMMAND_TIMEOUTS["manager_show"],
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
    except subprocess.TimeoutExpired as exc:
        last_error = f"{_format_command(exc.cmd)} timed out after {COMMAND_TIMEOUTS['manager_show']}s"
    except FileNotFoundError:
        raise

    try:
        cp = run_cmd(
            ["proxmox-backup-manager", "datastore", "show", datastore_name],
            check=False,
            timeout=COMMAND_TIMEOUTS["manager_show"],
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
    except subprocess.TimeoutExpired as exc:
        last_error = f"{_format_command(exc.cmd)} timed out after {COMMAND_TIMEOUTS['manager_show']}s"
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
    except subprocess.TimeoutExpired:
        return []
    except subprocess.CalledProcessError:
        pass
    except FileNotFoundError:
        return []

    try:
        cp = run_cmd(
            ["proxmox-backup-manager", "datastore", "list"],
            timeout=COMMAND_TIMEOUTS["manager_list"],
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return []

    names2: List[str] = []
    for line in (cp.stdout or "").splitlines():
        line = line.strip()
        m = re.match(r"^([A-Za-z0-9_.-]+)\b", line)
        if m:
            names2.append(m.group(1))
    return sorted(set(names2))


def ensure_required_tools() -> None:
    """Verify that required PBS CLI tools are available before proceeding."""
    global COMMAND_ENV
    required = ["proxmox-backup-manager", "proxmox-backup-debug"]
    resolved: Dict[str, str] = {}
    missing = []
    for cmd in required:
        path = shutil.which(cmd)
        if path is None:
            missing.append(cmd)
            continue
        resolved[cmd] = str(Path(path).resolve())

    if missing:
        missing_str = ", ".join(sorted(set(missing)))
        sys.stderr.write(
            f"{ICONS['error']} Error: required command(s) missing: {missing_str}\n"
        )
        sys.stderr.write("Please install the Proxmox Backup Server CLI tools and retry.\n")
        sys.exit(1)

    COMMAND_PATHS.update(resolved)

    path_entries: List[str] = []
    for command_path in resolved.values():
        parent = str(Path(command_path).parent)
        if parent not in path_entries:
            path_entries.append(parent)
    for default_dir in DEFAULT_PATH_SEGMENTS:
        if default_dir not in path_entries:
            path_entries.append(default_dir)
    if not path_entries and COMMAND_ENV.get("PATH"):
        path_entries = COMMAND_ENV["PATH"].split(os.pathsep)
    if path_entries:
        COMMAND_ENV["PATH"] = os.pathsep.join(path_entries)


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


def resolve_search_path(datastore_path: str, searchpath: str) -> Path:
    """Return an absolute path inside datastore_path for the provided searchpath."""
    base = Path(datastore_path).resolve()
    relative = (searchpath or "").lstrip("/")
    candidate = base if not relative else (base / relative)
    candidate = candidate.resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise ValueError("Search path escapes datastore root.") from exc
    return candidate

# =============================================================================
# Interactive helpers (menu-driven mode)
# =============================================================================

_CURSES_SENTINEL_MANUAL = "__CURSES_MANUAL__"
_UI_OPTIONS_HANDLER: Optional[Callable[[Optional[object]], None]] = None
_UI_VERSION_HANDLER: Optional[Callable[[Optional[object]], None]] = None

def _want_curses_ui() -> bool:
    if os.environ.get("PBS_CC_NO_CURSES") == "1":
        return False
    if curses is None:
        return False
    try:
        return os.isatty(0) and os.isatty(1) and (os.environ.get("TERM") not in (None, "", "dumb"))
    except Exception:
        return False


def _set_ui_handlers(
    options_handler: Optional[Callable[[Optional[object]], None]] = None,
    version_handler: Optional[Callable[[Optional[object]], None]] = None,
) -> None:
    global _UI_OPTIONS_HANDLER, _UI_VERSION_HANDLER
    _UI_OPTIONS_HANDLER = options_handler
    _UI_VERSION_HANDLER = version_handler


def _invoke_options_handler(stdscr: Optional[object] = None) -> None:
    handler = _UI_OPTIONS_HANDLER
    if handler is not None:
        handler(stdscr)
    elif stdscr is not None and curses is not None:
        try:
            curses.flash()
        except Exception:
            pass


def _invoke_version_handler(stdscr: Optional[object] = None) -> None:
    handler = _UI_VERSION_HANDLER
    if handler is not None:
        handler(stdscr)
    elif stdscr is not None and curses is not None:
        try:
            curses.flash()
        except Exception:
            pass


def _curses_select_menu(prompt: str, options: List[str], allow_manual: bool) -> Optional[str]:
    def _draw(stdscr):
        curses.curs_set(0)
        stdscr.keypad(True)
        idx = 0
        top = 0
        while True:
            stdscr.erase()
            h, w = stdscr.getmaxyx()
            y = 0
            for line in prompt.splitlines():
                try:
                    stdscr.addstr(y, 0, line[: max(1, w - 1)])
                except Exception:
                    pass
                y += 1
            view_h = max(1, h - (y + 1))
            if idx < top:
                top = idx
            if idx >= top + view_h:
                top = max(0, idx - view_h + 1)
            end = min(len(options), top + view_h)
            for i in range(top, end):
                label = options[i]
                prefix = "> " if i == idx else "  "
                text = (prefix + label)[: max(1, w - 1)]
                try:
                    stdscr.addstr(y + (i - top), 0, text, curses.A_REVERSE if i == idx else 0)
                except Exception:
                    pass
            help_line = (
                "↑/↓ move  Space/Enter select  m manual  o options  v version  q quit"
                if allow_manual
                else "↑/↓ move  Space/Enter select  o options  v version  q quit"
            )
            try:
                stdscr.addstr(h - 1, 0, help_line[: max(1, w - 1)])
            except Exception:
                pass
            stdscr.refresh()
            ch = stdscr.getch()
            if ch in (curses.KEY_UP, ord('k')):
                if idx > 0:
                    idx -= 1
            elif ch in (curses.KEY_DOWN, ord('j')):
                if idx < len(options) - 1:
                    idx += 1
            elif ch in (curses.KEY_NPAGE,):
                step = max(1, view_h - 1)
                idx = min(len(options) - 1, idx + step)
            elif ch in (curses.KEY_PPAGE,):
                step = max(1, view_h - 1)
                idx = max(0, idx - step)
            elif ch in (curses.KEY_HOME,):
                idx = 0
            elif ch in (curses.KEY_END,):
                idx = len(options) - 1
            elif ch in (10, 13, ord(' ')):
                return options[idx]
            elif allow_manual and ch in (ord('m'), ord('M')):
                return _CURSES_SENTINEL_MANUAL
            elif ch in (ord('o'), ord('O')):
                _invoke_options_handler(stdscr)
            elif ch in (ord('v'), ord('V')):
                _invoke_version_handler(stdscr)
            elif ch in (ord('q'), ord('Q'), 27):
                return None
    try:
        return curses.wrapper(_draw)  # type: ignore[attr-defined]
    except Exception:
        return None


def _curses_popup(
    stdscr: object,
    title: str,
    body_lines: List[str],
    prompt: Optional[str] = None,
) -> Optional[str]:
    if curses is None:
        return None
    try:
        h, w = stdscr.getmaxyx()  # type: ignore[attr-defined]
    except Exception:
        return None

    lines = body_lines[:]
    content_width = max([len(title) + 2 if title else 0] + [len(line) for line in lines] + ([len(prompt)] if prompt else [0]))
    width = max(32, min(w - 2, content_width + 4)) if w > 10 else max(10, w - 1)
    body_space = max(1, (h - (4 if prompt else 3)))
    visible_lines = lines[:body_space]
    height = len(visible_lines) + (4 if prompt else 3)
    if height > h - 1:
        height = max(4 if prompt else 3, h - 1)
        visible_lines = visible_lines[: max(0, height - (4 if prompt else 3))]
    start_y = max(0, (h - height) // 2)
    start_x = max(0, (w - width) // 2)
    try:
        win = curses.newwin(height, width, start_y, start_x)
    except Exception:
        return None
    win.box()
    if title:
        title_text = f" {title} "
        try:
            win.addstr(0, max(1, (width - len(title_text)) // 2), title_text, curses.A_BOLD)
        except Exception:
            pass
    y = 1
    for line in visible_lines:
        truncated = line[: width - 2]
        try:
            win.addstr(y, 1, truncated.ljust(width - 2))
        except Exception:
            pass
        y += 1
    if prompt is None:
        footer = "Press any key to continue..."
        try:
            win.addstr(height - 2, 1, footer[: width - 2].ljust(width - 2), curses.A_DIM)
        except Exception:
            pass
        win.refresh()
        win.getch()
        del win
        stdscr.touchwin()  # type: ignore[attr-defined]
        stdscr.refresh()  # type: ignore[attr-defined]
        return None

    prompt_line = prompt[: width - 2]
    input_y = height - 2
    try:
        win.addstr(input_y, 1, prompt_line.ljust(width - 2))
    except Exception:
        pass
    win.refresh()
    curses.echo()
    try:
        cursor_x = min(len(prompt_line) + 1, width - 2)
        win.move(input_y, cursor_x)
        max_chars = max(1, width - cursor_x - 1)
        raw = win.getstr(input_y, cursor_x, max_chars)
    except Exception:
        raw = b""
    finally:
        curses.noecho()
    text = raw.decode(errors="ignore").strip()
    del win
    stdscr.touchwin()  # type: ignore[attr-defined]
    stdscr.refresh()  # type: ignore[attr-defined]
    return text


def _curses_show_version(stdscr: object) -> None:
    _curses_popup(stdscr, "Version", [f"PBS_Chunk_Checker version {__version__}"])


def _curses_threads_dialog(stdscr: object, args) -> None:
    lines = [
        "Threads control the number of parallel operations used to:",
        "  - parse index files (*.fidx/*.didx)",
        "  - stat chunk files under .chunks",
        "",
        f"Current value: {args.threads}",
        "",
        "Enter a number between 1 and 32.",
        "Leave blank to keep the current value.",
    ]
    result = _curses_popup(stdscr, "Thread Settings", lines, prompt="New thread count: ")
    if result is None:
        return
    if not result:
        return
    if not result.isdigit():
        _curses_popup(stdscr, "Thread Settings", ["Please enter a positive integer."])
        return
    val = int(result)
    if val < 1 or val > 32:
        _curses_popup(stdscr, "Thread Settings", ["Threads must be between 1 and 32."])
        return
    args.threads = val
    _curses_popup(stdscr, "Thread Settings", [f"Threads set to {val}."])


def _text_show_version() -> None:
    clear_console()
    print(f"PBS_Chunk_Checker version: {__version__}")
    input("Press Enter to continue...")


def _text_threads_dialog(args) -> None:
    while True:
        clear_console()
        print("PBS_Chunk_Checker - Thread Settings\n")
        print("Threads control the number of parallel operations used to:")
        print("  - parse index files (*.fidx/*.didx)")
        print("  - stat chunk files under .chunks\n")
        print(f"Current threads: {args.threads}\n")
        typed = input("Enter new thread count (1-32), or leave blank to keep: ").strip()
        if not typed:
            return
        if not typed.isdigit():
            input("Please enter a positive integer. Press Enter to retry...")
            continue
        val = int(typed)
        if val < 1 or val > 32:
            input("Threads must be between 1 and 32. Press Enter to retry...")
            continue
        args.threads = val
        print(f"\nThreads set to {val}.")
        input("Press Enter to continue...")
        return


def _show_threads_dialog(args, stdscr: Optional[object]) -> None:
    if stdscr is not None and curses is not None:
        _curses_threads_dialog(stdscr, args)
    else:
        _text_threads_dialog(args)


def _show_version_dialog(stdscr: Optional[object]) -> None:
    if stdscr is not None and curses is not None:
        _curses_show_version(stdscr)
    else:
        _text_show_version()


def _emoji_checkbox() -> str:
    return "✔" if _EMOJI_ENABLED else "✘"


def _toggle_emoji_setting(args, stdscr: Optional[object]) -> None:
    new_state = not _EMOJI_ENABLED
    _set_emoji_mode(new_state)
    args.no_emoji = not new_state
    # No separate popup to keep the menu context visible.


def _options_menu_curses(stdscr: object, args) -> None:
    curses.curs_set(0)
    stdscr.keypad(True)
    idx = 0
    top = 0
    notice = ""

    def _entries() -> List[Tuple[str, str]]:
        return [
            ("threads", f"Set threads ({args.threads})"),
            ("emoji", f"Emoji output [{_emoji_checkbox()}]"),
            ("back", "Back"),
        ]

    while True:
        entries = _entries()
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        header = "PBS_Chunk_Checker - Options"
        try:
            stdscr.addstr(0, 0, header[: max(1, w - 1)], curses.A_BOLD)
        except Exception:
            pass
        y = 1
        if notice:
            try:
                stdscr.addstr(y, 0, notice[: max(1, w - 1)])
            except Exception:
                pass
            y += 1
        view_h = max(1, h - (y + 1))
        if idx < top:
            top = idx
        if idx >= top + view_h:
            top = max(0, idx - view_h + 1)
        end = min(len(entries), top + view_h)
        for i in range(top, end):
            label = entries[i][1]
            prefix = "> " if i == idx else "  "
            text = (prefix + label)[: max(1, w - 1)]
            try:
                stdscr.addstr(y + (i - top), 0, text, curses.A_REVERSE if i == idx else 0)
            except Exception:
                pass
        help_line = "↑/↓ move  Space toggle  Enter open  q back"
        try:
            stdscr.addstr(h - 1, 0, help_line[: max(1, w - 1)])
        except Exception:
            pass
        stdscr.refresh()
        ch = stdscr.getch()
        if ch in (curses.KEY_UP, ord('k')):
            if idx > 0:
                idx -= 1
        elif ch in (curses.KEY_DOWN, ord('j')):
            if idx < len(entries) - 1:
                idx += 1
        elif ch in (curses.KEY_NPAGE,):
            step = max(1, view_h - 1)
            idx = min(len(entries) - 1, idx + step)
        elif ch in (curses.KEY_PPAGE,):
            step = max(1, view_h - 1)
            idx = max(0, idx - step)
        elif ch in (curses.KEY_HOME,):
            idx = 0
        elif ch in (curses.KEY_END,):
            idx = len(entries) - 1
        elif ch in (10, 13):
            action = entries[idx][0]
            if action == "threads":
                _curses_threads_dialog(stdscr, args)
                notice = ""
            elif action == "emoji":
                _toggle_emoji_setting(args, stdscr)
                notice = f"Emoji output {'enabled' if _EMOJI_ENABLED else 'disabled'}."
            elif action == "back":
                return
        elif ch == ord(' '):
            action = entries[idx][0]
            if action == "emoji":
                _toggle_emoji_setting(args, stdscr)
                notice = f"Emoji output {'enabled' if _EMOJI_ENABLED else 'disabled'}."
        elif ch in (ord('q'), ord('Q'), 27):
            return


def _options_menu_text(args) -> None:
    notice = ""
    while True:
        clear_console()
        print("PBS_Chunk_Checker - Options\n")
        if notice:
            print(f"{notice}\n")
        print(f" 1) Set threads ({args.threads})")
        emoji_state = "enabled" if _EMOJI_ENABLED else "disabled"
        print(f" 2) Emoji output [{_emoji_checkbox()} - {emoji_state}]")
        print(" q) Back")
        raw = input("> ")
        if raw == " ":
            choice = "space"
        else:
            choice = raw.strip().lower()
        if choice == "1":
            _text_threads_dialog(args)
            notice = ""
        elif choice in ("2", "space"):
            _toggle_emoji_setting(args, None)
            notice = f"Emoji output {'enabled' if _EMOJI_ENABLED else 'disabled'}."
        elif choice == "q":
            return
        else:
            notice = "Invalid input."


def _options_menu(args, stdscr: Optional[object]) -> None:
    if stdscr is not None and curses is not None:
        _options_menu_curses(stdscr, args)
    else:
        _options_menu_text(args)


def _prompt_select(prompt: str, options: List[str], allow_manual: bool = True) -> Optional[str]:
    """Selection menu using curses (if available) with arrow keys + space. Falls back to numeric input."""
    if _want_curses_ui():
        while True:
            result = _curses_select_menu(prompt, options, allow_manual)
            if result == _CURSES_SENTINEL_MANUAL and allow_manual:
                clear_console()
                manual = input("Enter value: ").strip()
                return manual or None
            if result is not None:
                return result

    # Fallback: simple numeric menu
    feedback = ""
    while True:
        clear_console()
        print(prompt)
        if feedback:
            print(f"{feedback}\n")
            feedback = ""
        for i, opt in enumerate(options, 1):
            print(f"  {i}) {opt}")
        extra = []
        if allow_manual:
            extra.append("m = enter manually")
        if _UI_OPTIONS_HANDLER is not None:
            extra.append("o = options")
        if _UI_VERSION_HANDLER is not None:
            extra.append("v = version")
        extra.append("q = quit")
        print("  (" + ", ".join(extra) + ")")
        choice = input("> ").strip()
        if choice.lower() == "q":
            return None
        if choice.lower() == "o" and _UI_OPTIONS_HANDLER is not None:
            _invoke_options_handler(None)
            continue
        if choice.lower() == "v" and _UI_VERSION_HANDLER is not None:
            _invoke_version_handler(None)
            continue
        if allow_manual and choice.lower() == "m":
            clear_console()
            manual = input("Enter value: ").strip()
            if manual:
                return manual
            feedback = "No value entered. Please try again."
            continue
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        feedback = "Invalid input, please try again."


def _curses_choose_directory(base_path: str, feedback: str = "") -> Optional[str]:
    base = Path(base_path)
    if not base.is_dir():
        return None

    def _draw(stdscr):
        curses.curs_set(0)
        stdscr.keypad(True)
        current = base
        idx = 0
        top = 0
        local_feedback = feedback
        while True:
            # build entries
            try:
                subs = []
                for p in sorted([p for p in current.iterdir() if p.is_dir()], key=lambda x: x.name.lower()):
                    if p.name.startswith('.'):
                        continue
                    if p.name == ".chunks":
                        continue
                    subs.append(p)
            except PermissionError:
                subs = []

            entries: List[Tuple[str, str]] = []  # (label, action)
            entries.append(("Use current path", "use"))
            if current != base:
                entries.append((".. (up one level)", "up"))
            for p in subs:
                entries.append((p.name + "/", f"enter:{p.name}"))

            stdscr.erase()
            h, w = stdscr.getmaxyx()
            rel = "/" if current == base else "/" + str(current.relative_to(base))
            header = f"{ICONS.get('folder_current','')} Current path: {rel}"
            y = 0
            try:
                stdscr.addstr(y, 0, header[: max(1, w - 1)])
            except Exception:
                pass
            y += 1
            if local_feedback:
                try:
                    stdscr.addstr(y, 0, local_feedback[: max(1, w - 1)])
                except Exception:
                    pass
                y += 1

            view_h = max(1, h - (y + 1))
            if idx < top:
                top = idx
            if idx >= top + view_h:
                top = max(0, idx - view_h + 1)
            end = min(len(entries), top + view_h)
            for i in range(top, end):
                label = entries[i][0]
                prefix = "> " if i == idx else "  "
                text = (prefix + label)[: max(1, w - 1)]
                try:
                    stdscr.addstr(y + (i - top), 0, text, curses.A_REVERSE if i == idx else 0)
                except Exception:
                    pass
            help_line = "↑/↓ move  Space/Enter open/select  m manual  o options  v version  q quit"
            try:
                stdscr.addstr(h - 1, 0, help_line[: max(1, w - 1)])
            except Exception:
                pass
            stdscr.refresh()
            ch = stdscr.getch()
            if ch in (curses.KEY_UP, ord('k')):
                if idx > 0:
                    idx -= 1
            elif ch in (curses.KEY_DOWN, ord('j')):
                if idx < len(entries) - 1:
                    idx += 1
            elif ch in (curses.KEY_NPAGE,):
                step = max(1, view_h - 1)
                idx = min(len(entries) - 1, idx + step)
            elif ch in (curses.KEY_PPAGE,):
                step = max(1, view_h - 1)
                idx = max(0, idx - step)
            elif ch in (curses.KEY_HOME,):
                idx = 0
            elif ch in (curses.KEY_END,):
                idx = len(entries) - 1
            elif ch in (10, 13, ord(' ')):
                label, action = entries[idx]
                if action == "use":
                    return str(current)
                if action == "up":
                    if current != base:
                        current = current.parent
                        idx = 0
                        top = 0
                        local_feedback = ""
                    continue
                if action.startswith("enter:"):
                    name = action.split(":", 1)[1]
                    nxt = current / name
                    if nxt.is_dir():
                        current = nxt
                        idx = 0
                        top = 0
                        local_feedback = ""
                    else:
                        local_feedback = "Path no longer exists."
                    continue
            elif ch in (ord('m'), ord('M')):
                return _CURSES_SENTINEL_MANUAL
            elif ch in (ord('o'), ord('O')):
                _invoke_options_handler(stdscr)
            elif ch in (ord('v'), ord('V')):
                _invoke_version_handler(stdscr)
            elif ch in (ord('q'), ord('Q'), 27):
                return None
    try:
        return curses.wrapper(_draw)  # type: ignore[attr-defined]
    except Exception:
        return None


def _choose_directory(base_path: str) -> Optional[str]:
    """Interactive directory browser inside base_path.

    Returns absolute path of the chosen directory, or None if aborted.
    """
    base = Path(base_path)
    if not base.is_dir():
        return None

    if _want_curses_ui():
        feedback = ""
        while True:
            res = _curses_choose_directory(base_path, feedback)
            if res == _CURSES_SENTINEL_MANUAL:
                clear_console()
                manual = input("Enter relative path from datastore (e.g. /ns/MyNamespace): ").strip()
                if manual:
                    manual = manual.lstrip("/")
                    abs_path = base / manual
                    if abs_path.is_dir():
                        return str(abs_path)
                    feedback = "Path does not exist. Please try again."
                else:
                    feedback = "No path entered. Please try again."
                continue
            return res

    # Fallback: simple text browser
    current = base
    feedback = ""
    while True:
        clear_console()
        rel = "/" if current == base else "/" + str(current.relative_to(base))
        print(f"\n{ICONS['folder_current']} Current path: {rel}")
        if feedback:
            print(f"{feedback}\n")
            feedback = ""
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
        extra_cmds = ["u = go up one level"]
        if _UI_OPTIONS_HANDLER is not None:
            extra_cmds.append("o = options")
        if _UI_VERSION_HANDLER is not None:
            extra_cmds.append("v = version")
        extra_cmds.append("m = enter path manually")
        extra_cmds.append("q = quit")
        print("  (" + ", ".join(extra_cmds) + ")")

        choice = input("> ").strip().lower()
        if choice == "q":
            return None
        if choice == "u":
            if current != base:
                current = current.parent
            continue
        if choice == "o" and _UI_OPTIONS_HANDLER is not None:
            _invoke_options_handler(None)
            continue
        if choice == "v" and _UI_VERSION_HANDLER is not None:
            _invoke_version_handler(None)
            continue
        if choice == "m":
            clear_console()
            manual = input("Enter relative path from datastore (e.g. /ns/MyNamespace): ").strip()
            if manual:
                manual = manual.lstrip("/")
                abs_path = base / manual
                if abs_path.is_dir():
                    return str(abs_path)
                feedback = "Path does not exist. Please try again."
            else:
                feedback = "No path entered. Please try again."
            continue
        if choice.isdigit():
            idx = int(choice)
            if idx == 0:
                return str(current)
            if 1 <= idx <= len(subs):
                current = subs[idx - 1]
                continue
        feedback = "Invalid input, please try again."


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
            timeout=COMMAND_TIMEOUTS["debug_inspect"],
        )
        if cp.returncode == 0 and cp.stdout:
            parsed = _parse_chunks_from_json(cp.stdout)
            if parsed is not None:
                return parsed
        elif cp.returncode != 0 and cp.stderr:
            sys.stderr.write(
                f"{ICONS['warning']} Warning: failed to inspect {index_file} (json): {cp.stderr.strip()}\n"
            )
    except subprocess.TimeoutExpired as exc:
        sys.stderr.write(
            f"{ICONS['warning']} Warning: {_format_command(exc.cmd)} timed out while inspecting {index_file} (json).\n"
        )
    except FileNotFoundError:
        raise RuntimeError("Required command 'proxmox-backup-debug' not available.") from None

    try:
        cp_text = run_cmd(
            ["proxmox-backup-debug", "inspect", "file", "--output-format", "text", index_file],
            check=False,
            timeout=COMMAND_TIMEOUTS["debug_inspect"],
        )
    except subprocess.TimeoutExpired as exc:
        sys.stderr.write(
            f"{ICONS['warning']} Warning: {_format_command(exc.cmd)} timed out while inspecting {index_file} (text).\n"
        )
        return set()
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
    pct = (i / total * 100.0) if total else 0.0
    msg = f"\r\033[K{prefix} {i}/{total} ({pct:6.2f}%) {extra}"
    print(msg, end="", flush=True)


# =============================================================================
# Interactive main menu flow
# =============================================================================

def _interactive_menu(args) -> Optional[Tuple[str, str]]:
    """Main interactive flow allowing datastore selection, path browsing, thread tweak, and version display.

    Returns (datastore_name, searchpath) or None if user aborted.
    Updates args.threads in-place when changed.
    """
    header = "PBS_Chunk_Checker - Interactive Mode"
    datastore_name: Optional[str] = None
    search_rel: Optional[str] = None

    def _options_handler(stdscr: Optional[object]) -> None:
        _options_menu(args, stdscr)

    def _version_handler(stdscr: Optional[object]) -> None:
        _show_version_dialog(stdscr)

    _set_ui_handlers(_options_handler, _version_handler)

    try:
        while True:
            entries: List[str] = []
            ds_label = datastore_name or "not set"
            entries.append(f"Select datastore [{ds_label}]")
            if datastore_name:
                sp_label = search_rel or "/"
                entries.append(f"Choose search path [{sp_label}]")
            if datastore_name and search_rel:
                entries.append("Start")
            entries.append("Quit")

            choice = _prompt_select(f"{header}\n\nSelect an option:", entries, allow_manual=False)
            if choice is None:
                return None

            if choice.startswith("Select datastore"):
                stores = list_datastores()
                if stores:
                    ds = _prompt_select(f"{header}\n\nSelect datastore:", stores, allow_manual=True)
                    if ds is None:
                        continue
                    if not DATASTORE_PATTERN.fullmatch(ds):
                        clear_console()
                        print(f"{ICONS['error']} Error: invalid datastore name '{ds}'.")
                        input("Press Enter to continue...")
                        continue
                    try:
                        # Validate existence right away to enable path selection later
                        get_datastore_path(ds)
                    except SystemExit:
                        continue
                    except FileNotFoundError:
                        clear_console()
                        print(f"{ICONS['error']} Error: required command 'proxmox-backup-manager' not available.")
                        input("Press Enter to continue...")
                        continue
                    datastore_name = ds
                    search_rel = None  # reset path after datastore change
                else:
                    clear_console()
                    print(f"{header}\n")
                    ds = input("Enter datastore name: ").strip()
                    if not ds:
                        continue
                    if not DATASTORE_PATTERN.fullmatch(ds):
                        clear_console()
                        print(f"{ICONS['error']} Error: invalid datastore name '{ds}'.")
                        input("Press Enter to continue...")
                        continue
                    try:
                        get_datastore_path(ds)
                    except SystemExit:
                        continue
                    except FileNotFoundError:
                        clear_console()
                        print(f"{ICONS['error']} Error: required command 'proxmox-backup-manager' not available.")
                        input("Press Enter to continue...")
                        continue
                    datastore_name = ds
                    search_rel = None

            elif choice.startswith("Choose search path"):
                if not datastore_name:
                    continue
                try:
                    datastore_path = get_datastore_path(datastore_name)
                except SystemExit:
                    continue
                except FileNotFoundError:
                    clear_console()
                    print(f"{ICONS['error']} Error: required command 'proxmox-backup-manager' not available.")
                    input("Press Enter to continue...")
                    continue
                abs_selected = _choose_directory(datastore_path)
                if abs_selected is None:
                    continue
                rel = "/" + str(Path(abs_selected).relative_to(datastore_path)) if abs_selected != datastore_path else "/"
                search_rel = rel

            elif choice == "Start":
                if datastore_name and search_rel:
                    return datastore_name, search_rel

            elif choice == "Quit":
                return None
    finally:
        _set_ui_handlers()


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
    default_threads = min(32, (os.cpu_count() or 4) * 2)
    parser.add_argument(
        "--threads",
        dest="threads",
        type=int,
        default=default_threads,
        help="Number of parallel threads to use when parsing indexes and statting chunk files. "
             "Defaults to 2× available CPUs (capped at 32).",
    )
    # Backward compatibility alias
    parser.add_argument(
        "--workers",
        dest="threads",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-emoji",
        action="store_true",
        help="Disable emoji characters in console output.",
    )
    args = parser.parse_args(argv)

    _set_emoji_mode(not args.no_emoji)
    clear_console()

    ensure_required_tools()

    if args.threads <= 0:
        sys.stderr.write(
            f"{ICONS['error']} Error: invalid thread count ({args.threads}). Using default value {default_threads}.\n"
        )
        args.threads = default_threads

    # ----- Interactive mode detection -----
    interactive = not args.datastore and not args.searchpath
    if (args.datastore and not args.searchpath) or (args.searchpath and not args.datastore):
        parser.error("Either provide both --datastore and --searchpath, or none for interactive mode.")

    if interactive:
        res = _interactive_menu(args)
        if res is None:
            print("Aborted.")
            return 130
        args.datastore, args.searchpath = res
        clear_console()

    elif not DATASTORE_PATTERN.fullmatch(args.datastore or ""):
        sys.stderr.write(
            f"{ICONS['error']} Error: invalid datastore name '{args.datastore}'. "
            "Only letters, digits, '.', '_', and '-' are allowed.\n"
        )
        return 1

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

    datastore_root = Path(datastore_path).resolve()
    if not datastore_root.is_dir():
        sys.stderr.write(
            f"{ICONS['error']} Error: datastore path is not a directory → {datastore_root}\n"
        )
        return 1

    try:
        search_path_obj = resolve_search_path(str(datastore_root), args.searchpath or "/")
    except ValueError as exc:
        sys.stderr.write(f"{ICONS['error']} Error: {exc}\n")
        return 1

    chunks_root = datastore_root / ".chunks"
    search_path = str(search_path_obj)

    print(f"{ICONS['folder']} Path to datastore: {datastore_root}")
    print(f"{ICONS['chunk']} Chunk path: {chunks_root}")
    print(f"{ICONS['folder']} Search path: {search_path}")
    print(f"{ICONS['threads']} Threads: {args.threads}")

    if not search_path_obj.is_dir():
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
    with futures.ThreadPoolExecutor(max_workers=args.threads) as pool:
        futs = {pool.submit(extract_chunks_from_file, f): f for f in index_files}
        for fut in futures.as_completed(futs):
            try:
                digests = fut.result()
                digest_counter.update(digests)
            except Exception as e:
                sys.stderr.write(f"\n{ICONS['warning']} Warning: failed to parse {futs[fut]}: {e}\n")
            processed += 1
            elapsed_display = format_elapsed(time.time() - start_ts)
            _progress_line(
                f"{ICONS['index']} Index",
                processed,
                total_files,
                f"| {ICONS['timer']} {elapsed_display}",
            )

    print()

    chunk_counter_total = sum(digest_counter.values())
    total_unique = len(digest_counter)

    if total_unique == 0:
        print(f"{ICONS['info']} No chunks referenced. Nothing to sum.")
        return 0

    # ----- Sum chunk files under .chunks -----
    print(f"{ICONS['sum']} Summing up chunks")

    missing_count = 0
    summed = 0
    unique_bytes = 0
    duplicate_bytes = 0

    def _stat_one(digest: str) -> Tuple[int, bool]:
        path = chunk_path_for_digest(chunks_root, digest)
        size = stat_size_if_exists(path)
        missing = (size == 0 and not path.exists())
        return size, missing

    with futures.ThreadPoolExecutor(max_workers=args.threads) as pool:
        futs2 = {pool.submit(_stat_one, d): d for d in digest_counter.keys()}
        for fut in futures.as_completed(futs2):
            try:
                size, missing = fut.result()
                unique_bytes += size
                occurrences = digest_counter[futs2[fut]]
                if occurrences > 1 and size:
                    duplicate_bytes += size * (occurrences - 1)
                if missing:
                    missing_count += 1
                    print(
                        f"\r\033[K{ICONS['missing']} Missing: {chunk_path_for_digest(chunks_root, futs2[fut])}",
                        flush=True,
                    )
            except Exception as e:
                sys.stderr.write(f"\n{ICONS['warning']} Warning: failed to stat chunk {futs2[fut]}: {e}\n")
            summed += 1
            elapsed_display = format_elapsed(time.time() - start_ts)
            _progress_line(
                f"{ICONS['chunk']} Chunk",
                summed,
                total_unique,
                f"| {ICONS['total']} Size so far: {human_readable_size(unique_bytes)} "
                f"| {ICONS['timer']} {elapsed_display}",
            )

    print()
    print("\033[2K", end="")

    print(f"{ICONS['total']} Total size: {unique_bytes} Bytes ({human_readable_size(unique_bytes)})")

    total_elapsed = format_elapsed(time.time() - start_ts)
    print(f"{ICONS['timer']} Evaluation duration: {total_elapsed}")

    duplicate_count = chunk_counter_total - total_unique
    unique_percent = (total_unique / chunk_counter_total * 100) if chunk_counter_total else 0.0
    duplicate_percent = 100.0 - unique_percent if chunk_counter_total else 0.0
    print(f"{ICONS['puzzle']} Chunk usage summary:")
    # Align summary values in table-like columns
    label_unique = "Unique chunks"
    label_dupe = "Duplicate refs"
    label_total = "Total references"
    count_unique = f"{total_unique}"
    count_dupe = f"{duplicate_count}"
    count_total = f"{chunk_counter_total}"
    perc_unique = f"{unique_percent:.2f}%"
    perc_dupe = f"{duplicate_percent:.2f}%"
    perc_total = ""  # no percentage for total references
    size_unique = human_readable_size(unique_bytes)
    size_dupe = human_readable_size(duplicate_bytes)
    size_total = human_readable_size(unique_bytes + duplicate_bytes)

    w_label = max(len(label_unique), len(label_dupe), len(label_total))
    w_count = max(len(count_unique), len(count_dupe), len(count_total))
    w_perc = max(len(perc_unique), len(perc_dupe), len(perc_total))
    w_size = max(len(size_unique), len(size_dupe), len(size_total))

    print(
        f"  {label_unique.ljust(w_label)} : {count_unique.rjust(w_count)}  {perc_unique.rjust(w_perc)} | {size_unique.rjust(w_size)}"
    )
    print(
        f"  {label_dupe.ljust(w_label)} : {count_dupe.rjust(w_count)}  {perc_dupe.rjust(w_perc)} | {size_dupe.rjust(w_size)}"
    )
    print(
        f"  {label_total.ljust(w_label)} : {count_total.rjust(w_count)}  {perc_total.rjust(w_perc)} | {size_total.rjust(w_size)}"
    )

    if missing_count:
        print(f"{ICONS['warning']} Missing chunk files: {missing_count}")

    return 0


# =============================================================================
# Program entrypoint
# =============================================================================

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(130))
    sys.exit(main())
