# PBS_Chunk_Checker (Python Edition)

## 🧩 Overview

The **PBS_Chunk_Checker** is a diagnostic and analysis tool for **Proxmox Backup Server (PBS)** datastores.  
It calculates the **real disk space usage** of a specific **namespace**, **VM**, or **container** by summing only the **unique chunk files** that are actually referenced.

This allows accurate insights into space consumption per tenant or object — useful for chargeback, reporting, and storage optimization.

**Current version:** 2.7.2 (`./PBS_Chunk_Checker.py --version`)

See full changes in `CHANGELOG.md`.

---

## 💡 Why This Script Exists

### The Problem
The PBS web UI shows only the total (provisioned) disk size of virtual disks.  
However, it doesn’t display the *actual storage usage* due to deduplication across backups.

Deduplicated chunks can be shared by:
- Multiple restore points,
- Multiple VMs or containers,
- Different namespaces.

Hence, the “used space” per VM or namespace cannot be determined directly.

### The Solution
This script performs a deep inspection of PBS index files (`*.fidx`, `*.didx`) to:
1. Identify all referenced chunk digests.
2. Deduplicate them.
3. Calculate the total byte size of these unique chunk files.

The result is the **true storage usage** of the selected backup object.

---

## ⚙️ Usage

You can run the checker either from a local copy (useful for repeated runs) or fetch it on the fly without leaving any files behind.

### Local execution
Clone or download the repository, then run the script with Python.

Syntax (script mode):
```bash
./PBS_Chunk_Checker.py --datastore <DATASTORE_NAME> --searchpath <SEARCH_PATH> [--threads N]
```

Examples:
```bash
# Namespace summary
./PBS_Chunk_Checker.py --datastore MyDatastore --searchpath /ns/MyNamespace

# VM inside a namespace
./PBS_Chunk_Checker.py --datastore MyDatastore --searchpath /ns/MyNamespace/vm/100
```

### Interactive mode
Run without parameters to open a menu for selecting the datastore and the search path:

```bash
./PBS_Chunk_Checker.py
```

In interactive mode you can:
- Select an existing datastore from the list or enter one manually
- Navigate the datastore directory structure and choose the search path (or enter it manually)
- Open the Options overlay (press `o`) to adjust threads and toggle emoji output

Interactive controls (TUI):
- Use Up/Down arrows (or j/k) to move
- Press Space or Enter to select/confirm
- Press m to enter a value/path manually
- Press o to open Options (threads)
- Press v to show current version
- Press q (or Esc) to abort
- Inside the Options overlay, press Space to toggle items (Emoji output shows ✔/✘)

Notes:
- The TUI uses the built-in Python curses module; no extra packages are required.
- If your terminal doesn’t support curses, the script falls back to the numeric menu.
- Set `PBS_CC_NO_CURSES=1` to force the numeric menu.

### What “threads” do

Threads are the number of parallel operations the script uses to:
- Parse PBS index files (`*.fidx`, `*.didx`)
- Stat chunk files under the `.chunks` directory

More threads can significantly speed up evaluations on fast storage and when many files are involved. However, setting the value too high can cause disk thrashing or extra CPU load, which might reduce overall throughput on slower disks.

Defaults: `2 × CPU cores`, capped at `32`.

You can change the value in interactive mode by pressing `o` (Options) or by passing `--threads N` on the command line. Within the Options overlay you can also toggle emoji output (equivalent to `--no-emoji`) using the Space key.

### Portable execution (no local file)
Stream the script from GitHub and execute it immediately.

Script mode example:

```bash
wget -q -O - https://raw.githubusercontent.com/VoltKraft/PBS_Chunk_Checker/main/PBS_Chunk_Checker.py | python3 - --datastore MyDatastore --searchpath /ns/MyNamespace
```

Interactive mode example:

```bash
wget -q -O - https://raw.githubusercontent.com/VoltKraft/PBS_Chunk_Checker/main/PBS_Chunk_Checker.py | python3 -
```

Notes:
- The hyphen after `python3` instructs Python to read the script from STDIN, so no file remains on disk.
- Replace `MyDatastore` and the search path with your desired PBS datastore and object (e.g. `/ns/MyNamespace/vm/100`).
- Running it this way always fetches the latest version from the repository.
- When both `--datastore` and `--searchpath` are omitted, the interactive mode starts automatically.

### Parameters
| Option | Requirement | Description | Default |
|--------|-------------|-------------|---------|
| `--datastore` | Required (script mode) | PBS datastore name that contains the object you want to analyse | — |
| `--searchpath` | Required (script mode) | Object path inside the datastore (e.g. `/ns/MyNamespace` or `/ns/MyNamespace/vm/100`) | — |
| `--threads` | Optional | Degree of parallelism for parsing index files and statting chunks | `2 × CPU cores (max 32)` |
| `--no-emoji` | Optional | Replace emoji icons in CLI output with ASCII labels | Emoji output |
| `--version` | Optional | Show the script version and exit | — |
| `--update` | Optional | Check for new releases and offer self-update, then exit | — |

---

## 🔄 Update (Releases)

In the interactive menus, press `v` (Version) to see the current version. While opening this menu, the script checks in the background for a newer release on GitHub. If one is available, you will be offered to update automatically. The script downloads the latest `PBS_Chunk_Checker.py` and replaces the current file atomically (a `.bak` backup is kept next to it). Restart the script to use the new version.

You can now also use `--update` on the command line to perform the same check-and-offer flow without entering the interactive menus. This prints the current version, queries GitHub Releases, and — if a newer version exists — asks whether to update.

Notes:
- The update mechanism uses GitHub Releases. If no dedicated asset is attached, it falls back to the tagged raw file.
- When a `.sha256` checksum asset is published with a release, the script verifies the downloaded file before replacing the current version.
- Network errors or rate limits will be reported and do not affect normal operation.

---

## 📊 Output Example

```
📁 Path to datastore: /mnt/datastore/MyDatastore
📦 Chunk path: /mnt/datastore/MyDatastore/.chunks
📁 Search path: /mnt/datastore/MyDatastore/ns/MyNamespace
🧵 Threads: 8

💾 Saving all used chunks
📄 Index 75/75 (100.00%) | ⏱️ 02m 15s
➕ Summing up chunks
📦 Chunk 12450/12450 (100.00%) | 🧮 Size so far: 1.23TiB | ⏱️ 12m 09s

🧮 Total size: 1356782934123 Bytes (1.23TiB)
⏱️ Evaluation duration: 12m 10s
🧩 Chunk usage summary:
  Unique chunks     :  8505    9.59% |   12.2GiB
  Duplicate refs    : 80186   90.41% |  186.2GiB
  Total references  : 88692          |  198.4GiB
```

---

## 🧾 Output Details

- Clear console on start for a clean view.
- Path header order: datastore path → chunk path → search path.
- Live runtime timer (⏱️) on both index and chunk progress lines.
- Thread count prints with a thread icon (ASCII fallback: [THREADS]).
- Progress lines include a completion percentage for Index and Chunk phases.
- Total size prints the actual on-disk size of unique chunks referenced by the selected object.
- Chunk usage summary:
  - Values are aligned in columns for readability (label, count, percent, size).
  - Unique chunks: number of distinct chunk digests, share of all references, and their total size.
  - Duplicate refs: additional references to already-counted chunks and their logical size.
  - Total references: overall reference count and the logical size when counting duplicates.

---


## ⚠️ Notes

- The script requires **no additional Python packages** — it uses only built-in modules.
- Runs on **Linux PBS hosts**; other operating systems are not supported.
- It must be executed **directly on a PBS host** because it depends on:
  - `proxmox-backup-manager`
  - `proxmox-backup-debug`
- The script validates that these CLI tools are available before starting and aborts with an actionable error if they are missing.
- Use the `--no-emoji` flag when your terminal cannot display Unicode emoji; the script will switch to ASCII labels automatically.

---

**Author:** Jan Paulzen (VoltKraft)
