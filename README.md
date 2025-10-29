# PBS_Chunk_Checker (Python Edition)

## 🧩 Overview

The **PBS_Chunk_Checker** is a diagnostic and analysis tool for **Proxmox Backup Server (PBS)** datastores.  
It calculates the **real disk space usage** of a specific **namespace**, **VM**, or **container** by summing only the **unique chunk files** that are actually referenced.

This allows accurate insights into space consumption per tenant or object — useful for chargeback, reporting, and storage optimization.

**Current version:** 2.1.0 (`./PBS_Chunk_Checker.py --version`)

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

Syntax:
```bash
./PBS_Chunk_Checker.py --datastore <DATASTORE_NAME> --seachpath <SEARCH_PATH> [--workers N]
```

Examples:
```bash
# Namespace summary
./PBS_Chunk_Checker.py --datastore MyDatastore --seachpath /ns/MyNamespace

# VM inside a namespace
./PBS_Chunk_Checker.py --datastore MyDatastore --seachpath /ns/MyNamespace/vm/100
```

### Portable execution (no local file)
Stream the script from GitHub and execute it immediately.

```bash
wget -q -O - https://raw.githubusercontent.com/VoltKraft/PBS_Chunk_Checker/main/PBS_Chunk_Checker.py | python3 - --datastore MyDatastore --seachpath /ns/MyNamespace
```

Notes:
- The hyphen after `python3` instructs Python to read the script from STDIN, so no file remains on disk.
- Replace `MyDatastore` and the search path with your desired PBS datastore and object (e.g. `/ns/MyNamespace/vm/100`).
- Running it this way always fetches the latest version from the repository.

### Parameters
| Option | Requirement | Description | Default |
|--------|-------------|-------------|---------|
| `--datastore` | Required | PBS datastore name that contains the object you want to analyse | — |
| `--seachpath` | Required | Object path inside the datastore (e.g. `/ns/MyNamespace` or `/ns/MyNamespace/vm/100`) | — |
| `--workers` | Optional | Degree of parallelism for parsing index files and statting chunks | `2 × CPU cores (max 32)` |
| `--version` | Optional | Show the script version and exit | — |

---

## 📊 Output Example

```
📁 Path to datastore: /mnt/datastore/MyDatastore
📁 Search path: /mnt/datastore/MyDatastore/ns/MyNamespace
📁 Chunk path: /mnt/datastore/MyDatastore/.chunks

💾 Saving all used chunks
📄 Index 75/75
➕ Summing up chunks
📦 Chunk 12450/12450 | 🧮 Size so far: 1.23TiB

🧮 Total size: 1356782934123 Bytes (1.23TiB)
⏱️ Evaluation duration: 0 hours, 24 minutes, and 32 seconds
🧩 Unique chunks: 12450 (91.45% unique, 8.55% duplicates)
📁 Searched object: /ns/MyNamespace
```

---

## ⚠️ Notes

- The script requires **no additional Python packages** — it uses only built-in modules.
- It must be executed **directly on a PBS host** because it depends on:
  - `proxmox-backup-manager`
  - `proxmox-backup-debug`

---

## 🚀 Performance Improvements

This new Python version is designed for **significantly faster processing**:
- Parallelized parsing and chunk size summation with `ThreadPoolExecutor`
- Reduced overhead by avoiding file I/O redirection and external logging
- Efficient deduplication using Python’s `Counter` and `set` types

---

**Author:** Jan Paulzen (VoltKraft) 
**Version:** 2.1.0 (Python rewrite, performance-optimized)
