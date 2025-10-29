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

### Syntax
```bash
./PBS_Chunk_Checker.py --datastore <DATASTORE_NAME> --seachpath <SEARCH_PATH> [--workers N]
```

### Examples
Check the total unique chunk size of a namespace:
```bash
./PBS_Chunk_Checker.py --datastore MyDatastore --seachpath /ns/MyNamespace
```

Check the total unique chunk size of a VM within a namespace:
```bash
./PBS_Chunk_Checker.py --datastore MyDatastore --seachpath /ns/MyNamespace/vm/100
```

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
