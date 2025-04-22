# PBS_Chunk_Checker
## What is this about
### The Problem
I was faced with the problem that I wanted to know how much disk space the backup of a single VM or a single namespace consumes.
Unfortunately, the GUI of the Proxmox Backup Server only shows the total size of the VM's virtual hard disks and not the actual memory used on the datastore.

## Usage
```bash
./PBS_Chunk_Checker "<PATH_TO_DATASTORE>" "<SERCHPATH>"
```
Example:
```bash
./PBS_Chunk_Checker "/MyDatastore" "/ns/MyNamespace"
```
