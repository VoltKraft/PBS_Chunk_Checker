# PBS_Chunk_Checker
## What is this about
### The problem
The Proxmox Backup Server (PBS) web interface only displays the total (thick) size of a VM's virtual hard disks. However, it does not show the actual storage used on the datastore.

This leads to a challenge:
Backup chunks may be shared across multiple restore points, VMs, or even within the same VM. While this deduplication is great for saving space, it makes it difficult to determine how much disk space is truly consumed by a specific VM, container, or namespace — especially in multi-tenant environments.

For example, if Tenant A and Tenant B both use overlapping chunks, the total space shown includes shared data. This makes it hard to allocate costs fairly or understand usage distribution per tenant.
### The solution
To accurately determine the disk usage of a specific namespace, VM, or container, this script:

1. Scans all index files (*.fidx, *.didx) in the specified datastore path.

2. Extracts all unique chunk IDs referenced.

2. Deduplicates the chunk list (since chunks may be used multiple times).

4. Sums the total storage size of the actual chunk files referenced.

This gives you the real disk usage (in bytes) of the selected backup object — accounting only for the unique chunks it uses.
## Usage
### Syntax
```bash
./PBS_Chunk_Checker "<DATASTORE_NAME>" "<SERCHPATH>"
```
Example:
Check the size of the namespace “MyNamespace”:
```bash
./PBS_Chunk_Checker "MyDatastore" "/ns/MyNamespace"
```
Check the size of the VM with ID 100 within the namespace “MyNamespace”:
```bash
./PBS_Chunk_Checker "MyDatastore" "/ns/MyNamespace/vm/100"
```
### Recommendation for use
The runtime of the script depends on the number and size of restore points. For large namespaces, the analysis may take several hours.

For long-running evaluations, it is recommended to run the script inside a [screen](https://www.gnu.org/software/screen/manual/screen.html "Screen User's Manual") session or a similar terminal multiplexer.

Redirect the output to a log file for later review:
```bash
./PBS_Chunk_Checker "MyDatastore" "/ns/MyNamespace" | tee mynamespace_report.log
```