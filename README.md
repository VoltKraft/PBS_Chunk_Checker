# PBS_Chunk_Checker
## What is this about
### The Problem
I was faced with the problem that I wanted to know how much disk space the backup of a single VM or a single namespace consumes.
Unfortunately, the GUI of the Proxmox Backup Server only shows the total size of the VM's virtual hard disks and not the actual memory used on the datastore.
This is because the chunks that are used for the backup of VM1 can also be used for VM2 or even several times for VM1 if they are the same.
This is good for saving storage space, but if I have several tenants on my backup server, for example, I cannot differentiate between how much of the storage used is attributable to tenant A and how much to tenant B. However, this can be important if I want to charge for the storage space used.

## Usage
```bash
./PBS_Chunk_Checker "<PATH_TO_DATASTORE>" "<SERCHPATH>"
```
Example:
```bash
./PBS_Chunk_Checker "/MyDatastore" "/ns/MyNamespace"
```
