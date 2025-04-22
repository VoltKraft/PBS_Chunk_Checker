# PBS_Chunk_Checker
## What is this about
### The problem
I was faced with the problem that I wanted to know how much disk space the backup of a single VM or a single namespace consumes.
Unfortunately, the GUI of the Proxmox Backup Server only shows the total size of the VM's virtual hard disks and not the actual memory used on the datastore.
This is because the chunks that are used for the backup of VM1 can also be used for VM2 or even several times for VM1 if they are the same.
This is good for saving storage space, but if I have several tenants on my backup server, for example, I cannot differentiate between how much of the storage used is attributable to tenant A and how much to tenant B. However, this can be important if I want to charge for the storage space used.
### The solution
For example, to find out how much disk space the namespace “Tenant A” alone would consume, I have to look at all index files of all restore points from each host, vm and ct backup, and read out all chunks used.
That's exactly what the script does.
It first lists all chunks, removes all but one of the chunks that are used multiple times and calculates how much memory these chunks require.
## Usage
```bash
./PBS_Chunk_Checker "<PATH_TO_DATASTORE>" "<SERCHPATH>"
```
Example:
```bash
./PBS_Chunk_Checker "/MyDatastore" "/ns/MyNamespace"
```
