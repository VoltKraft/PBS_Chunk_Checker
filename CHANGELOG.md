# 📝 Changelog

- 2.8.1
  - Rename the main script file from `PBS_Chunk_Checker.py` to `pbs_chunk_checker.py` and adjust the self‑update logic and GitHub release workflow accordingly.
  - Make the codebase more PEP‑8 compliant (import grouping, line breaks, and long f‑strings) without changing runtime behavior.
  - Documentation updates: use the new lowercase filename in all usage examples and add a note that older installations may need a one‑time manual rename after updating.
- 2.8.0
  - Add an `--all-guests` mode and interactive option to scan the entire datastore (all namespaces) and list every VM/CT by size with a sortable overview.
  - Allow scoping the all-guests scan to a specific namespace via `--searchpath` or the interactive path selector.
  - Show live per-guest list and running summary while guests are being processed.
  - Show a confirmation prompt before starting the datastore-wide scan because it can take a long time.
- 2.7.2
  - Rename CLI flag `--check-updates` to `--update`.
  - Keep `--version` as pure version output again.
- 2.7.1
  - Split version and update logic: `--version` prints version; `--check-updates` checks and offers updates.
+
