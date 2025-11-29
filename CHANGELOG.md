# 📝 Changelog

- 2.8.0
  - Add an `--all-guests` mode and interactive option to scan the entire datastore (all namespaces) and list every VM/CT by size with a sortable overview.
  - Show a confirmation prompt before starting the datastore-wide scan because it can take a long time.
- 2.7.2
  - Rename CLI flag `--check-updates` to `--update`.
  - Keep `--version` as pure version output again.
- 2.7.1
  - Split version and update logic: `--version` prints version; `--check-updates` checks and offers updates.
+