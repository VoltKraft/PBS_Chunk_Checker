# Contributing to PBS_Chunk_Checker

Thanks for your interest in contributing! Bug fixes, docs improvements, and small features are all welcome.

Note on project nature: this is a single‑maintainer hobby project. There is no SLA, no commercial support, and reviews happen on a best‑effort basis.

## Code of Conduct

Please follow the guidelines in `CODE_OF_CONDUCT.md`. Be respectful and constructive.

## Ways to Contribute

- Report bugs: open an issue with clear steps to reproduce, expected vs. actual behavior, environment (PBS version, OS, Python version), and sample output if possible.
- Suggest features: open an issue first to discuss scope and fit. Small, incremental improvements are preferred over large refactors.
- Improve docs: PRs that clarify the README, examples, or error messages are highly appreciated.
- Security: do not post vulnerabilities publicly. See `SECURITY.md` for private reporting.

## Pull Request Guidelines

- Keep PRs small and focused; one change per PR when possible.
- Discuss major changes in an issue before implementation.
- No new runtime dependencies: this tool intentionally uses only the Python standard library.
- Target Python 3.9+ (uses modern type hints like `list[str]`).
- Don’t bump `__version__` in PRs; the maintainer handles releases.
- Make sure both “script mode” and “interactive mode” continue to work.
- Preserve portability: the script must run directly on a PBS host and rely on `proxmox-backup-manager` and `proxmox-backup-debug` being available.

## Development Setup

Prerequisites:
- Python 3.9+ on a Proxmox Backup Server host (or a machine with PBS CLI tools installed)
- Access to a PBS datastore for testing

Run locally:
```bash
./PBS_Chunk_Checker.py --datastore <DATASTORE_NAME> --searchpath <SEARCH_PATH>
# or interactive mode
./PBS_Chunk_Checker.py
```

Tip (offline/mocking): if you don’t have PBS tools locally, you can temporarily put simple stub scripts named `proxmox-backup-manager` and `proxmox-backup-debug` earlier in your `PATH` that print minimal JSON/text the script expects. This helps iterate on parsing and CLI behavior, but please validate on a real PBS host before submitting.

## Style and Quality

- Follow PEP 8 in spirit; prefer readable, simple code over cleverness.
- Use type hints where practical; keep functions small and focused.
- Prefer `pathlib`, `argparse`, `subprocess.run`, and f‑strings (as already used).
- Keep output user‑friendly and consistent with existing messages.
- Add comments or docstrings only where they clarify intent (no boilerplate).

## Commit Messages

- Keep them concise and informative. Prefixes like `fix:`, `feat:`, `docs:`, `refactor:` are welcome but not required.
- Reference related issues (e.g., `Fixes #123`).

## License and Contribution Terms

By contributing, you agree that your contributions are licensed under the project’s license (GPL‑3.0). See `LICENSE`.

## Maintainer Availability

Reviews and releases are best‑effort and may take time. Thank you for your patience and for helping improve PBS_Chunk_Checker!
