# Security Policy

This document explains how to report and handle security issues for PBS_Chunk_Checker in a responsible way.

Note on support: this is a hobby project maintained by a single volunteer. There is no SLA, no commercial support, and response times are best‑effort only.

## Supported Versions

- Supported (best‑effort): the `main` branch and the latest released version.
- Not supported: older releases generally do not receive security fixes; please upgrade.
- Backports: unlikely. If a fix is risky, a workaround or documentation may be provided instead of a patch.

## Reporting a Vulnerability

Please do not post sensitive details publicly. Preferred channels:

1. GitHub “Private vulnerability report” (Security Advisories), if enabled for this repository.
2. Email the maintainer (contact via the GitHub profile). If no address is available, open a brief issue without details and request a private channel.
3. Last resort: open a minimal public issue without exploit/details and ask for private follow‑up.

Helpful information to include:
- Affected version/commit and runtime environment (PBS version, OS, Python version)
- Reproduction steps and expected vs. actual behavior
- Security impact (e.g., DoS, privilege escalation, information disclosure)
- Minimal proof‑of‑concept (if needed) and relevant, sanitized logs

## Response and Disclosure Process

The following are targets, not guarantees (best‑effort, volunteer time):

- Acknowledgement: within 7 days
- Triage/verification: within 14–21 days
- Fix/release: as soon as feasible; coordinated disclosure is preferred and may take 30–90+ days depending on severity/complexity and maintainer availability
- Interim guidance: if a timely fix is not feasible, mitigation notes may be published first
- Credit: on request in the release notes (or anonymous, if preferred)

## Scope

- This policy covers code in this repository.
- Vulnerabilities in dependencies or system tools (e.g., Proxmox tools) should be reported upstream first; we will assist where helpful.
- Please do not conduct testing that risks availability, data, or the privacy of others (no DDoS, social engineering, or accessing third‑party data).

## Project Nature and Contributions

- Single‑maintainer hobby project; availability varies.
- No bug bounty program; responsible reports and high‑quality patches are appreciated.
- Well‑scoped pull requests with tests or reproduction steps can accelerate fixes.

## Safe Harbor

We support good‑faith security research. If you act without malicious intent, avoid data exfiltration and service disruption, and comply with applicable laws, we will not pursue legal action against you. Please report findings promptly and allow reasonable time for remediation.

## Handling Sensitive Information

- Do not send credentials, customer data, or other secrets.
- Sanitize logs and PoCs where possible.

## Disclaimer

This project is provided “as is.” To the maximum extent permitted by law, all warranties and liability are disclaimed. The warranty and liability terms in the GPL‑3.0 license also apply. Use at your own risk; validate changes before deploying to production. See `LICENSE`.

Non‑professional notice: the maintainer is not a professional application developer; this is a personal, hobby effort provided in good faith without guarantees or support obligations.

Last updated: 2025‑10‑29
