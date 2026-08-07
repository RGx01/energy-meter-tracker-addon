# Contributing to Energy Meter Tracker

Thank you for your interest in contributing. This document covers how to report bugs, suggest features and submit pull requests.

## Reporting Bugs

Please open an issue on GitHub with:

- Your HA installation type (OS, Supervised, Docker)
- Add-on version
- A description of the problem and steps to reproduce
- Relevant log output from the **Logs** page or `docker logs`
- Whether the issue is consistent or intermittent

For data accuracy issues, the most useful attachment is your `blocks.db` (the SQLite store — since 4.0.0 there is no `blocks.json`), or an export/screenshot of the affected blocks. Please censor identifiable info such as names, addresses and MPANs.

## Suggesting Features

Open a GitHub issue with the `enhancement` label. Please describe:

- What you want to achieve
- Why the current behaviour doesn't meet your need
- Any relevant context (e.g. tariff type, hardware setup)

Before suggesting a new sub-meter type or sensor, check the [Known Limitations](DEVELOPMENT.md#known-limitations--future-work) section in the development guide — some items are already on the roadmap.

## Pull Requests

### Branch naming

| Type | Pattern | Example |
|------|---------|---------|
| Bug fix | `fix/description` | `fix/rate-line-zero-on-current-day` |
| Feature | `feat/description` | `feat/gas-meter` |
| Housekeeping | `chore/description` | `chore/update-dependencies` |

### Workflow

1. Fork the repository
2. Create a branch from `dev` (not `main`)
3. Make your changes
4. Run the full test suite: `bash run_tests.sh` (runs the whole `tests/` suite — 1,850+ tests). To iterate on one area, `python3 -m pytest tests/test_engine.py -q`
5. Add tests for any new engine logic
6. Open a PR targeting `dev`

`main` is the stable release branch — PRs directly to `main` will not be accepted except from `dev` as part of a release.

### Code style

- Python: follow the existing style — 4-space indent, type hints where the existing code uses them, `logger.info/warning/error` not `print`
- JavaScript: vanilla ES5-compatible JS in templates (no build step, must work in Safari)
- HTML: Jinja2 templates, inline styles matching the existing CSS variable system
- Keep functions focused — the engine functions are deliberately small and testable

### What makes a good PR

- Fixes one thing or adds one feature
- Includes or updates unit tests for engine logic
- Updates `CHANGELOG.md` under an `## [Unreleased]` section
- Does not break the supervised mode (the primary supported mode)
- Tested against a real HA instance if possible

### What won't be accepted

- Changes that alter existing billing results (kWh/cost/carbon for already-stored history). Accuracy is the project's first commitment; `run_tests.sh` guards it.
- Changes that break reading an existing `blocks.db` (schema migrations must be additive + backward-compatible)
- New dependencies without a strong justification
- UI changes that break Safari compatibility
- Removing the informational disclaimer from charts or help pages

## Adding support for another supplier

EMT is built on Kraken (Octopus's platform), and other suppliers (EDF, etc.) run their own Kraken instances. Support for a second supplier is not built yet, but the seam exists:

- The API endpoint is already configurable — `KrakenAPIClient(base_url, graphql_url)`, stored via `save_kraken_credentials(..., base_url)` or the `KRAKEN_BASE_URL` env var.
- Supplier capability is gated in one place — `engine._API_CAPABLE_SUPPLIERS` / `supplier_is_api_capable()`, mirroring the client `WIZ_SUPPLIERS` registry.

The main things a new supplier needs are a different **auth shape** (Octopus uses an API key; others use email + password with `obtainKrakenToken`), possibly **GraphQL-only** consumption (no Octopus REST `/v1/` surface), and turning off **GB-only** features (DCC settlement, UK carbon) via capability flags. If you want to work on this, open an issue first — a shared `SupplierProfile` seam is the intended approach and worth agreeing before coding.

## Questions

For general questions about usage, open a discussion on GitHub or post in the Home Assistant community forum thread.