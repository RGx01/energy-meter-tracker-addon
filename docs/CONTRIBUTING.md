# Contributing to Energy Meter Tracker

Thank you for your interest in contributing. This document covers how to report bugs, suggest features and submit pull requests.

## Reporting Bugs

Please open an issue on GitHub with:

- Your HA installation type (OS, Supervised, Docker)
- Add-on version
- A description of the problem and steps to reproduce
- Relevant log output from the **Logs** page or `docker logs`
- Whether the issue is consistent or intermittent

For data accuracy issues, include a sample of the affected blocks from `blocks.json` if possible (please censor identifiable info such as names addresses MPANs etc).

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
4. Run the unit tests: `python3 -m unittest test_engine -v`
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

- Changes that break backward compatibility with existing `blocks.json` data
- New dependencies without a strong justification
- UI changes that break Safari compatibility
- Removing the informational disclaimer from charts or help pages

## Questions

For general questions about usage, open a discussion on GitHub or post in the Home Assistant community forum thread.