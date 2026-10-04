# Roadmap

*Active backlog only — priority ordered. Shipped, closed and superseded items live in
[ROADMAP_archive.md](ROADMAP_archive.md); release detail is in [CHANGELOG.md](../CHANGELOG.md).
Nothing in this file has shipped.*

**4.5.14 — the last v4 feature release.** BL-71, BL-72 and BL-73 landed and have moved to the
archive. After .14 this repository accepts **critical fixes only** — data-loss, crash-on-start or a
pricing error — each cherry-picked into the EMT repository the same day. A fix that adds a heal also
raises the floor version EMT will import from.

**The open backlog moved to EMT on 4 Oct 2026.** v4 now takes critical fixes only, so every open
item is v5 work. Each was assessed against EMT's code and carried to
[RGx01/EMT `docs/ROADMAP.md` §F](https://github.com/RGx01/EMT/blob/dev/docs/ROADMAP.md), keeping its BL
number; the full entries are in [ROADMAP_archive.md](ROADMAP_archive.md#moved-to-emt--4-oct-2026).

| Item | In EMT |
|------|--------|
| BL-50 — Unify user-job mutual exclusion | done — workstream B |
| BL-33 — Remove the one-time legacy migrations | §F, deletion |
| BL-61 — 4.5.7 settlement/chart cleanup follow-ups | §F — (1) deletion, (5) with schema step 5 |
| BL-63 — Two pricing models: collapse onto one | §F, deletion |
| BL-74 — Retire the `needs_review` writes nothing can display | §F, deletion + fix |
| BL-76 — Device grid attribution: proportional split | §F, forward rule + importer |
| BL-75 — Settlement visibility | §F, split into parts |
| BL-47 — Auto-run recorder attribution across gap-filled windows | §F |
| BL-60 — Usage Stats block inspector | §F |
| BL-64 — Bill-parser fixtures + a pypdf bump gate | §F |

New v4 work is a critical fix — data loss, crash-on-start or a pricing error — raised as an issue
here and ported to EMT as its forward fix.
