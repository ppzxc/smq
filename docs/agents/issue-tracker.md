# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v`; `gh` does this automatically when run inside a clone.

## Title prefixes

Every issue title starts with a fixed prefix that records where it came from. `#NN` is the parent's issue number; the prefix is followed by one space and the human-readable name.

| Issue                                   | Prefix                         | Example                                          |
| --------------------------------------- | ------------------------------ | ------------------------------------------------ |
| Wayfinder map                           | `[MAP]`                        | `[MAP] smq v1 스펙`                              |
| Map ticket, `wayfinder:research`        | `[MAP #NN / RESEARCH]`         | `[MAP #1 / RESEARCH] RabbitMQ 핵심 기능 조사`    |
| Map ticket, `wayfinder:grilling`        | `[MAP #NN / GRILLING]`         | `[MAP #1 / GRILLING] v1 기능 범위 확정`          |
| Map ticket, `wayfinder:prototype`       | `[MAP #NN / PROTOTYPE]`        | `[MAP #1 / PROTOTYPE] 공개 Java API의 모양`      |
| Map ticket, `wayfinder:task`            | `[MAP #NN / TASK]`             | `[MAP #1 / TASK] ...`                            |
| Spec published from a map               | `[MAP #NN / SPEC]`             | `[MAP #1 / SPEC] smq v1`                         |
| Spec published without a map            | `[SPEC]`                       | `[SPEC] 릴리스 게이트 검증 결과를 믿을 수 있게 한다` |
| Ticket published from a spec            | `[SPEC #NN / TICKET-N]`        | `[SPEC #12 / TICKET-3] SQLite 저장소 구현`       |

- `TICKET-N` numbers the spec's tickets in order from `1`, independent of GitHub issue numbers.
- The prefix is part of the title, not a substitute for labels: keep applying the `wayfinder:*` and triage labels as usual.
- When referring to an issue by name in prose, the name is the title without its prefix.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>` for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either: resolve with `gh pr view 42` and fall back to `gh issue view 42`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body. `gh issue create --label wayfinder:map`.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue (`gh api` on the sub-issues endpoint). Where sub-issues aren't enabled, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`). Once claimed, the ticket is assigned to the driving dev.
- **Blocking**: GitHub's **native issue dependencies**, the canonical, UI-visible representation. Add an edge with `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, where `<blocker-db-id>` is the blocker's numeric **database id** (`gh api repos/<owner>/<repo>/issues/<n> --jq .id`, _not_ the `#number` or `node_id`). GitHub reports `issue_dependencies_summary.blocked_by` (open blockers only, the live gate). Where dependencies aren't available, fall back to a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Frontier query**: list the map's open children (`gh issue list --state open`, scoped to the map's sub-issues / task list), drop any with an open blocker (`issue_dependencies_summary.blocked_by > 0`, or an open issue in the `Blocked by` line) or an assignee; first in map order wins.
- **Claim**: `gh issue edit <n> --add-assignee @me`, the session's first write.
- **Resolve**: `gh issue comment <n> --body "<answer>"`, then `gh issue close <n>`, then append a context pointer (gist + link) to the map's Decisions-so-far.
