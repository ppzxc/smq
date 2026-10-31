# smq

## Agent skills

### Issue tracker

GitHub Issues (ppzxc/smq), managed via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary (needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Static analysis

For Java changes, apply the configured formatter before handing off. Keep main code under the JSpecify nullness default; mark only supported nullable type uses in the public API and retain runtime null checks. Place any Error Prone suppression on the narrowest applicable element with an adjacent explanation of the intentional exception or tool limitation.

Run `./gradlew spotlessApply` to format Java sources. `./gradlew check` runs Spotless, Error Prone, and NullAway; main treats all Error Prone diagnostics as errors, while test and benchmark retain their normal Error Prone error severity.
