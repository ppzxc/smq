# Dependency vulnerability scan

The `Dependency scan / dependency-scan` check runs on every pull request and each Monday at 03:00 UTC. The CycloneDX Gradle plugin generates an SBOM from Gradle's resolved dependency graph, scanning all resolvable configurations. This includes direct and transitive runtime, test, and benchmark dependencies, including `sqlite-jdbc`.

The gate fails for vulnerabilities that Trivy rates High or Critical and for which a fixed version is available. Unfixed findings and lower severities do not block the check. The scan needs no repository secret.

## Exceptions

Record exceptions in `.trivyignore.yaml`. Every entry must identify the vulnerability, set `expired_at`, and give a `statement` containing the risk rationale and the approving maintainer (for example, `Rationale: ...; Approved by: @maintainer`). Remove or renew an exception before it expires. Do not add an exception without a named maintainer approval.

The exception file currently contains no accepted vulnerabilities.
