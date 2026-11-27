# Java quality check mapping

The `smq.java-quality` Gradle convention plugin owns the versions and settings in this table. The checks run through the existing `check` task on local, Linux, and Windows builds.

| SPEC ID | Check | Applied source sets | Enforcement |
| --- | --- | --- | --- |
| TOOL-001, STYLE-008 | javac `-Xlint:unchecked` | main, test, benchmark | warnings fail main compilation; errors fail test and benchmark |
| TOOL-001, STYLE-008 | javac `-Xlint:rawtypes` | main, test, benchmark | warnings fail main compilation; errors fail test and benchmark |
| OBJ-001 | Error Prone `ArrayEquals` | main, test, benchmark | error |
| OBJ-002 | Error Prone `ArrayToString` | main, test, benchmark | error |
| OBJ-003 | Error Prone `EqualsIncompatibleType` | main, test, benchmark | error |
| OBJ-004 | Error Prone `EqualsHashCode` | main, test, benchmark | error |
| OBJ-005 | Error Prone `EnumOrdinal` | main, test, benchmark | error |
| OBJ-006 | Error Prone `ModifyCollectionInEnhancedForLoop` | main, test, benchmark | error |
| OBJ-007 | Error Prone `ModifySourceCollectionInStream` | main, test, benchmark | error |
| OBJ-010 | Error Prone `NarrowCalculation` | main, test, benchmark | error |
| OBJ-010 | Error Prone `NarrowingCompoundAssignment` | main, test, benchmark | error |
| OBJ-010 | Error Prone `FloatingPointLiteralPrecision`, `IntLongMath`, `TimeUnitConversionChecker` | main, test, benchmark | error |
| OBJ-011 | Error Prone `DefaultLocale` | main, test, benchmark | error |
| OBJ-012 | Error Prone `DefaultCharset` | main, test, benchmark | error |
| ERR-008 | Error Prone `UnusedException` | main, test, benchmark | error |
| ERR-007 | Error Prone `CatchAndPrintStackTrace` | main, test, benchmark | error |
| CON-002 | Error Prone `FutureReturnValueIgnored`, `WaitNotInLoop` | main, test, benchmark | error |
| CON-002 | Error Prone `LockNotBeforeTry`, `NonAtomicVolatileUpdate`, `SynchronizeOnNonFinalField` | main, test, benchmark | error |
| CON-002 | Error Prone `InterruptedExceptionSwallowed` | main, benchmark | error |
| | Error Prone `JavaTimeDefaultTimeZone`, `MissingFail` | main, test, benchmark | error |
| Multiple | [PMD MUST rules](../../config/pmd/must.xml) | main, test, benchmark | any violation fails the build |
| Multiple | [PMD SHOULD rules](../../config/pmd/should.xml) | main, test, benchmark | any violation fails the build |
| TOOL-003 | SpotBugs 4.10.5 with maximum effort and low confidence threshold | main | findings fail the build |
| NUL-001 | NullAway / JSpecify | main | error; only `@NullMarked` code, explicit nullness required |
| STYLE-002 | Spotless with Palantir Java Format 2.103.0 | src main, test, benchmark, deploy-test Java | `spotlessCheck` is part of `check` |

Error Prone 2.51.0 runs its default checks and the explicitly adopted checks. Main compilation treats warnings as errors; test and benchmark compilations retain their existing error levels and Korean test scenario names. `InterruptedExceptionSwallowed` is an error in main and benchmark.

For additional disabled-by-default checks and suggestions, run `./gradlew --rerun-tasks compileJava -PextendedErrorProne`. Diagnostics appear in the console and Gradle problems report (`build/reports/problems/problems-report.html`); this optional compilation can fail on diagnostics. Omit the property for the normal gate.

NullAway 0.14.3 and explicit JSpecify nullness marking are errors for `main` only. Public nullable contracts, runtime null checks, and the non-blocking CodeQL operating policy remain as specified in [ADR 0011](../adr/0011-static-analysis-is-strict-in-main-and-advisory-in-code-scanning.md).

Test fixtures retain narrowly scoped PMD suppressions for behavior that the tests or their callers require:

- `SystemPrintln` preserves subprocess readiness/output protocols and the concurrency test's diagnostic output.
- `AvoidThrowingRawExceptionTypes` preserves the fixture's existing exception type and cause when a worker or fake executor fails.
- `UseTryWithResources` permits a snapshot race test to call `close` at its intentional synchronization point and again during cleanup.
- `UnusedLocalVariable` permits samplers to remain open for the body of their try-with-resources, even though the variable itself is not read.
- `DoNotCallGarbageCollectionExplicitly` is necessary to probe weakly reachable records during a session.

Benchmark code retains narrowly scoped PMD suppressions to preserve its external behavior:

- `SystemPrintln` preserves the parent and child CLI output, progress, errors, and module-path diagnostics.
- `UseTryWithResources` preserves `RandomBenchMain.run`'s optional JFR lifecycle and `RecordSpool.close`'s synchronized close order and exception behavior.
- `CloseResource` preserves `RandomBenchMain.run`'s optional JFR recording, which its cleanup path closes.

## PMD enforcement and reports

PMD 7.29.0 analyzes `main`, `test`, and `benchmark`, excluding `module-info.java`. The [MUST ruleset](../../config/pmd/must.xml) contains 86 built-in rule IDs, grouped as follows, and the custom `PolicyException` rule:

| PMD category | MUST rules |
| --- | ---: |
| bestpractices | 21 |
| codestyle | 7 |
| design | 6 |
| errorprone | 40 |
| multithreading | 5 |
| performance | 5 |
| security | 2 |

Both adopted PMD rulesets cover priorities 1 through 5. Violations fail their respective tasks in all three source sets. Run `./gradlew --continue pmdMain pmdTest pmdBenchmark pmdMainShould pmdTestShould pmdBenchmarkShould` to collect the gate reports.

Run `./gradlew pmdMainAll pmdTestAll pmdBenchmarkAll` for the separate, non-blocking report of all eight Java categories at priorities 1–5. Its ruleset excludes `LoosePackageCoupling`, which requires a project-specific list, and `CommentDefaultAccessModifier` and `CommentRequired`, which conflict with the comment policy, and `PrematureDeclaration`, which conflicts with the adopted block-start declaration rule. The XML and HTML reports are under `build/reports/pmd/`, with task names distinguishing the gate and extended reports. Linux and Windows CI upload gate HTML reports as `linux-pmd-report` and `windows-pmd-report`.

`SqliteStorage` retains narrowly scoped PMD suppressions for these contracts:

- `UnusedAssignment` keeps the initial cleanup flag for exceptions during statement creation or setup; the SHOULD ruleset also runs that MUST rule with findings suppressed so it can validate the suppression, while the MUST tasks enforce the findings.
- `UseTryWithResources` preserves connection and lock ownership when initialization succeeds, closes them on failure, and releases storage-owned resources in order on close.
- `CloseResource` preserves the same connection ownership and failure cleanup in `open` after the declaration and initialization are separated.
- `PreserveStackTrace` retains existing exception translation on open and rethrows the original interruption cause from the two deadline-aware methods.
- `ExhaustiveSwitchHasDefault` preserves the documented exception for unknown dispositions.
- `ExceptionAsFlowControl` keeps the established wrapped I/O error for an existing snapshot target and for invalid encoded header lengths.

PMD MUST tasks also check each PMD suppression for a matching `@PolicyException`, a nonempty reason and approver, and an unexpired ISO-8601 review date. The tasks rerun when the date changes and do not reuse PMD incremental results. Existing Error Prone exceptions retain their documented adjacent explanations.

## SpotBugs report

Run `./gradlew spotbugsMain` to analyze the compiled production classes with SpotBugs 4.10.5. It uses maximum analysis effort and reports findings down to low confidence. The existing `check` task also runs it. Findings without a documented exclusion fail the build and appear in `build/reports/spotbugs/main.html`. Linux and Windows CI upload that report as `linux-spotbugs-report` and `windows-spotbugs-report`, respectively.

## STYLE-011 comment policy

`pmdMainComments` and `pmdTestComments` enforce STYLE-011 through the PMD 7 Java rule in `pmd-rules`; both tasks and the rule tests run in `check`. Generated source directories are excluded. Javadoc on API declarations is forbidden until a public API documentation policy is agreed; package documentation remains permitted. PMD 7.29 cannot parse both modifiers on the module descriptor’s `requires static transitive` directive, so the comment task analyzes a copy with one modifier blanked while preserving line and column positions. Parsing errors fail the task. Reports identify STYLE-011 and the file and line. Spotless does not configure `toggleOffOn()`, so neither toggle is currently accepted. If enabled, configure the rule's `spotlessToggles` property together with Spotless and use matched pairs.

ADR 0011 and AGENTS.md require an adjacent explanation for intentional Error Prone suppressions, while STYLE-011 and STYLE-005 section 9.4 prohibit ordinary comments and comment-based suppression. The repository instructions take precedence for these explanations: only the existing specific Error Prone rationale comments are permitted, alongside their matching annotations; PMD suppression comments remain forbidden. This exception retains the reviewable reason without treating the comment as a suppression mechanism. New exceptions require review and a corresponding rule test. The `ArrayRecordComponent` suppressions keep the public API and the internal payload ownership contracts unchanged.

### PMD SHOULD test baseline (#247)

On Linux with JDK 25, integration commit `aab13e0` reported 89 violations in
`pmdTestShould`: 52 CloseResource, 14 UnnecessaryWarningSuppression,
13 AvoidCatchingGenericException, 6 CognitiveComplexity, 2 CyclomaticComplexity,
and 2 UnitTestShouldIncludeAssert. The XML and HTML reports identify each location.
The findings were resolved or addressed with narrowly scoped, justified exceptions before the test gate was enabled.

### PMD SHOULD benchmark baseline (#248)

On Linux with JDK 25, the initial `pmdBenchmarkShould` baseline was 58 violations:
20 CyclomaticComplexity, 12 AvoidCatchingGenericException, 11 CognitiveComplexity,
9 UnnecessaryWarningSuppression, 5 CloseResource, and 1 AvoidReassigningParameters.
After the rule changes in #242 and #247, the benchmark cleanup started at 57.
The findings were resolved or addressed with narrowly scoped, justified exceptions before the benchmark gate was enabled.
`RandomRun` coordinates the workload, recording, and judgment phases; its aggregate
class complexity is excepted with a class-node-only XPath and `@PolicyException`.
Method complexity findings remain enabled, including for `RandomRun`.
