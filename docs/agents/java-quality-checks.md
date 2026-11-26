# Java quality check mapping

The `smq.java-quality` Gradle convention plugin owns the versions and settings in this table. The checks run through the existing `check` task on local, Linux, and Windows builds.

| SPEC ID | Check | Applied source sets | Enforcement |
| --- | --- | --- | --- |
| TOOL-001, STYLE-008 | javac `-Xlint:unchecked` | main, test, benchmark | warning; `main` also has `-Werror` |
| TOOL-001, STYLE-008 | javac `-Xlint:rawtypes` | main, test, benchmark | warning; `main` also has `-Werror` |
| OBJ-001 | Error Prone `ArrayEquals` | main, test, benchmark | error |
| OBJ-002 | Error Prone `ArrayToString` | main, test, benchmark | error |
| OBJ-003 | Error Prone `EqualsIncompatibleType` | main, test, benchmark | error |
| OBJ-004 | Error Prone `EqualsHashCode` | main, test, benchmark | error |
| OBJ-006 | Error Prone `ModifyCollectionInEnhancedForLoop` | main, test, benchmark | error |
| OBJ-007 | Error Prone `ModifySourceCollectionInStream` | main, test, benchmark | error |
| OBJ-010 | Error Prone `NarrowCalculation` | main, test, benchmark | error |
| OBJ-010 | Error Prone `NarrowingCompoundAssignment` | main, test, benchmark | error |
| NUL-001 | NullAway / JSpecify | main | error; only `@NullMarked` code, explicit nullness required |
| STYLE-002 | Spotless with Palantir Java Format 2.103.0 | src main, test, benchmark, deploy-test Java | `spotlessCheck` is part of `check` |
