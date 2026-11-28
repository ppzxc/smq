#!/usr/bin/env python3
"""Aggregate isolated designated-host samples and enforce the smq release baseline."""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


DESIGNATED_HOST = "ns0270.nanoit.kr"
DESIGNATED_FILESYSTEM = "xfs"
DURABILITIES = ["PROCESS_CRASH", "POWER_LOSS"]
QUEUE_KINDS = ["DEFAULT", "MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD"]
DEPTHS = ["EMPTY", "BACKLOG"]
BACKLOG_COUNT = 1_000_000
REPEATS = [1, 2, 3]
THROUGHPUT_MINIMUM_FRACTION = 0.8
LATENCY_MAXIMUM_FRACTION = 1.2
MINIMUM_SETTLE_DEPTH_RATIO = 0.7
SAMPLE_FILE = re.compile(r"round-(\d+)-([A-Z_]+)-([A-Z_]+)-(EMPTY|BACKLOG)\.json$")
MAX_RELEASE_RESULT_AGE_DAYS = 14
ALLOWED_RESULT_PATHS = {
    "benchmark-results/release-report.json",
    "benchmark-results/initial-comparison.json",
    "benchmark-results/initial-comparison.md",
    "smoke-harness/release-baseline.json",
}


@dataclass
class GateResult:
    failures: list[str]
    passes: list[str]


def aggregate_reports(report_dir: Path) -> dict:
    reports = sorted(report_dir.glob("round-*.json"))
    if not reports:
        raise ValueError(f"no round-*.json reports found in {report_dir}")

    samples = []
    seen = set()
    generated_times = []
    first_report = None
    normalized_config = None
    for path in reports:
        match = SAMPLE_FILE.fullmatch(path.name)
        if match is None:
            raise ValueError(f"unexpected report filename: {path.name}")
        repeat, durability, queue_kind, depth = match.groups()
        repeat = int(repeat)
        if repeat not in REPEATS:
            raise ValueError(f"repeat index out of range in {path.name}")
        report = load_json(path)
        validate_single_report(report, path)
        generated_times.append(parse_timestamp(report["generatedAt"]))
        if first_report is None:
            first_report = report
        elif report["revision"] != first_report["revision"]:
            raise ValueError("reports were produced from different source revisions")
        elif report["environment"]["host"] != first_report["environment"]["host"]:
            raise ValueError("reports were produced on different hosts")
        elif report["environment"]["workDirFileSystem"] != first_report["environment"]["workDirFileSystem"]:
            raise ValueError("reports used different work directory filesystems")

        config = report["config"]
        condition_config = dict(config)
        condition_config.update(
            {
                "durabilities": DURABILITIES,
                "queueKinds": QUEUE_KINDS,
                "depths": DEPTHS,
                "repeats": len(REPEATS),
            }
        )
        if normalized_config is None:
            normalized_config = condition_config
        elif condition_config != normalized_config:
            raise ValueError(f"reports used different workload configurations: {path.name}")
        expected_depth_count = 0 if depth == "EMPTY" else BACKLOG_COUNT
        if config.get("durabilities") != [durability]:
            raise ValueError(f"durability does not match report filename: {path.name}")
        if config.get("queueKinds") != [queue_kind]:
            raise ValueError(f"queue kind does not match report filename: {path.name}")
        if config.get("depths") != [depth]:
            raise ValueError(f"depth does not match report filename: {path.name}")
        if config.get("repeats") != 1:
            raise ValueError(f"isolated report must contain one repeat: {path.name}")
        if len(report.get("results", [])) != 1:
            raise ValueError(f"isolated report must contain one sample: {path.name}")

        sample = dict(report["results"][0])
        if sample.get("durability") != durability or sample.get("queueKind") != queue_kind:
            raise ValueError(f"sample condition does not match report filename: {path.name}")
        if sample.get("preloadedMessages") != expected_depth_count:
            raise ValueError(f"sample depth does not match report filename: {path.name}")
        if sample.get("repeat") != 1:
            raise ValueError(f"isolated sample repeat must be 1: {path.name}")
        key = (repeat, durability, queue_kind, depth)
        if key in seen:
            raise ValueError(f"duplicate sample: {key}")
        seen.add(key)
        sample["repeat"] = repeat
        samples.append(sample)

    required = {
        (repeat, durability, queue_kind, depth)
        for repeat in REPEATS
        for durability in DURABILITIES
        for queue_kind in QUEUE_KINDS
        for depth in DEPTHS
    }
    missing = sorted(required - seen)
    if missing:
        raise ValueError(f"missing required sample(s): {missing}")

    config = dict(first_report["config"])
    config.update(
        {
            "durabilities": DURABILITIES,
            "queueKinds": QUEUE_KINDS,
            "depths": DEPTHS,
            "repeats": len(REPEATS),
        }
    )
    results = sorted(
        samples,
        key=lambda row: (
            DURABILITIES.index(row["durability"]),
            QUEUE_KINDS.index(row["queueKind"]),
            row["repeat"],
            row["preloadedMessages"],
        ),
    )
    aggregate = {
        "schemaVersion": 1,
        "scenario": "backlog",
        "status": "SUCCESS",
        "revision": first_report["revision"],
        "generatedAt": max(generated_times).isoformat(),
        "measurementStartedAt": min(generated_times).isoformat(),
        "environment": first_report["environment"],
        "config": config,
        "results": results,
        "depthRatios": calculate_depth_ratios(results),
        "sampleFiles": [path.name for path in reports],
    }
    validate_full_report(aggregate)
    return aggregate


def build_baseline(report: dict, captured_at: str | None = None) -> dict:
    validate_full_report(report)
    references = {}
    for durability in DURABILITIES:
        for queue_kind in QUEUE_KINDS:
            for depth in DEPTHS:
                rows = samples_for(report, durability, queue_kind, depth)
                references[f"{durability}/{queue_kind}/{depth}"] = {
                    "publishMedianMessagesPerSecond": median_metric(
                        rows, "publishThroughputMessagesPerSecond"
                    ),
                    "settleMedianMessagesPerSecond": median_metric(
                        rows, "settleThroughputMessagesPerSecond"
                    ),
                    "deliveryLatencyP99MedianNanos": median_metric(rows, "deliveryLatencyP99Nanos"),
                }

    depth_ratios = calculate_depth_ratios(report["results"])
    for durability in DURABILITIES:
        for queue_kind in QUEUE_KINDS:
            ratios = [
                row["settleThroughputRatio"]
                for row in depth_ratios
                if row["durability"] == durability and row["queueKind"] == queue_kind
            ]
            if len(ratios) != len(REPEATS):
                raise ValueError(f"baseline is missing depth ratios for {durability}/{queue_kind}")
            median_ratio = statistics.median(ratios)
            if median_ratio < MINIMUM_SETTLE_DEPTH_RATIO:
                raise ValueError(
                    "baseline median settle depth ratio is below the 70% mapped floor: "
                    f"{durability}/{queue_kind}={median_ratio:.3f}"
                )

    return {
        "schemaVersion": 1,
        "designatedHost": DESIGNATED_HOST,
        "workDirFileSystem": DESIGNATED_FILESYSTEM,
        "capturedAt": captured_at or report["measurementStartedAt"],
        "sourceRevision": report["revision"],
        "config": report["config"],
        "limits": {
            "throughputMinimumFraction": THROUGHPUT_MINIMUM_FRACTION,
            "latencyP99MaximumFraction": LATENCY_MAXIMUM_FRACTION,
            "minimumSettleDepthRatio": MINIMUM_SETTLE_DEPTH_RATIO,
        },
        "references": references,
        "depthRatios": depth_ratios,
    }


def check_report(report: dict, baseline: dict, expected_revision: str) -> GateResult:
    failures = []
    passes = []
    try:
        validate_full_report(report)
    except (KeyError, TypeError, ValueError) as error:
        return GateResult([str(error)], [])
    if baseline.get("schemaVersion") != 1:
        failures.append("unsupported baseline schemaVersion")
    if baseline.get("designatedHost") != DESIGNATED_HOST:
        failures.append("baseline is not for the designated host")
    if baseline.get("workDirFileSystem") != DESIGNATED_FILESYSTEM:
        failures.append("baseline is not for the designated XFS filesystem")
    if report.get("config") != baseline.get("config"):
        failures.append("candidate report configuration does not match the baseline")
    revision = report.get("revision", "")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_revision):
        failures.append("expected revision must be a full 40-character Git SHA")
    elif revision != expected_revision:
        failures.append(f"report revision {revision} does not match expected {expected_revision}")

    by_key = index_samples(report.get("results", []), failures)
    references = baseline.get("references", {})
    limits = baseline.get("limits", {})
    for durability in DURABILITIES:
        for queue_kind in QUEUE_KINDS:
            for depth in DEPTHS:
                key = f"{durability}/{queue_kind}/{depth}"
                rows = by_key.get(key, [])
                if len(rows) != len(REPEATS):
                    failures.append(f"missing required sample(s) for {key}: expected {len(REPEATS)}, got {len(rows)}")
                    continue
                reference = references.get(key)
                if reference is None:
                    failures.append(f"baseline is missing required reference: {key}")
                    continue
                checks = (
                    (
                        "publish throughput",
                        median_metric(rows, "publishThroughputMessagesPerSecond"),
                        reference.get("publishMedianMessagesPerSecond"),
                        limits.get("throughputMinimumFraction", THROUGHPUT_MINIMUM_FRACTION),
                        "minimum",
                    ),
                    (
                        "settle throughput",
                        median_metric(rows, "settleThroughputMessagesPerSecond"),
                        reference.get("settleMedianMessagesPerSecond"),
                        limits.get("throughputMinimumFraction", THROUGHPUT_MINIMUM_FRACTION),
                        "minimum",
                    ),
                    (
                        "delivery p99 latency",
                        median_metric(rows, "deliveryLatencyP99Nanos"),
                        reference.get("deliveryLatencyP99MedianNanos"),
                        limits.get("latencyP99MaximumFraction", LATENCY_MAXIMUM_FRACTION),
                        "maximum",
                    ),
                )
                for name, candidate, reference_value, fraction, direction in checks:
                    if not is_positive_finite(reference_value):
                        failures.append(f"baseline has invalid {name} reference for {key}")
                        continue
                    if direction == "minimum" and candidate < reference_value * fraction:
                        failures.append(
                            f"{name} regressed for {key}: median {candidate:.3f}, "
                            f"minimum {reference_value * fraction:.3f}"
                        )
                    elif direction == "maximum" and candidate > reference_value * fraction:
                        failures.append(
                            f"{name} regressed for {key}: median {candidate:.3f}, "
                            f"maximum {reference_value * fraction:.3f}"
                        )
                    else:
                        passes.append(f"{name} {key}: {candidate:.3f}")

            ratios = []
            for repeat in REPEATS:
                empty = _find_sample(by_key, durability, queue_kind, 0, repeat)
                backlog = _find_sample(by_key, durability, queue_kind, BACKLOG_COUNT, repeat)
                if empty is not None and backlog is not None:
                    ratios.append(
                        backlog["settleThroughputMessagesPerSecond"]
                        / empty["settleThroughputMessagesPerSecond"]
                    )
            if len(ratios) == len(REPEATS):
                ratio = statistics.median(ratios)
                floor = limits.get("minimumSettleDepthRatio", MINIMUM_SETTLE_DEPTH_RATIO)
                if ratio < floor:
                    failures.append(
                        f"median settle depth ratio below {floor:.0%} for {durability}/{queue_kind}: {ratio:.1%}"
                    )
                else:
                    passes.append(f"median settle depth ratio {durability}/{queue_kind}: {ratio:.1%}")
    return GateResult(failures, passes)


def check_committed_report(report: dict, baseline: dict, current_revision: str, changed_paths: list[str]) -> GateResult:
    result = check_report(report, baseline, report.get("revision", ""))
    failures = list(result.failures)
    passes = list(result.passes)
    source_revision = report.get("revision", "")
    if not re.fullmatch(r"[0-9a-f]{40}", current_revision):
        failures.append("current revision must be a full 40-character Git SHA")
    elif not re.fullmatch(r"[0-9a-f]{40}", source_revision):
        failures.append("report source revision must be a full 40-character Git SHA")
    try:
        measured_at = parse_timestamp(report["measurementStartedAt"])
        age = datetime.now(timezone.utc) - measured_at.astimezone(timezone.utc)
        if age.total_seconds() < -300:
            failures.append("release report measurement time is in the future")
        elif age.total_seconds() > MAX_RELEASE_RESULT_AGE_DAYS * 24 * 60 * 60:
            failures.append(f"release report is older than {MAX_RELEASE_RESULT_AGE_DAYS} days")
    except (KeyError, ValueError) as error:
        failures.append(f"release report has an invalid measurement time: {error}")
    forbidden_paths = sorted(path for path in changed_paths if not result_path_allowed(path))
    if forbidden_paths:
        failures.append(
            "source files changed after the report was measured: " + ", ".join(forbidden_paths)
        )
    elif source_revision != current_revision:
        passes.append("current release tree contains only benchmark evidence changes after the measured source")
    return GateResult(failures, passes)


def result_path_allowed(path: str) -> bool:
    return path in ALLOWED_RESULT_PATHS or path.startswith("benchmark-results/initial/")


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def calculate_depth_ratios(results: list[dict]) -> list[dict]:
    by_key = {}
    for row in results:
        key = (row.get("durability"), row.get("queueKind"), row.get("repeat"), row.get("preloadedMessages"))
        by_key[key] = row
    ratios = []
    for durability in DURABILITIES:
        for queue_kind in QUEUE_KINDS:
            for repeat in REPEATS:
                empty = by_key.get((durability, queue_kind, repeat, 0))
                backlog = by_key.get((durability, queue_kind, repeat, BACKLOG_COUNT))
                if empty is None or backlog is None:
                    continue
                ratios.append(
                    {
                        "durability": durability,
                        "queueKind": queue_kind,
                        "repeat": repeat,
                        "backlogCount": BACKLOG_COUNT,
                        "publishThroughputRatio": backlog["publishThroughputMessagesPerSecond"]
                        / empty["publishThroughputMessagesPerSecond"],
                        "settleThroughputRatio": backlog["settleThroughputMessagesPerSecond"]
                        / empty["settleThroughputMessagesPerSecond"],
                        "deliveryLatencyP99Ratio": backlog["deliveryLatencyP99Nanos"]
                        / empty["deliveryLatencyP99Nanos"],
                    }
                )
    return ratios


def validate_full_report(report: dict) -> None:
    validate_single_report(report, None)
    parse_timestamp(report["generatedAt"])
    parse_timestamp(report["measurementStartedAt"])
    config = report["config"]
    expected = {
        "durabilities": DURABILITIES,
        "queueKinds": QUEUE_KINDS,
        "depths": DEPTHS,
        "backlogCount": BACKLOG_COUNT,
        "durationMillis": 10_000,
        "warmupMillis": 3_000,
        "repeats": len(REPEATS),
        "payloadBytes": 256,
        "batchSize": 100,
        "chunkSize": 100,
        "publishers": 1,
        "consumers": 1,
    }
    for key, value in expected.items():
        if config.get(key) != value:
            raise ValueError(f"required release benchmark config {key} must be {value!r}")
    failures = []
    index_samples(report.get("results", []), failures)
    if failures:
        raise ValueError("; ".join(failures))
    keys = index_samples(report["results"], [])
    required_keys = {
        f"{durability}/{queue_kind}/{depth}"
        for durability in DURABILITIES
        for queue_kind in QUEUE_KINDS
        for depth in DEPTHS
    }
    for key in sorted(required_keys):
        rows = keys.get(key, [])
        repeats = {row.get("repeat") for row in rows}
        if repeats != set(REPEATS) or len(rows) != len(REPEATS):
            raise ValueError(f"missing required sample(s) for {key}: expected repeats {REPEATS}, got {sorted(repeats)}")


def validate_single_report(report: dict, path: Path | None) -> None:
    label = str(path) if path else "report"
    if report.get("schemaVersion") != 1 or report.get("scenario") != "backlog":
        raise ValueError(f"{label}: unsupported backlog report schema")
    if report.get("status") != "SUCCESS":
        raise ValueError(f"{label}: report status is not SUCCESS")
    if not re.fullmatch(r"[0-9a-f]{40}", report.get("revision", "")):
        raise ValueError(f"{label}: report has no full source revision")
    if not isinstance(report.get("generatedAt"), str):
        raise ValueError(f"{label}: report has no generatedAt timestamp")
    environment = report.get("environment", {})
    if environment.get("host") != DESIGNATED_HOST:
        raise ValueError(f"{label}: report was not produced on the designated host {DESIGNATED_HOST}")
    if environment.get("workDirFileSystem", "").lower() != DESIGNATED_FILESYSTEM:
        raise ValueError(f"{label}: report work directory must be on {DESIGNATED_FILESYSTEM}")
    config = report.get("config")
    if not isinstance(config, dict):
        raise ValueError(f"{label}: report has no config")
    if not isinstance(report.get("results"), list):
        raise ValueError(f"{label}: report results must be an array")


def index_samples(results: list[dict], failures: list[str]) -> dict[str, list[dict]]:
    indexed = {}
    seen = set()
    for row in results:
        try:
            durability = row["durability"]
            queue_kind = row["queueKind"]
            depth = "EMPTY" if row["preloadedMessages"] == 0 else "BACKLOG"
            repeat = row["repeat"]
            if durability not in DURABILITIES or queue_kind not in QUEUE_KINDS or depth not in DEPTHS:
                raise ValueError("sample has an unsupported condition")
            if repeat not in REPEATS:
                raise ValueError("sample repeat must be 1, 2, or 3")
            key_tuple = (durability, queue_kind, depth, repeat)
            if key_tuple in seen:
                raise ValueError(f"duplicate sample for {key_tuple}")
            seen.add(key_tuple)
            for metric in (
                "publishThroughputMessagesPerSecond",
                "settleThroughputMessagesPerSecond",
                "deliveryLatencyP99Nanos",
            ):
                if not is_positive_finite(row.get(metric)):
                    raise ValueError(f"sample has invalid {metric}")
            key = f"{durability}/{queue_kind}/{depth}"
            indexed.setdefault(key, []).append(row)
        except (KeyError, TypeError, ValueError) as error:
            failures.append(f"invalid sample: {error}")
    return indexed


def samples_for(report: dict, durability: str, queue_kind: str, depth: str) -> list[dict]:
    target = 0 if depth == "EMPTY" else BACKLOG_COUNT
    return sorted(
        [
            row
            for row in report["results"]
            if row["durability"] == durability
            and row["queueKind"] == queue_kind
            and row["preloadedMessages"] == target
        ],
        key=lambda row: row["repeat"],
    )


def median_metric(rows: list[dict], metric: str) -> float:
    values = [float(row[metric]) for row in rows if is_positive_finite(row.get(metric))]
    if len(values) != len(rows):
        raise ValueError(f"missing or invalid metric {metric}")
    return statistics.median(values)


def _find_sample(indexed: dict, durability: str, queue_kind: str, depth: int, repeat: int):
    depth_name = "EMPTY" if depth == 0 else "BACKLOG"
    return next(
        (
            row
            for row in indexed.get(f"{durability}/{queue_kind}/{depth_name}", [])
            if row.get("repeat") == repeat
        ),
        None,
    )


def is_positive_finite(value) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def verify_current_host(report: dict, expected_revision: str | None = None) -> None:
    actual_host = subprocess.run(
        ["hostname", "-f"], check=True, capture_output=True, text=True
    ).stdout.strip().lower()
    if actual_host != DESIGNATED_HOST:
        raise ValueError(f"release benchmark must run on {DESIGNATED_HOST}; current host is {actual_host}")
    work_dir = Path(report["environment"].get("workDir", ""))
    if not work_dir.is_dir():
        raise ValueError(f"report work directory does not exist: {work_dir}")
    filesystem = subprocess.run(
        ["stat", "-f", "-c", "%T", str(work_dir)], check=True, capture_output=True, text=True
    ).stdout.strip().lower()
    if filesystem != DESIGNATED_FILESYSTEM:
        raise ValueError(f"release benchmark work directory must use XFS; current filesystem is {filesystem}")
    if report["environment"].get("workDirFileSystem", "").lower() != filesystem:
        raise ValueError("report filesystem does not match the current work directory filesystem")
    actual_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    if report.get("revision") != actual_revision:
        raise ValueError(f"report source revision {report.get('revision')} does not match checkout {actual_revision}")
    if expected_revision is not None and actual_revision != expected_revision:
        raise ValueError(f"checkout revision {actual_revision} does not match expected {expected_revision}")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    aggregate_parser = commands.add_parser("aggregate", help="aggregate isolated condition reports")
    aggregate_parser.add_argument("--report-dir", type=Path, required=True)
    aggregate_parser.add_argument("--output", type=Path, required=True)
    baseline_parser = commands.add_parser("baseline", help="derive baseline medians from a full report")
    baseline_parser.add_argument("--report", type=Path, required=True)
    baseline_parser.add_argument("--output", type=Path, required=True)
    check_parser = commands.add_parser("check", help="check a designated-host report against the baseline")
    check_parser.add_argument("--report", type=Path, required=True)
    check_parser.add_argument("--baseline", type=Path, required=True)
    check_parser.add_argument("--expected-revision", required=True)
    committed_parser = commands.add_parser(
        "check-committed", help="validate a fresh report committed with no later source changes"
    )
    committed_parser.add_argument("--report", type=Path, required=True)
    committed_parser.add_argument("--baseline", type=Path, required=True)
    committed_parser.add_argument("--current-revision", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "aggregate":
            report = aggregate_reports(args.report_dir)
            verify_current_host(report)
            write_json(args.output, report)
            print(f"aggregated {len(report['results'])} isolated samples into {args.output}")
            return 0
        report = load_json(args.report)
        if args.command == "baseline":
            verify_current_host(report)
            baseline = build_baseline(report)
            write_json(args.output, baseline)
            print(f"created baseline from {len(report['results'])} samples: {args.output}")
            return 0
        baseline = load_json(args.baseline)
        if args.command == "check-committed":
            head = subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
            if head != args.current_revision:
                raise ValueError(f"checkout revision {head} does not match expected {args.current_revision}")
            source = report.get("revision", "")
            subprocess.run(["git", "merge-base", "--is-ancestor", source, args.current_revision], check=True)
            changed = subprocess.run(
                ["git", "diff", "--name-only", f"{source}..{args.current_revision}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.splitlines()
            result = check_committed_report(report, baseline, args.current_revision, changed)
            for line in result.passes:
                print(f"PASS {line}")
            for line in result.failures:
                print(f"FAIL {line}", file=sys.stderr)
            print(f"committed release gate: {len(result.passes)} passed, {len(result.failures)} failed")
            return 1 if result.failures else 0
        verify_current_host(report, args.expected_revision)
        result = check_report(report, baseline, args.expected_revision)
        for line in result.passes:
            print(f"PASS {line}")
        for line in result.failures:
            print(f"FAIL {line}", file=sys.stderr)
        print(f"release gate: {len(result.passes)} passed, {len(result.failures)} failed")
        return 1 if result.failures else 0
    except (OSError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError) as error:
        print(f"release gate failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
