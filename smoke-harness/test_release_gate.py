import unittest
import json
import tempfile
from pathlib import Path
from datetime import datetime, timedelta, timezone

import release_gate


REVISION = "a" * 40


class ReleaseGateTest(unittest.TestCase):
    def test_baseline_uses_repeated_samples_and_gate_accepts_matching_results(self):
        report = make_report()

        baseline = release_gate.build_baseline(report, captured_at="2026-11-28T00:00:00Z")
        result = release_gate.check_report(report, baseline, expected_revision=REVISION)

        self.assertEqual([], result.failures)
        reference = baseline["references"]["PROCESS_CRASH/DEFAULT/EMPTY"]
        self.assertEqual(10_200.0, reference["publishMedianMessagesPerSecond"])

    def test_gate_rejects_missing_required_repeat(self):
        report = make_report()
        del report["results"][-1]
        baseline = release_gate.build_baseline(make_report(), captured_at="2026-11-28T00:00:00Z")

        result = release_gate.check_report(report, baseline, expected_revision=REVISION)

        self.assertTrue(any("missing required sample" in failure for failure in result.failures))

    def test_gate_rejects_throughput_below_baseline_tolerance(self):
        report = make_report()
        baseline = release_gate.build_baseline(report, captured_at="2026-11-28T00:00:00Z")
        for sample in report["results"]:
            if (
                sample["durability"] == "PROCESS_CRASH"
                and sample["queueKind"] == "DEFAULT"
                and sample["preloadedMessages"] == 0
            ):
                sample["publishThroughputMessagesPerSecond"] = 1.0

        result = release_gate.check_report(report, baseline, expected_revision=REVISION)

        self.assertTrue(any("publish throughput regressed" in failure for failure in result.failures))

    def test_baseline_rejects_non_designated_host(self):
        report = make_report()
        report["environment"]["host"] = "other-host"

        with self.assertRaisesRegex(ValueError, "designated host"):
            release_gate.build_baseline(report, captured_at="2026-11-28T00:00:00Z")

    def test_aggregator_combines_isolated_process_reports_and_requires_complete_coverage(self):
        with tempfile.TemporaryDirectory() as temporary:
            report_dir = Path(temporary)
            write_isolated_reports(report_dir)

            aggregate = release_gate.aggregate_reports(report_dir)

            self.assertEqual(48, len(aggregate["results"]))
            self.assertEqual(24, len(aggregate["depthRatios"]))
            self.assertEqual([1, 2, 3], sorted({row["repeat"] for row in aggregate["results"]}))
            (report_dir / "round-3-POWER_LOSS-LEASED_HEAD-BACKLOG.json").unlink()
            with self.assertRaisesRegex(ValueError, "missing required sample"):
                release_gate.aggregate_reports(report_dir)

    def test_committed_gate_rejects_source_changes_after_measurement(self):
        report = make_report()
        now = datetime.now(timezone.utc).isoformat()
        report["generatedAt"] = now
        report["measurementStartedAt"] = now
        baseline = release_gate.build_baseline(report)

        result = release_gate.check_committed_report(
            report, baseline, "b" * 40, ["src/main/java/io/github/ppzxc/smq/Smq.java"]
        )

        self.assertTrue(any("source files changed after the report was measured" in failure for failure in result.failures))

    def test_committed_gate_rejects_stale_measurement(self):
        report = make_report()
        stale = (datetime.now(timezone.utc) - timedelta(days=16)).isoformat()
        report["generatedAt"] = stale
        report["measurementStartedAt"] = stale
        baseline = release_gate.build_baseline(report)

        result = release_gate.check_committed_report(report, baseline, REVISION, [])

        self.assertTrue(any("release report is older" in failure for failure in result.failures))


def make_report():
    queue_kinds = ["DEFAULT", "MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD"]
    durabilities = ["PROCESS_CRASH", "POWER_LOSS"]
    report = {
        "schemaVersion": 1,
        "scenario": "backlog",
        "status": "SUCCESS",
        "revision": REVISION,
        "generatedAt": "2026-11-28T00:00:00+00:00",
        "measurementStartedAt": "2026-11-28T00:00:00+00:00",
        "environment": {"host": "ns0270.nanoit.kr", "workDirFileSystem": "xfs"},
        "config": {
            "durabilities": durabilities,
            "queueKinds": queue_kinds,
            "depths": ["EMPTY", "BACKLOG"],
            "backlogCount": 1_000_000,
            "durationMillis": 10_000,
            "warmupMillis": 3_000,
            "repeats": 3,
            "payloadBytes": 256,
            "batchSize": 100,
            "chunkSize": 100,
            "publishers": 1,
            "consumers": 1,
        },
        "results": [],
    }
    for durability in durabilities:
        for queue_kind in queue_kinds:
            for repeat in range(1, 4):
                for backlog_count in (0, 1_000_000):
                    multiplier = 0.9 if backlog_count else 1.0
                    report["results"].append(
                        {
                            "durability": durability,
                            "queueKind": queue_kind,
                            "preloadedMessages": backlog_count,
                            "repeat": repeat,
                            "publishThroughputMessagesPerSecond": (10_000 + repeat * 100) * multiplier,
                            "settleThroughputMessagesPerSecond": (9_000 + repeat * 100) * multiplier,
                            "deliveryLatencyP99Nanos": 8_000_000 + repeat * 100_000,
                        }
                    )
    return report


def write_isolated_reports(report_dir):
    aggregate = make_report()
    for row in aggregate["results"]:
        repeat = row["repeat"]
        durability = row["durability"]
        queue_kind = row["queueKind"]
        depth = "EMPTY" if row["preloadedMessages"] == 0 else "BACKLOG"
        config = dict(aggregate["config"])
        config.update(
            {
                "durabilities": [durability],
                "queueKinds": [queue_kind],
                "depths": [depth],
                "repeats": 1,
            }
        )
        sample = dict(row)
        sample["repeat"] = 1
        report = dict(aggregate)
        report["config"] = config
        report["results"] = [sample]
        report["depthRatios"] = []
        path = report_dir / f"round-{repeat}-{durability}-{queue_kind}-{depth}.json"
        path.write_text(json.dumps(report), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
