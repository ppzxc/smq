#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"
REVISION=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain)" ]; then
    echo "release benchmark requires a clean checkout at $REVISION" >&2
    exit 1
fi

REPORT_DIR=${SMQ_RELEASE_REPORT_DIR:-"build/release-benchmark/$REVISION"}
if [[ "$REPORT_DIR" != /* ]]; then
    REPORT_DIR="$ROOT_DIR/$REPORT_DIR"
fi
BASELINE=${SMQ_RELEASE_BASELINE:-"$ROOT_DIR/smoke-harness/release-baseline.json"}

"$ROOT_DIR/smoke-harness/run-backlog-suite.sh" "$REPORT_DIR"
python3 "$ROOT_DIR/smoke-harness/release_gate.py" check \
    --report "$REPORT_DIR/backlog-report.json" \
    --baseline "$BASELINE" \
    --expected-revision "$REVISION"
echo "release report: $REPORT_DIR/backlog-report.json"
