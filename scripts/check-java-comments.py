#!/usr/bin/env python3
"""Reject production implementation comments absent from the checked-in baseline."""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "config/java-comment-baseline.json"


def comments(source: str) -> list[str]:
    source = re.sub(r"/\*\*.*?\*/", "", source, flags=re.S)
    return re.findall(r"//[^\r\n]*|/\*.*?\*/", source, flags=re.S)


def violations(source: str, approved: list[str]) -> list[str]:
    remaining = list(approved)
    rejected = []
    source_without_javadocs = re.sub(r"/\*\*.*?\*/", "", source, flags=re.S)
    for match in re.finditer(r"//[^\r\n]*|/\*.*?\*/", source_without_javadocs, flags=re.S):
        comment = match.group()
        preceding = source_without_javadocs[: match.start()].splitlines()
        if comment.startswith("//") and preceding and "@SuppressWarnings" in preceding[-1] and comment[2:].strip():
            continue
        if comment in remaining:
            remaining.remove(comment)
        else:
            rejected.append(comment)
    return rejected


def main() -> int:
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    violations = []
    for path in sorted((ROOT / "src/main/java").rglob("*.java")):
        relative = path.relative_to(ROOT).as_posix()
        approved = baseline.get(relative, [])
        for comment in violations_for_file(path, approved):
            violations.append(f"{relative}: new implementation comment: {comment.strip()}")
    if violations:
        print("\n".join(violations), file=sys.stderr)
        return 1
    return 0


def violations_for_file(path: Path, approved: list[str]) -> list[str]:
    return violations(path.read_text(encoding="utf-8"), approved)


if __name__ == "__main__":
    raise SystemExit(main())
