#!/usr/bin/env bash
set -euo pipefail

gitleaks_bin=${GITLEAKS:-gitleaks}
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

git -C "$tmp_dir" init -q
git -C "$tmp_dir" config core.autocrlf false
git -C "$tmp_dir" config user.name 'Secret scan fixture'
git -C "$tmp_dir" config user.email 'secret-scan-fixture@example.invalid'
printf 'safe baseline\n' > "$tmp_dir/README.md"
git -C "$tmp_dir" add README.md
git -C "$tmp_dir" commit -qm 'baseline'
base_sha=$(git -C "$tmp_dir" rev-parse HEAD)

# This is a fabricated GitHub token-shaped fixture; it is not a credential.
fake_key="ghp_$(printf '%s' 01234567890123456789 01234567890123456789)"
printf 'GITHUB_TOKEN=%s\n' "$fake_key" > "$tmp_dir/fixture.env"
git -C "$tmp_dir" add fixture.env
git -C "$tmp_dir" commit -qm 'add synthetic credential fixture'
head_sha=$(git -C "$tmp_dir" rev-parse HEAD)

set +e
scan_output=$(cd "$tmp_dir" && "$gitleaks_bin" git \
  --log-opts="$base_sha..$head_sha" \
  --redact=100 \
  --verbose \
  --no-banner 2>&1)
scan_status=$?
set -e

if [[ $scan_status -eq 0 ]]; then
  echo 'Secret scan fixture was not detected.' >&2
  exit 1
fi
if [[ "$scan_output" == *"$fake_key"* ]]; then
  echo 'Secret scan output exposed the synthetic secret value.' >&2
  exit 1
fi
if [[ "$scan_output" != *'fixture.env'* ]]; then
  echo 'Secret scan output did not identify the finding location.' >&2
  exit 1
fi

echo 'Secret scan detection, location reporting, and value redaction passed.'
