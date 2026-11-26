#!/usr/bin/env bash
set -euo pipefail

base_sha=${1:?usage: secret-scan.sh BASE_SHA HEAD_SHA}
head_sha=${2:?usage: secret-scan.sh BASE_SHA HEAD_SHA}
gitleaks_bin=${GITLEAKS:-gitleaks}

for commit in "$base_sha" "$head_sha"; do
  if ! git cat-file -e "${commit}^{commit}" 2>/dev/null; then
    echo "Secret scan cannot find required commit: $commit" >&2
    exit 2
  fi
done

# Scan the PR's commit patches. Full redaction keeps detected values out of logs.
"$gitleaks_bin" git \
  --log-opts="$base_sha..$head_sha" \
  --redact=100 \
  --verbose \
  --no-banner
