#!/usr/bin/env bash
# 저장소에서 smq 아티팩트가 보일 때까지 기다린다. 사용법: deploy-test/wait-for-artifact.sh <버전> <저장소 URL>
# Central은 공개 뒤 동기화가 늦을 수 있어서, 해석 실패(404 등)는 상한까지 재시도한다.
# 상한을 넘으면 비정상 종료한다. 소비자 실행은 이 단계 뒤에 한 번만 하므로,
# 아티팩트가 보인 뒤의 실패는 동기화 지연이 아니라 실제 결함으로 바로 드러난다.
# 환경 변수: SMQ_WAIT_LIMIT_SECONDS(기본 7200, 약 2시간), SMQ_WAIT_INTERVAL_SECONDS(기본 60),
#           SMQ_REPO_TOKEN(선택, Bearer 토큰)
set -euo pipefail

version="${1:?usage: wait-for-artifact.sh <smq-version> <repo-url>}"
repo="${2:?usage: wait-for-artifact.sh <smq-version> <repo-url>}"
limit="${SMQ_WAIT_LIMIT_SECONDS:-7200}"
interval="${SMQ_WAIT_INTERVAL_SECONDS:-60}"
base="${repo%/}/io/github/ppzxc/smq/$version"

curl_args=(-sS -o /dev/null --max-time 60 -w "%{http_code}")
if [ -n "${SMQ_REPO_TOKEN:-}" ]; then
  curl_args+=(-H "Authorization: Bearer $SMQ_REPO_TOKEN")
fi

# 소비자가 읽는 세 파일이 모두 보여야 한다. 일부만 동기화된 상태에서 소비자를 돌리지 않기 위해서다.
visible() {
  local f code
  for f in "smq-$version.pom" "smq-$version.module" "smq-$version.jar"; do
    code=$(curl "${curl_args[@]}" "$base/$f" 2>/dev/null || true)
    case "$code" in
      200) ;;
      401|403) echo "::error::인증 또는 권한 오류($code): $base/$f. 동기화 지연이 아니므로 재시도하지 않는다." >&2; exit 2 ;;
      *) return 1 ;;  # 404, 5xx, 네트워크 오류는 동기화 지연으로 보고 재시도한다
    esac
  done
}

start=$SECONDS
attempt=0
while true; do
  attempt=$((attempt + 1))
  if visible; then
    echo "아티팩트 확인: smq $version ($repo), 시도 $attempt회, $((SECONDS - start))초"
    exit 0
  fi
  elapsed=$((SECONDS - start))
  if [ "$elapsed" -ge "$limit" ]; then
    echo "::error::${limit}초 안에 해석되지 않았다: smq $version ($repo). 동기화 지연 상한을 넘었으니 Central 상태를 확인한다." >&2
    exit 1
  fi
  echo "아직 해석되지 않음(시도 $attempt회, ${elapsed}초 경과). ${interval}초 뒤 재시도한다."
  sleep "$interval"
done
