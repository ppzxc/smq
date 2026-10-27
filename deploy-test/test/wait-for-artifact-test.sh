#!/usr/bin/env bash
# wait-for-artifact.sh 검증. 로컬 HTTP 서버로 동기화 지연, 상한 초과, Bearer 헤더를 흉내 낸다.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/../wait-for-artifact.sh"
work="$(mktemp -d)"
trap 'kill "${server:-}" 2>/dev/null || true; rm -rf "$work"' EXIT
version=9.9.9
dir="$work/io/github/ppzxc/smq/$version"
mkdir -p "$dir"

# $1=포트 파일, $2=요구 토큰(없으면 빈 문자열). 파일이 생기기 전에는 404를 돌려준다.
cat > "$work/server.py" <<'PY'
import http.server, os, sys
root, token = sys.argv[1], sys.argv[2]
class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k): super().__init__(*a, directory=root, **k)
    def do_GET(self):
        if token and self.headers.get("Authorization") != "Bearer " + token:
            self.send_error(401); return
        super().do_GET()
    def log_message(self, *a): pass
s = http.server.HTTPServer(("127.0.0.1", 0), H)
open(os.path.join(root, "port"), "w").write(str(s.server_port))
s.serve_forever()
PY
start_server() {
  rm -f "$work/port"
  python3 -I "$work/server.py" "$work" "$1" & server=$!
  for _ in $(seq 50); do [ -s "$work/port" ] && break; sleep 0.1; done
  url="http://127.0.0.1:$(cat "$work/port")"
}
make_files() { for e in pom module jar; do echo x > "$dir/smq-$version.$e"; done; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# 1. 처음엔 없고 나중에 생기면 재시도 끝에 성공한다.
start_server ""
( sleep 3; make_files ) &
SMQ_WAIT_LIMIT_SECONDS=30 SMQ_WAIT_INTERVAL_SECONDS=1 "$script" "$version" "$url" >/dev/null || fail "지연 뒤 성공해야 한다"
echo "ok: 동기화 지연 뒤 성공"

# 2. 일부 파일만 있으면 기다린다(상한 초과로 실패).
rm "$dir/smq-$version.jar"
if SMQ_WAIT_LIMIT_SECONDS=2 SMQ_WAIT_INTERVAL_SECONDS=1 "$script" "$version" "$url" >/dev/null 2>&1; then fail "일부 파일만 있으면 실패해야 한다"; fi
echo "ok: 일부만 보이면 상한 뒤 실패"

# 3. 상한을 넘으면 비정상 종료한다.
rm -rf "$dir"/*
start=$SECONDS
if SMQ_WAIT_LIMIT_SECONDS=2 SMQ_WAIT_INTERVAL_SECONDS=1 "$script" "$version" "$url" >/dev/null 2>&1; then fail "상한 초과 시 실패해야 한다"; fi
[ $((SECONDS - start)) -lt 10 ] || fail "상한을 지키지 않았다"
echo "ok: 상한 초과 시 실패"

# 4. Bearer 토큰이 필요한 서버에서 토큰이 있으면 성공하고 없으면 실패한다.
kill "$server"; make_files; start_server secret
SMQ_REPO_TOKEN=secret SMQ_WAIT_LIMIT_SECONDS=5 SMQ_WAIT_INTERVAL_SECONDS=1 "$script" "$version" "$url" >/dev/null || fail "토큰이 있으면 성공해야 한다"
if SMQ_WAIT_LIMIT_SECONDS=2 SMQ_WAIT_INTERVAL_SECONDS=1 "$script" "$version" "$url" >/dev/null 2>&1; then fail "토큰이 없으면 실패해야 한다"; fi
echo "ok: Bearer 토큰"
