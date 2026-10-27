#!/usr/bin/env bash
# 외부 소비자 4개를 차례로 돌린다. 사용법: deploy-test/run-all.sh <smq 버전> [저장소 URL]
# 저장소 URL을 생략하면 mavenLocal을 쓴다. 하나라도 실패하면 비정상 종료한다.
set -euo pipefail

version="${1:?usage: run-all.sh <smq-version> [repo-url]}"
repo="${2:-mavenLocal}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"

gradle_args=("-PsmqVersion=$version")
maven_args=("-DsmqVersion=$version")
if [ "$repo" != "mavenLocal" ]; then
  gradle_args+=("-PrepoUrl=$repo")
  maven_args+=("-DrepoUrl=$repo")
fi

run_gradle() {
  echo "=== $1 ==="
  # 소비자는 smq 빌드와 분리돼 있어도 같은 Gradle 래퍼를 쓴다.
  "$root/gradlew" -p "$here/$1" run --no-daemon --console=plain "${gradle_args[@]}" | tee "$here/$1/output.log"
  tr -d "\r" < "$here/$1/output.log" | grep -qx "hello"
  tr -d "\r" < "$here/$1/output.log" | grep -qx "OK"
}

run_maven() {
  echo "=== $1 ==="
  mvn -B -ntp -f "$here/$1/pom.xml" compile exec:exec "${maven_args[@]}" | tee "$here/$1/output.log"
  tr -d "\r" < "$here/$1/output.log" | grep -qx "hello"
  tr -d "\r" < "$here/$1/output.log" | grep -qx "OK"
}

run_gradle gradle-classpath
run_gradle gradle-modulepath
run_maven maven-classpath
run_maven maven-modulepath
echo "배포 테스트 소비자 4개 통과: smq $version ($repo)"
