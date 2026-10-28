# 릴리스 런북

`io.github.ppzxc:smq`를 Maven Central에 공개하는 절차다. 사람이 이 문서를 따라 한다. 예시는 `0.0.1`이다. 다음 버전을 낼 때는 버전 번호만 바꾼다.

공개 워크플로(`.github/workflows/release.yml`)는 업로드와 스테이징 검증까지만 하고, **Publish는 사람이 Portal에서 직접 누른다.** Publish는 되돌릴 수 없다.

## 한눈에 보기

1. 릴리스 PR을 만든다(버전, CHANGELOG, README 상태 문구). PR head에서 수동 게이트를 돌려 결과를 PR 본문에 붙인다.
2. PR을 main에 병합하고, 병합 커밋의 CI가 녹색이 될 때까지 기다린다.
3. 공개 워크플로(`Release`)를 실행한다. CI 조회, 기존 배포 확인, 업로드, VALIDATED 대기, 배포 테스트 (b)까지 돈다.
4. Portal에서 **Publish**를 누른다.
5. 배포 테스트 워크플로를 `repository=central`로 실행해 (c)를 확인한다.
6. 릴리스 커밋에 태그와 GitHub Release를 만든다.
7. `0.0.2-SNAPSHOT`으로 올리는 PR을 낸다.

## 릴리스 게이트

공개 워크플로를 실행하기 전에 아래를 모두 통과해야 한다.

| 항목 | 강제 방식 |
|---|---|
| Linux·Windows `check` | CI. 공개 워크플로가 조회 |
| 배포 테스트 (a), Linux·Windows | CI. 공개 워크플로가 조회 |
| `randomBenchSmoke` | CI. 공개 워크플로가 조회 |
| ns0270 기준선 벤치 PASS | 릴리스 PR head에서 수동. PR 본문에 기록 |
| 전체 `randomBench` 불변식 위반 0 | 릴리스 PR head에서 수동. PR 본문에 기록. 위반은 공개 전에 반드시 고친다 |
| 배포 테스트 (b) | 공개 워크플로 안. 실패하면 Publish하지 않는다 |

조회하는 CI는 릴리스 커밋(main에서 실행한 SHA)의 main push 실행이다. `Linux` 워크플로와 `Windows` 워크플로 각각에서 `check`와 `deploy-test` 잡이 `success`여야 한다. `randomBenchSmoke`는 `Linux`의 `check` 잡 안의 단계라서 그 잡이 녹색이면 통과한 것이다. 배포 테스트 (a), (b), (c)는 같은 하네스(`deploy-test/run-all.sh`)를 쓰고 저장소 URL만 다르다. (a)는 `publishToMavenLocal`, (b)는 Portal 스테이징, (c)는 공개 후 Central이다.

## 1. 릴리스 PR

릴리스 PR 하나에서 아래를 한꺼번에 바꾼다.

### 바꿀 지점

| 파일 | 바꾸는 곳 | 바꾸기 전 | 바꾼 뒤 |
|---|---|---|---|
| `build.gradle.kts` | `version` | `0.0.1-SNAPSHOT` | `0.0.1` |
| `CHANGELOG.md` | `[Unreleased]` 절 이름과 맨 아래 링크 | `## [Unreleased]` | `## [0.0.1] - YYYY-MM-DD`. 위에 빈 `## [Unreleased]` 절을 새로 둔다. 맨 아래 링크는 `[Unreleased]: .../compare/0.0.1...HEAD`, `[0.0.1]: .../releases/tag/0.0.1`로 바꾼다 |
| `README.md` | 맨 위 `> **상태**` 문구 | "곧 Maven Central에 `0.0.1`로 공개할 예정이다. 0.x 동안은 API 호환을 약속하지 않는다." | "Maven Central에 `0.0.1` 공개. 0.x 동안은 API 호환을 약속하지 않는다." |
| `README.md` | 빠른 시작의 "아직 공개 전이라면 ..." 문장 | 있음 | 지운다 |

공개 워크플로는 입력 버전이 `build.gradle.kts`의 `version`과 다르면 실패한다. 버전은 빌드 인자로 주입하지 않고 빌드 스크립트에 하드코딩한다.

### 수동 게이트 실행

PR head를 체크아웃해서 돌린다. 다른 부하가 없는 베어메탈 Linux(ns0270 또는 이를 대체한 장비)에서 한다.

```bash
./gradlew benchmark      # ns0270 기준선과 비교. FAIL이 하나라도 있으면 비정상 종료한다. 비율 검사는 3회 측정의 중앙값으로 판정한다
./gradlew randomBench    # 전체 무작위 테스트. 첫 위반에서 멈추고 비정상 종료한다
```

`benchmark`는 비율 검사(scaling, depth)의 S1 조건 약 20개를 기본 3회 재서 하네스가 중앙값으로 판정하므로, 비율 항목이 FAIL이면 손으로 다시 재지 말고 그대로 막는다. 소요 시간은 단발일 때의 약 1.5~2배다(조건 38개에 비율 조건 20개가 두 번 더 돈다). PR 본문에는 `scaling`, `depth` 줄의 바퀴별 값과 중앙값을 그대로 옮긴다. 시간이 부족한 개발 환경에서는 `-Pbenchmark.args="--repeat 1"`로 줄일 수 있지만 게이트에서는 기본값을 쓴다.

벤치 절차와 비교 기준은 [docs/benchmark.md](benchmark.md), 기준선은 [benchmark/README.md](../benchmark/README.md)를 본다. `randomBench`의 옵션(`--seed`, `--budget` 등)과 결과 위치도 docs/benchmark.md에 있다. 위반이 나오면 실패한 실행의 시드로 재현해 고치고, 공개는 미룬다.

### PR 본문 양식

```markdown
## 수동 게이트 결과 (PR head <커밋 SHA>)

- 장비: <호스트, OS, 파일시스템, 컨트롤러 캐시 상태>
- ns0270 기준선 벤치: PASS (`./gradlew benchmark`, 합계: 통과 <n>, 실패 0, 건너뜀 <n>)
- 전체 `randomBench`: 불변식 위반 0 (마스터 시드 <값>, 실행 <n>회, 예산 <시간>)

## 바꾼 곳

- [ ] `build.gradle.kts` version 0.0.1
- [ ] `CHANGELOG.md` `[0.0.1]` 절, 새 `[Unreleased]` 절
- [ ] `README.md` 상태 문구
```

## 2. 병합과 CI 확인

PR을 main에 병합한다. 병합 커밋이 **릴리스 커밋**이다. 그 커밋의 `Linux`와 `Windows` 워크플로(main push)가 끝나 녹색인지 기다린다. 공개 워크플로가 이 결과를 조회하므로 끝나기 전에 실행하면 실패한다.

## 3. 공개 워크플로 실행

GitHub Actions에서 `Release` 워크플로를 main에서 실행하고 `version`에 `0.0.1`을 넣는다. Environment `release`를 쓰고, main에서만 돈다(deployment branch 규칙).

```bash
gh workflow run release.yml --ref main -f version=0.0.1
```

단계는 다음과 같다.

1. `ci-status`: 릴리스 커밋의 CI 상태를 조회한다. 녹색이 아니면 실패한다.
2. `upload`: 입력 버전과 빌드 버전을 맞춰 보고, 같은 버전의 기존 배포를 확인한 뒤, `publishToMavenCentral`로 USER_MANAGED 업로드를 하고 VALIDATED를 기다린다(최대 30분).
3. `staging-test`: 배포 테스트 (b). Portal 스테이징에서 소비자 4개(Gradle과 Maven, 클래스패스와 모듈 경로)를 Linux와 Windows에서 돌린다.
4. `announce`: Publish를 안내하는 요약을 남기고 끝난다.

### 기존 배포 조회의 한계

Portal API에는 이름과 버전으로 배포를 찾는 목록 조회가 없다. 그래서 워크플로는 배포 상태 대신 POM 파일이 보이는지로 판단한다.

- Maven Central에 `smq-<버전>.pom`이 있으면 이미 PUBLISHED로 보고 실패한다. 버전을 다음 패치로 올린다.
- Portal 다운로드 경로(Bearer 인증)에 `smq-<버전>.pom`이 있으면 VALIDATED 배포가 남아 있다고 보고 실패한다. Portal에서 그 배포를 drop한 뒤 다시 실행한다.
- 그 외에는 기존 배포가 없다고 본다. 검증이 실패한 배포(FAILED)나 아직 검증 중인 배포는 POM이 보이지 않아 이 조회에 걸리지 않는다. Portal 배포 목록을 직접 확인한다.

### Maven 대비책은 적용하지 않았다

Maven `settings.xml`의 `httpHeaders`로 Bearer 헤더를 붙일 수 있는지가 스펙의 위험이었다. #96에서 가능함을 확인했다. 그래서 (b)에서 Gradle 소비자 2개만 돌리는 대비책은 적용하지 않았고, (b)는 소비자 4개를 모두 돌린다.

## 4. Portal Publish

(b)가 통과하면 `announce` 요약의 안내대로 한다.

1. <https://central.sonatype.com/publishing/deployments>에서 smq `0.0.1` 배포(VALIDATED)를 연다.
2. **Publish**를 누른다. 되돌릴 수 없다.

Central에 반영되기까지 시간이 걸린다.

## 5. 배포 테스트 (c)

Publish한 뒤 `Deploy test` 워크플로를 `repository=central`로 실행한다. 시크릿 없이 Central에서 해석될 때까지 재시도하고, 상한은 약 2시간이다. 해석된 뒤의 소비자 실행은 재시도하지 않는다.

```bash
gh workflow run deploy-test.yml --ref main -f version=0.0.1 -f repository=central
```

## 6. 태그와 GitHub Release

태그는 `v` 없이 버전 그대로(`0.0.1`) 쓴다. Publish하고 (c)를 확인한 뒤 **릴리스 커밋**에 건다. `--target`에는 2단계의 병합 커밋 SHA를 준다. 이 시점의 main HEAD가 아닐 수 있으니 SHA를 직접 적는다.

```bash
# CHANGELOG.md의 [0.0.1] 절만 잘라 노트 파일로 만든다
awk '/^## \[0\.0\.1\]/{f=1;next} /^## \[/{f=0} f' CHANGELOG.md > /tmp/notes-0.0.1.md
gh release create 0.0.1 --target <릴리스 커밋 SHA> --title 0.0.1 --notes-file /tmp/notes-0.0.1.md
```

공개는 됐는데 (c)가 실패해도 태그와 Release는 만들고, 문제는 이슈로 적는다.

## 7. 다음 SNAPSHOT으로 올리기

공개 후 `build.gradle.kts`의 `version`을 `0.0.2-SNAPSHOT`으로 올리는 PR을 낸다. main은 항상 "다음 패치-SNAPSHOT"이다. 이 PR은 6단계 뒤에 병합해도 되지만, 공개 워크플로를 다시 돌릴 일이 있으면 그 전에는 병합하지 않는다(버전 확인 단계가 실패한다). 0.0.x 동안은 무엇을 바꾸든 패치 번호만 올리고, 정해진 릴리스 주기는 없다.

## 문제가 생겼을 때

### 스테이징 실패: drop하고 다시 올린다

(b)나 업로드 검증이 실패하면 Publish하지 않는다. Portal 배포 목록에서 그 배포를 drop하고, 원인을 고친 뒤 공개 워크플로를 다시 실행한다. 소스를 고쳤다면 릴리스 PR을 새로 내서 릴리스 커밋부터 다시 정한다. drop하지 않고 다시 실행하면 기존 배포 조회 단계가 실패한다.

### 공개 후 문제: 되돌리지 않는다

Central에 PUBLISHED된 버전은 지우거나 덮어쓰지 않는다. 문제는 GitHub 이슈로 적고 다음 패치(`0.0.2`)에서 고친다.

### 로컬 예비 경로

CI의 업로드가 막히면 로컬에서 직접 올릴 수 있다. WSL의 `~/.gradle/gradle.properties`에 `mavenCentralUsername`, `mavenCentralPassword`, `signing.keyId`, `signing.password`, `signing.secretKeyRingFile`(`~/.gnupg/secring-smq.gpg`)이 있어야 한다. 릴리스 커밋을 체크아웃한 상태에서 한다.

```bash
./gradlew publishToMavenCentral
```

이것도 USER_MANAGED라서 Portal에서 Publish를 눌러야 공개된다. 업로드 후에는 배포 테스트 (b)를 `Deploy test` 워크플로로 직접 실행하고(`repository=staging`), 3~6단계의 나머지를 이어서 한다. 서명 키는 RSA 4096 `710BF4DF`이고 keyserver.ubuntu.com에 공개되어 있다.

### 토큰이나 키가 유출됐을 때

Portal 사용자 토큰이 유출되면 Portal(<https://central.sonatype.com/account>)에서 토큰을 폐기하고 다시 발급한다. 그리고 아래 두 곳을 새 값으로 바꾼다.

- GitHub Environment `release`의 시크릿 `ORG_GRADLE_PROJECT_MAVENCENTRALUSERNAME`, `ORG_GRADLE_PROJECT_MAVENCENTRALPASSWORD`
- 로컬 `~/.gradle/gradle.properties`

Portal 토큰은 만료가 없어서 폐기하지 않으면 계속 유효하다. 서명 키가 유출되면 키를 폐기(revocation)하고 새 키를 만들어 keyserver에 올린 뒤 시크릿 `ORG_GRADLE_PROJECT_SIGNINGINMEMORYKEY`, `ORG_GRADLE_PROJECT_SIGNINGINMEMORYKEYPASSWORD`를 바꾼다. 이미 공개된 버전의 서명은 바꿀 수 없다.

## 참고

- 워크플로: `.github/workflows/release.yml`(Release), `.github/workflows/deploy-test.yml`(Deploy test), `linux.yml`, `windows.yml`
- 배포 테스트 하네스: `deploy-test/run-all.sh`
- 벤치마크: [docs/benchmark.md](benchmark.md)
