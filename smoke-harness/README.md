# smq 공개 API 스모크 하네스

하네스는 별도 Gradle 빌드다. smq 배포 JAR과 SQLite JDBC만 모듈 경로에 올리고, `io.github.ppzxc.smq` 모듈이 내보낸 공개 API만 호출한다. 브로커 서버는 필요하지 않다.

저장소 루트에서 기본 실행은 두 내구성 수준과 단건·배치 발행을 모두 실행한다. 루트 JAR 빌드도 함께 한다.

```sh
./gradlew smokeHarness
```

기본값은 측정 반복마다 1,000건, 워밍업 100건, 반복 3회, payload 256B, 배치 최대 100건, 미정산 최대 1,000건, 꺼내기 청크 최대 100건이다. `POWER_LOSS`의 단건 발행은 저장 장치의 fsync 속도에 따라 오래 걸릴 수 있다. 측정 크기를 바꾸려면 다음처럼 넘긴다.

```sh
./gradlew smokeHarness -Psmoke.args="--count 100 --warmup 10 --repeat 2 --payload-bytes 256 --batch-size 16 --max-in-flight 64 --chunk-size 16"
```

보고서는 `smoke-harness/build/smoke-harness/report.json`에, SQLite 파일은 `smoke-harness/build/smoke-harness/work/`에 생긴다. 보고서에는 설정을 그대로 다시 넘길 수 있는 `reproduction.args`가 포함된다. 내구성이나 모드 하나만 실행하려면 `--durability PROCESS_CRASH` 또는 `--mode batch`를 추가한다.

별도 빌드만 실행할 수도 있다. 이 방법은 먼저 smq JAR을 만들어야 한다.

```sh
./gradlew jar
./gradlew -p smoke-harness run --args="--count 100 --warmup 10 --repeat 1"
```

프로세스 강제 종료 뒤 복구를 짧게 검증하려면 다음 명령을 실행한다. 외부 제어 프로세스가 대상 JVM을 종료한 뒤 공개 API로 같은 SQLite 저장소를 다시 연다. 브로커는 필요하지 않다.

```sh
./gradlew smokeHarness -Psmoke.args="recovery --durability BOTH --message-count 8 --lease-ms 150"
```

결과는 `smoke-harness/build/smoke-harness/recovery-report.json`에 기록된다. `--negative-control drop-confirmed-publish`를 추가하면 대상 JVM 종료 후 격리 저장소 파일을 제거해, 확인된 발행 유실을 하네스가 실패로 잡는지 확인한다.

루트의 `./gradlew check`는 smq JAR/소스 JAR에서 하네스와 그 의존성이 빠졌는지 검사하고 하네스 CLI 테스트도 실행한다. 이 테스트는 별도 JVM에서 배포 JAR을 이름 있는 JPMS 모듈로 실행한다.
