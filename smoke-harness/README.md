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

## RabbitMQ 비교 (선택 실행)

RabbitMQ 비교기는 smq API가 아니라 공식 RabbitMQ Java 클라이언트로 직접 메시지를 발행·가져온다. RabbitMQ Server 이미지는 `rabbitmq:4.4.1-management`, 클라이언트는 `com.rabbitmq:amqp-client:5.36.0`으로 고정한다. 클래식 큐와 단일 멤버 quorum 큐 모두 durable이며, 메시지는 persistent (`deliveryMode=2`)로 발행한다. quorum 큐는 `x-quorum-initial-group-size=1`로 한 멤버를 지정한다.

선택적인 Docker 통합 실행은 루트에서 다음 명령으로 시작한다. 기본값은 측정 반복당 1,000건, 워밍업 100건, 3회 반복이다. 짧은 재현은 예시처럼 크기를 낮춘다.

```sh
RABBITMQ_COUNT=24 RABBITMQ_WARMUP=2 RABBITMQ_REPEAT=1 ./smoke-harness/rabbitmq-integration.sh
```

이 스크립트는 Docker가 필요하며 로컬 포트 5674에 컨테이너를 띄우고 `smoke-harness/build/rabbitmq-data/`를 `/var/lib/rabbitmq`에 마운트한다. 컨테이너와 데이터 디렉터리는 그대로 남긴다. 기본 컨테이너 이름은 `smq-rabbitmq-441-integration`이다. 다른 이름·포트·데이터 위치는 `RABBITMQ_CONTAINER`, `RABBITMQ_AMQP_PORT`, `RABBITMQ_DATA_DIR`로 지정할 수 있다. 사용자와 암호 기본값은 로컬 실험용 `bench`이며 `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD`로 덮어쓸 수 있다.

측정 결과는 `smoke-harness/build/smoke-harness/rabbit-report.json`, 강제 종료 복구 결과는 `smoke-harness/build/smoke-harness/rabbit-recovery-report.json`에 기록한다. 두 보고서에 실제 브로커·클라이언트 버전, queue 설정, 작업량, 발행 확인·라우팅 오류, consumer ACK 전송, 복구 뒤 관측값과 재현 명령이 들어간다. 성능 보고서는 단건·배치 발행과 가져오기·settle 처리의 처리량과 지연 표본을 기록한다. 로컬 Docker 파일시스템과 자원 제한은 결과에 영향을 주므로 이 수치는 기준 성능으로 간주하지 않는다.

발행은 `mandatory=true`와 ReturnListener로 라우팅 실패를 오류로 세고, publisher confirm을 받은 라우팅 성공 메시지만 완료로 센다. Consumer `basic.ack`는 별도로 전송을 기록한다. RabbitMQ는 consumer ACK에 publisher confirm을 보내지 않으므로, ACK 적용은 연결을 닫은 뒤 새 consumer가 메시지를 다시 가져오는지와 ready-message 수를 별도로 관찰한다.

복구 실행은 확인 직후, 메시지를 가져온 뒤 ACK 전, 그리고 broker를 잠시 멈춘 동안 publish 요청을 보내 confirm을 얻기 전에 각각 같은 컨테이너와 데이터 디렉터리를 강제 종료·재시작한다. 새 큐를 만든 뒤 2초 기다린 다음 해당 메시지 관측 직후 broker를 중단한다. 각 결과의 `queueStableForMs`와 method 설정에 이 대기 시간을 기록한다. 이는 RabbitMQ가 quorum 큐 생성 직후 Raft 디렉터리 동기화를 지연하는 알려진 동작을 피하기 위한 설정이다 ([upstream discussion #17710](https://github.com/rabbitmq/rabbitmq-server/discussions/17710)).

이 비교의 quorum 큐는 멤버가 하나뿐이므로 노드 장애 허용성이 없다. 관측하는 것은 단일 노드 프로세스 재시작 뒤 같은 저장소에서의 복구이며, quorum failover나 노드 손실 내구성을 검증하지 않는다. 다중 노드 quorum 큐는 과반수 멤버가 유지될 때 메시지를 보존한다는 RabbitMQ의 [quorum queue 문서](https://www.rabbitmq.com/docs/quorum-queues)를 참고한다. 일반 `./gradlew check`는 Docker를 사용하지 않는다.
