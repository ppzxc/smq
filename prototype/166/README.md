# #166 스키마 버전 2 프로토타입

#166의 사전 그릴링에서 쓴 프로토타입과 프로브다. 병합하지 않는 실험 브랜치이며, 확정한 DDL과 SQL은 #168, #169의 "구현 메모" 댓글에 있다.

## 들어 있는 것

- `src/main/java/.../storage/sqlite/SqliteSchema.java`, `SqliteStorage.java`: 버전 2 설계를 적용한 프로토타입. 인덱스 넷, 승격, 저장된 준비로 고르기, 인덱스를 쓰는 살아 있는 리스 확인과 한도 도달 조회가 들어 있다. **스키마 버전 번호와 버전 1에서 올리는 단계는 넣지 않았다**(새 파일에서만 동작한다). 테스트도 고치지 않았으므로 `./gradlew check`는 통과하지 않는다.
- `patch.py`: main의 두 파일에 위 변경을 적용한 스크립트(기록용).
- `Probe.java`: 처음 문제를 재현한 프로브(깊이별 처리량, 긴 `Duration`, lock 대기 중 interrupt, dead letter 큐 대기자).
- `Probe2.java`: 버전 1과 버전 2를 비교한 프로브(얕은 큐 비용, 깊은 조건, 승격 비용, 의미 확인 17개, 쿼리 플랜).
- `Sac.java`: 단일 활성 컨슈머 큐에서 지연 NACK가 순서를 건너뛰는 것을 재현한 프로브(#178).

## 돌리는 법

프로브는 공개 API만 쓰는 단독 프로그램이다. 메인 소스를 컴파일한 클래스 디렉터리와 sqlite-jdbc jar를 클래스패스에 두고 JDK 25로 돌린다. 인자는 DB를 만들 작업 디렉터리다.

```
javac -cp <main classes> -d out prototype/166/Probe2.java
java --enable-native-access=ALL-UNNAMED -cp <main classes>:out:<sqlite-jdbc.jar> Probe2 <작업 디렉터리>
```

버전 1과 비교하려면 main에서 컴파일한 클래스와 이 브랜치에서 컴파일한 클래스로 각각 돌린다. 결과는 #166의 "프로토타입 결과" 댓글에 있다.
