# Java AI Engineering Quality Specification

**작성자 강제 정책 (Author-Enforced Engineering Policy)**

> 문서 ID: JAVA-AI-QUALITY-SPEC  
> 버전: 1.3.3  
> 작성 기준일: 2026-11-25  
> 상태: 작성자가 정의한 강제 품질 정책 (Authoritative Policy; 프로젝트 적합성 평가 후 적용)  
> 대상: Java 애플리케이션/라이브러리, Gradle 또는 Maven 프로젝트, AI 코드 생성·수정 에이전트 및 CI 파이프라인

### 변경 이력

| 버전 | 일자 | 주요 변경 |
|---|---|---|
| 1.3.3 | 2026-11-25 | 검사 억제 표기 계약(9.4, `@PolicyException`) 신설 및 주석 기반 억제 금지. STYLE-011 PMD 7 API 정정·허용 목록 확정·테스트 코드 포함. 수준 정정(OBJ-004, STYLE-005, SPR-001, MB-001, Fast Gate). 무ID 정책에 TST/AGT/TOOL ID 부여. NUL·DET·LOG 규칙군, SEC-015/016, STYLE-013/014, MB-006, TOOL-006~008 신설. CON-010 이동. SpotBugs 참조를 PMD/Error Prone 내장 검사로 대체 |

## 0. 목적과 사용 지침

이 문서는 Java 프로젝트에서 AI가 생성·수정하는 코드에 대한 **예방 → 자동 검출 → 실행 검증 → 재발 방지** 체계를 수립하기 위해 **작성자가 의도적으로 업계 일반 권고보다 엄격하게 정의한 규범적 SPEC**이다. Lauren Tan 발표에서 설명한 *Codebase → Static Analysis → Runtime Verification → Formal Verification → Prose* 우선순위를 품질 엔지니어링 프레임으로 사용하되, 아래 세부 규칙은 Oracle, SEI CERT, CWE, OWASP, Java 도구·프레임워크 공식 문서의 지침과 프로젝트 정책 후보로 구성한다. **이 문서는 업계 표준 또는 특정 발표자의 공식 규칙집이 아니다.** 외부 자료는 기술적 근거와 참고 자료이며, 최종 규칙의 강제력·범위·예외를 결정하는 주체는 이 SPEC의 작성자다. 이 문서를 적용하도록 지시받은 AI는 일반적인 관행이 더 느슨하다는 이유만으로 규칙을 완화해서는 안 된다.

AI 에이전트는 이 SPEC을 전달받았을 때 다음 순서를 반드시 따른다.

1. JDK·빌드 도구·모듈·프레임워크·배포 방식·테스트 체계·기존 규칙을 조사한다.
2. 각 규칙의 **적용 조건**을 판단한다. 프로젝트에 없는 기술은 적용하지 않는다.
3. 이미 도구/CI로 강제된 규칙은 재구현하지 않는다. 기존 검사기를 먼저 활용한다.
4. 실제 위반에 대한 증거(경로·라인·오류·재현)를 수집한다. 추정 결함을 확정 결함으로 표기하지 않는다.
5. 예방 효과가 큰 **API/모듈/타입 설계**를 먼저 평가하고 이후 정적 분석, 테스트, 문서 순으로 보완한다.
6. 변경 권한이 **진단 전용**이면 절대 파일을 수정하지 않는다. **적용 허가**가 있으면 작은 단계로 변경하고 검증한다.
7. 새로운 금지 규칙은 정상 사례·위반 사례·예외 사례 테스트와 함께 등록한다.
8. 적용 결과를 `적용 / 미적용(근거) / 예외 / 미확인`으로 보고한다.

### 0.1 정책의 권위 및 적용 원칙

1. **최우선 원칙**: 본 문서의 규칙은 작성자가 정한 강제 엔지니어링 정책이다. 업계 표준을 소개하는 참고 문서가 아니다.
2. **기술 근거와 강제력의 분리**: Oracle·CERT·Google·Spring 등의 출처는 규칙을 설계할 때 참고한 자료이며, 이 SPEC의 금지·강제 수준이 해당 출처에서 동일하게 의무화되었다는 뜻은 아니다.
3. **적용 우선순위**: 보안·법령·라이선스 요구사항을 우선 충족한다. 그 범위 안에서 이 SPEC을 프로젝트의 일반 스타일 관행보다 우선 적용한다. 상충하거나 실행할 수 없는 정책은 임의로 무시하지 말고 충돌을 보고하고 결정권자의 승인을 받는다.
4. **자동 강제**: `MUST`와 `MUST NOT`은 적용 가능한 코드에서 CI 실패를 목표로 한다. 자동 검사가 불가능한 규칙은 명시적 검토 게이트와 증거로 강제한다.
5. **기술별 활성화**: Spring·MyBatis·DDD 등의 조건부 규칙은 해당 기술/아키텍처가 확인되면 적용한다. 단순히 `PROFILE`이라는 이유로 기본적으로 무시하지 않는다.
6. **예외 관리**: 예외에는 사유, 제한된 범위, 승인 주체, 가능하면 종료·재검토 시점이 필요하다. AI 에이전트는 스스로 강제 정책을 완화하거나 우회하지 못한다.
7. **레거시 처리**: 기존 위반을 발견했다고 무차별 수정하지 않는다. 신규 위반은 차단하고, 기존 위반은 증거와 영향 범위에 따라 계획적으로 정리한다.

### 0.2 규범 용어

- **MUST**: 해당 규칙의 적용 조건을 충족한다면 준수해야 한다. 검증 가능한 경우 CI 필수 게이트 대상.
- **MUST NOT**: 적용 조건 안에서 금지한다. 보안/정합성 위험이 크거나 프로젝트가 확정한 정책에 사용.
- **SHOULD / SHOULD NOT**: 일반적으로 권장/지양하나 기술적 근거가 있는 예외를 허용한다.
- **MAY**: 프로젝트의 성격과 비용에 따라 선택한다.
- **PROFILE**: 기술·아키텍처 조건에 따라 적용되는 작성자 정책. 조건이 충족되면 표기된 강도(`PROFILE-MUST`/`PROFILE-SHOULD`)로 적용하며, 정책을 무단 비활성화하지 않는다.

**주의:** `MUST`는 이 SPEC을 채택한 프로젝트에서 작성자가 정한 강제 요구사항이며, 법적 의무나 모든 Java 코드에 대한 보편적 강제 규범을 의미하지 않는다. CWE는 취약점 분류이며 직접적인 Lint 규칙 구현을 보장하지 않는다. 정적 분석은 완전한 정당성 증명이 아니다.

### 0.3 규칙 레코드 스키마

| 필드 | 의미 |
|---|---|
| ID | 이 SPEC에서 사용하는 안정적 식별자 |
| 수준 | MUST / SHOULD / PROFILE |
| 적용 조건 | 어떤 코드·모듈·기능에 적용되는지 |
| 검사 | 1차 자동화 수단 또는 인간 검토 |
| 근거 | 공식 출처(섹션 12) / 프로젝트 선택 |

규칙 번호는 **이 SPEC 내부 ID**이다. CERT/SpotBugs/Error Prone/PMD/CWE의 공식 ID는 혼동을 막기 위해 필요할 때만 명시한다.

접두사: `CBD` 코드베이스 설계, `ARC` 아키텍처, `OBJ` 객체·타입, `ERR` 예외, `CON` 동시성, `NUL` Nullness, `DET` 결정성, `LOG` 로깅, `STYLE` 스타일·유지보수, `SEC` 보안, `MB` MyBatis, `SPR` Spring, `VER` 실행 검증, `TST` 테스트 데이터, `FORM` 형식 검증, `AGT` 에이전트 작업 계약, `TOOL` 툴체인. 삭제·이동된 ID는 재사용하지 않고 표에 표기를 남긴다.

## 1. Codebase: 코드와 아키텍처 자체로 오류 예방

이 계층은 Lint의 상위 단계다. Lint로 경고하기 전에 **잘못된 사용을 막는 API·타입·모듈 설계**가 가능한지 평가한다.

| ID | 수준 | 규칙/금지 대상 | 적용 조건 | 확인/강제 수단 |
|---|---|---|---|---|
| CBD-001 | SHOULD | 같은 기능의 중복 구현·충돌하는 패턴을 줄이고 권장 구현을 명확히 한다 | 전체 | 코드 검색·리뷰·중복 분석 |
| CBD-002 | SHOULD | Deprecated API의 신규 도입을 피하고 대체 API를 검토한다 | 전체 | `javac -Xlint:deprecation`, Error Prone |
| CBD-003 | SHOULD | 오용하기 쉬운 primitive 매개변수와 문자열 상태코드를 업무 타입/enum/VO로 치환한다 | 업무 API | API 리뷰·테스트 |
| CBD-004 | SHOULD | 객체의 유효성 검증은 명확한 생성·전이 경계에서 수행한다 | 상태 보유 모델 | 테스트·타입 설계 |
| CBD-005 | SHOULD | 캡슐화를 깨는 공개 가변 필드/배열/컬렉션 노출을 피한다 | 변경 가능한 데이터 | PMD/커스텀 규칙·방어적 복사 테스트 |
| CBD-006 | SHOULD | 불필요한 전역 가변 상태·숨은 공유 상태를 줄인다 | 동시 실행 코드 | 리뷰·정적 분석 |
| CBD-007 | SHOULD | 재발하는 오류는 해당 API/구조를 개선할 수 있는지 먼저 검토한다 | 반복 결함 | 결함 회고·회귀 테스트 |
| CBD-008 | SHOULD | 단순히 컴파일을 통과시키기 위해 모듈 경계 또는 캡슐화를 우회하지 않는다 | 멀티모듈 | 의존성 검사 |
| CBD-009 | SHOULD | 사용되지 않는 코드·설명 불일치 주석·레거시 우회책을 정리한다 | 전체 | 리뷰·PMD/Error Prone 탐지 후 AI 수정 |
| CBD-010 | PROFILE-SHOULD | 닫힌 상태 계층은 `sealed interface`/`sealed class`를 우선 사용한다 (DTO 성격의 데이터 전달 모델은 `STYLE-009`에 따라 `record` 및 프로젝트 규칙을 따름) | Java 17+ 및 합의된 도메인 | AST 규칙·설계 리뷰 |

### 1.1 아키텍처 선택 프로파일: DDD/헥사고날

**이 절은 조건부 강제 정책이다.** 모든 Java 프로젝트에 일괄 적용하지 않지만, DDD/헥사고날 아키텍처를 채택한 프로젝트에서는 해당 `PROFILE-MUST` 규칙을 필수로 강제한다.

| ID | 수준 | 프로젝트 정책 | 검사 |
|---|---|---|---|
| ARC-001 | PROFILE-MUST | Domain은 Spring, JPA, MyBatis, HTTP/DB 구현 등 기술 프레임워크에 의존하지 않는다 | Gradle/Maven 의존성 + ArchUnit |
| ARC-002 | PROFILE-MUST | Domain → Application/Adapter 역참조를 금지한다 | 모듈 빌드 그래프 + ArchUnit |
| ARC-003 | PROFILE-MUST | Application → Adapter 구현 직접 의존을 금지하고 Port로 연결한다 | ArchUnit |
| ARC-004 | PROFILE-SHOULD | Adapter 간 내부 구현 직접 참조를 최소화한다 | ArchUnit |
| ARC-005 | PROFILE-MUST | 패키지/모듈 순환 의존을 방지한다 | ArchUnit, 빌드 그래프 |
| ARC-006 | PROFILE-SHOULD | Aggregate가 상태 전이와 업무 불변식의 책임을 가진다 | 도메인 테스트·리뷰 |
| ARC-007 | PROFILE-SHOULD | Value Object는 외부 변경이 불가능하도록 설계한다 | 정적 분석·테스트 |
| ARC-008 | PROFILE-SHOULD | Domain Service는 숨은 인스턴스 상태를 지양한다 | ArchUnit/리뷰 |
| ARC-009 | PROFILE-SHOULD | Domain API에 `ResultSet`, Servlet Request, ORM 객체 등 기술 타입을 노출하지 않는다 | ArchUnit |
| ARC-010 | PROFILE-SHOULD | Domain Event는 발생한 사실을 표현하며 전달/저장 기술을 포함하지 않는다 | API 리뷰 |
| ARC-011 | PROFILE-SHOULD | 조회·저장·외부 연동 포트의 책임을 명확히 분리한다 | Port/API 리뷰 |
| ARC-012 | PROFILE-MUST | 아키텍처 위반 해결을 위해 금지 의존성을 추가하거나 테스트를 제거하지 않는다 | 변경 리뷰·CI |

> 위 규칙에서 `ARC-006`처럼 의미론적 내용은 Lint만으로 완전히 판별할 수 없다. 이름·패키지만 검사한 뒤 의미상 준수를 확정하지 않는다.

> **1차 검출 수단 선택**: Spring Modulith를 사용하면 모듈 경계·순환은 `ApplicationModules.of(..).verify()`를, jMolecules 어노테이션을 사용하면 `jmolecules-archunit`의 DDD/헥사고날 규칙을 1차 수단으로 사용한다. 이들이 이미 강제하는 규칙을 ArchUnit 수동 규칙으로 중복 구현하지 않으며, 수동 규칙은 공백 영역(예: ARC-009 기술 타입 노출)에만 추가한다.

## 2. Static Analysis: 일반 Java 결함 및 금지 규칙

### 2.1 객체·타입·동등성·컬렉션

| ID | 수준 | 금지/권장 규칙 | 검사 예시 | 근거 |
|---|---|---|---|---|
| OBJ-001 | MUST | 배열 내용 비교를 `array.equals()`로 처리하지 않는다 | Error Prone `ArrayEquals` | [EP] |
| OBJ-002 | MUST | 배열의 `toString()`을 내용 표현으로 오인하지 않는다 | Error Prone `ArrayToString` | [EP] |
| OBJ-003 | MUST | 서로 비교 불가능한 타입의 `equals()`를 사용하지 않는다 | Error Prone `EqualsIncompatibleType` | [EP] |
| OBJ-004 | MUST | `equals()`/`hashCode()` 계약을 유지한다 | Error Prone `EqualsHashCode`, PMD `OverrideBothEqualsAndHashcode` | [EP][PMD] |
| OBJ-005 | SHOULD | enum ordinal을 외부 저장 포맷·영속 식별자로 사용하지 않는다 | Error Prone `EnumOrdinal` | [EP] |
| OBJ-006 | MUST | 변경 가능한 컬렉션 순회 중 안전하지 않은 구조 변경을 피한다 | Error Prone `ModifyCollectionInEnhancedForLoop` | [EP] |
| OBJ-007 | MUST | Stream 처리 중 소스 컬렉션을 부적절하게 변경하지 않는다 | Error Prone `ModifySourceCollectionInStream` | [EP] |
| OBJ-008 | SHOULD | 배열/가변 객체를 방어적 복사 없이 노출·보관하지 않는다 | PMD `ArrayIsStoredDirectly`, `MethodReturnsInternalArray`, 컬렉션은 STYLE-013·테스트 | [PMD] |
| OBJ-009 | SHOULD | 공개 API에서 구현 세부사항을 과도하게 노출하는 타입을 피한다 | Error Prone `NonApiType`, 리뷰 | [EP] |
| OBJ-010 | MUST | 무의미하거나 위험한 축소 숫자 변환을 방지한다 | Error Prone `NarrowCalculation`, `NarrowingCompoundAssignment` | [EP] |
| OBJ-011 | SHOULD | 로케일에 민감한 연산에서는 Locale을 명시한다 | Error Prone `DefaultLocale` | [EP] |
| OBJ-012 | SHOULD | 기본 인코딩을 가정하지 않고 Charset을 명시한다 (JDK 18+ 기본 UTF-8 여부와 무관) | Error Prone `DefaultCharset` | [EP] |

### 2.2 예외 및 실패 처리

| ID | 수준 | 금지/권장 규칙 | 적용/예외 | 근거 |
|---|---|---|---|---|
| ERR-001 | MUST | Checked Exception을 무시하거나 억제하지 않는다 | 의도적 복구 경로는 명시 | CERT ERR00-J |
| ERR-002 | MUST | 예외 메시지·스택·응답에 민감정보를 노출하지 않는다 | 인증정보·비밀·개인정보 포함 | CERT ERR01-J, OWASP |
| ERR-003 | SHOULD | 실패 시 객체 불변식과 일관성을 유지한다 | 상태 변경 유스케이스 | CERT ERR03-J |
| ERR-004 | MUST | `finally`에서 `return`/`throw`로 기존 예외를 가리지 않는다 | 자원 정리 | CERT ERR04-J 취지 |
| ERR-005 | SHOULD | `Exception`, `RuntimeException`, `Throwable`을 의미 없는 일반 예외로 직접 던지지 않는다 | 구체적 타입 우선 | CERT ERR07-J |
| ERR-006 | SHOULD | NPE를 일반적인 정상 제어 흐름으로 catch하지 않는다 | 프레임워크 경계 등 공식 예외 검토 | CERT ERR08-J |
| ERR-007 | MUST | 빈 catch, 로그 후 성공 반환 등 실패 은폐를 금지한다 | 복구 동작이 의미 있게 정의된 경우 제외 | CERT/PMD 범주 |
| ERR-008 | SHOULD | 예외 재포장 시 원인(cause)을 보존한다 | 오류 추적 가능성 | Error Prone `UnusedException` 등 |
| ERR-009 | MUST | 정상적인 실패 경로를 위해 `System.exit()`를 라이브러리에서 호출하지 않는다 | 프로세스 종료 책임이 있는 실행 진입점 예외 | CERT ERR09-J 취지 |
| ERR-010 | SHOULD | Closeable 자원은 `try-with-resources` 등으로 안전하게 닫는다 | 소유권이 호출자에 있는 경우 예외 | CERT 권고, Error Prone/PMD |
| ERR-011 | SHOULD | 검증을 통과하기 위해 예외를 삼키거나 테스트를 약화하지 않는다 | 전체 | 품질 정책 |

### 2.3 동시성·스레드·성능

| ID | 수준 | 금지/권장 규칙 | 적용/예외 | 근거 |
|---|---|---|---|---|
| CON-001 | SHOULD | `Thread.sleep()`, `yield()` 등 시간 대기를 동기화의 정당성 증거로 사용하지 않는다 | 폴링·백오프 등 정당한 대기 제외 | CERT CON51-J |
| CON-002 | MUST | 공유 가변 데이터 접근의 가시성·원자성·동기화 계약을 지킨다 | 멀티스레드 | Java Memory Model/CERT |
| CON-003 | SHOULD | Thread Pool의 큐·최대 스레드·거부 정책을 명시적으로 검토한다 | Executor 구성 | Error Prone·CERT |
| CON-004 | MUST | 종료 불가능한 바쁜 대기/무제한 재시도로 자원을 고갈시키지 않는다 | 장애 경로 | Oracle DoS 가이드 |
| CON-005 | SHOULD | Lock 순서와 자원 생명주기를 설계하고 교착 가능성을 검증한다 | 둘 이상 Lock | CERT LCK 계열 |
| CON-006 | SHOULD | 비동기 동작을 `sleep`으로 맞추지 않고 완료 신호·Future·Latch 등으로 검증한다 | 비동기 테스트/운영 코드 | CERT CON51-J 취지 |
| CON-007 | SHOULD | 가상 스레드를 고정 크기 풀에 넣어 재사용하지 않는다 | Virtual Thread 사용 | Java 25 공식 가이드 |
| CON-008 | SHOULD | Virtual Thread 수가 아니라 제한 자원에 세마포어 등 동시성 제한을 둔다 | DB/외부 API 제한 | Java 25 공식 가이드 |
| CON-009 | SHOULD | Virtual Thread별 대용량 `ThreadLocal` 캐싱을 지양한다 | Virtual Thread 사용 | Java 25 공식 가이드 |
| CON-010 | - | *(이동됨 — 규칙 설계 지침이므로 12.1로 이동)* | - | - |
| CON-011 | SHOULD | CPU 집약 작업에서 Virtual Thread의 성능 이점을 무조건 가정하지 않는다 | 성능 평가 | Java 25 공식 가이드 |
| CON-012 | SHOULD | Timeout, 취소, 인터럽트 처리를 요구사항과 API 계약에 맞게 구현한다 | 네트워크/장기 작업 | Java 공식 API·리뷰 |

### 2.4 코드 스타일·유지보수·기계적 검사

| ID | 수준 | 규칙 | 자동 검사 | 성격 |
|---|---|---|---|---|
| STYLE-001 | PROFILE-SHOULD | 와일드카드 Import 금지 | Error Prone `WildcardImport` (ERROR로 승격) | Google 스타일 |
| STYLE-002 | PROFILE-SHOULD | 네이밍·들여쓰기·괄호·Import 정렬 통일 | Spotless (포맷·Import 정렬), PMD 내장 `codestyle` 네이밍 규칙 | Google 스타일 |
| STYLE-003 | PROFILE-SHOULD | 공개 API와 비직관적 설계 근거에 Javadoc 정책 정의 | PMD `CommentRequired` (공개 API 범위로 구성) | Google 스타일 |
| STYLE-004 | - | *(삭제됨 — `STYLE-011` 일반 주석 금지 정책으로 통합)* | - | - |
| STYLE-005 | MUST | 검사 억제는 9.4 계약(`@SuppressWarnings` + `@PolicyException` 짝)으로만 허용한다. `// NOPMD`, `//noinspection`(IntelliJ 전용, CI 무효) 등 주석 기반 억제와 빌드 설정을 통한 규칙 비활성화·심각도 하향을 금지한다 | PMD 커스텀(짝 검사), ArchUnit(만료 검사), 빌드 설정 변경 리뷰 | **작성자 강제 정책** |
| STYLE-006 | SHOULD | 정당한 이유 없이 `@Disabled`로 회귀 테스트를 제외하지 않는다 | PMD/테스트 검사 | 품질 정책 |
| STYLE-007 | SHOULD | 과도한 조건 중첩·복잡한 메서드·무의미한 Catch/재던지기를 개선한다 | PMD Design | PMD |
| STYLE-008 | SHOULD | 컴파일 경고를 숨기는 Raw Type·검사되지 않은 캐스트를 줄인다 | `javac -Xlint`, Error Prone | Java 컴파일러 |
| STYLE-009 | MUST | 불변 데이터 전달 객체(DTO)는 `record`를 기본으로 사용하되, 특정 `dto` 패키지명을 일괄 강제하지 않고 **프로젝트의 패키지 구조 및 네이밍 규칙(예: 패키지 경로, 접미사·접두사 컨벤션)에 맞춰 유연하게 적용**한다 (프레임워크·상속·상호운용성 예외는 승인 필요). 프로젝트 규칙에 부합하는 자동 강제 방법론(ArchUnit·PMD Custom Rule 등)을 수립해 적용한다 | 프로젝트 규칙 기반 ArchUnit / PMD Custom Rule | **작성자 강제 정책** |
| STYLE-010 | MUST | Lombok 의존성, import 및 Annotation의 신규 사용을 금지한다. Lombok 어노테이션은 `SOURCE` 보존이므로 바이트코드 기반 ArchUnit으로 검출하지 않는다 | Gradle: 모든 configuration(`compileOnly`·`annotationProcessor`·`testCompileOnly`·`testAnnotationProcessor` 포함)에서 `org.projectlombok` 해석 시 실패 / PMD: `lombok.` import·완전 한정 어노테이션 검사 / 저장소: `lombok.config` 존재 시 실패 | **작성자 강제 정책** |
| STYLE-011 | MUST | 일반 구현 주석을 전면 금지한다. 아래 보충 설명의 허용 목록에 해당하는 주석만 예외로 한다. `src/main`과 `src/test`에 모두 적용하며 생성 코드는 제외한다 | PMD 7 Java Custom Rule (`ASTCompilationUnit#getComments()` 순회 + 허용 목록 판정) | **작성자 강제 정책** |
| STYLE-012 | - | *(삭제됨 — `SPR-001` Spring 생성자 주입 정책으로 통합)* | - | - |
| STYLE-013 | MUST | `record`의 컬렉션·맵 컴포넌트는 compact constructor에서 `List.copyOf`·`Set.copyOf`·`Map.copyOf` 등으로 불변 복사한다 (`record`는 얕은 불변) | PMD 커스텀(record 컴포넌트 타입 + compact constructor 재대입 검사), 테스트 | **작성자 강제 정책** |
| STYLE-014 | MUST | `record` 컴포넌트에 배열 타입을 사용하지 않는다. 자동 생성 `equals`/`hashCode`/`toString`이 참조 기준으로 동작해 OBJ-001·OBJ-002 위험을 만든다. 바이너리는 불변 VO 또는 `List`로 표현한다 | ArchUnit(record 필드 타입) | **작성자 강제 정책** |

> **주석(`STYLE-011`) 및 DTO(`STYLE-009`) 강제 방법론 보충 설명**
> - **일반 주석 금지 (`STYLE-011`)**: XPath는 주석을 볼 수 없다. PMD 7에서 주석은 AST 노드가 아니라 컴파일 단위에 부속된 토큰이며 `ASTCompilationUnit#getComments()`가 `List<JavaComment>`로 제공한다(Javadoc은 하위 타입 `JavadocComment`). PMD 6의 `SingleLineComment`·`MultiLineComment`·`FormalComment` 클래스는 PMD 7에 없으므로 사용하지 않는다. 규칙은 `AbstractJavaRule#visit(ASTCompilationUnit, Object)`에서 전체 주석을 순회하고, Javadoc 허용 여부는 `JavadocCommentOwner#getJavadocComment()`로 역조회한 소유 선언의 가시성으로 판정한다. 세부 API는 고정한 PMD 버전(TOOL-007)의 문서로 확인한다.
>   - **허용**: (1) 파일의 첫 선언 이전에 위치한 라이선스·법률 고지 블록 1개, (2) `package-info.java`의 Javadoc, (3) 소유 선언과 그 모든 외곽 타입이 `public`/`protected`인 선언에 부착된 Javadoc, (4) `// spotless:off`·`// spotless:on` 짝(Spotless `toggleOffOn()` 구성 전제, 짝 불일치는 위반).
>   - **금지**: 그 외 모든 주석. `private`·package-private 선언의 Javadoc, 메서드 본문 안의 `/** */`, TODO/FIXME(이슈 트래커로 이관), 주석 기반 억제(`// NOPMD`, `//noinspection` 등, STYLE-005), 테스트의 given/when/then 구분 주석(`@DisplayName`·메서드명·헬퍼 메서드로 표현)을 명시적으로 포함한다.
> - **DTO 유연 적용 및 강제 방법론 (`STYLE-009`)**: 모든 프로젝트가 별도의 `dto` 하위 패키지를 두는 것은 아니므로 패키지명 `dto`를 일괄 강제하지 않는다. 대신 프로젝트가 채택한 아키텍처·패키지 구조(`..view..`, `..command..`, `..query..`, `..dto..`, `..api..` 등)와 네이밍 규칙(`*Dto`, `*Request`, `*Response`, `*Command`, `*Query` 등)을 기준으로 DTO 대상 범위를 정의하고, 해당 범위의 불변 데이터 모델이 `record`로 작성되도록 프로젝트 맞춤형 ArchUnit 또는 PMD 커스텀 규칙으로 유연하게 강제한다.

### 2.5 Nullness·결정성·로깅

| ID | 수준 | 규칙 | 검사 | 근거 |
|---|---|---|---|---|
| NUL-001 | MUST | NullAway 적용 범위는 `package-info.java`의 JSpecify `@NullMarked`로 선언하고 NullAway 검사 대상 구성과 일치시킨다. 신규 패키지는 `@NullMarked`로 시작한다 | NullAway, `package-info` 존재·어노테이션 검사(ArchUnit 또는 PMD) | [NULLAWAY][JSPECIFY] |
| NUL-002 | MUST | Nullness 어노테이션은 JSpecify(`org.jspecify.annotations`)만 신규 사용한다. `javax.annotation`, JetBrains, Spring `@Nullable` 등의 혼용을 금지한다 | PMD import 검사 | 작성자 정책 |
| NUL-003 | SHOULD | `Optional`은 반환 타입에 한정하고 필드·매개변수·컬렉션 원소로 사용하지 않는다 | PMD 커스텀·리뷰 | 작성자 정책 |
| DET-001 | MUST | 업무 코드에서 인자 없는 `now()`(`Instant`, `LocalDateTime`, `ZonedDateTime` 등)와 `System.currentTimeMillis()`를 직접 호출하지 않고 주입된 `Clock`/`InstantSource`를 사용한다. 실행 진입점의 `Clock` 생성, 로깅·메트릭 계측, 경과 시간 측정용 `System.nanoTime()`은 대상이 아니다 | ArchUnit(메서드 호출 규칙) | 작성자 정책 |
| DET-002 | SHOULD | 업무 결과에 영향을 주는 난수·식별자 생성(`new Random()`, `UUID.randomUUID()` 등)은 주입 가능한 공급자를 통해 수행한다 | ArchUnit·리뷰 | 작성자 정책 |
| LOG-001 | MUST | 로깅은 SLF4J 파라미터화 메시지(`{}`)를 사용하고 메시지 인자에서 문자열 결합·`String.format`을 금지한다 | PMD 커스텀 XPath | 작성자 정책 |
| LOG-002 | MUST | 예외 로깅 시 예외 객체를 마지막 인자로 전달해 스택과 cause를 보존한다. 예외 객체 없이 `getMessage()`만 기록하지 않는다 | PMD 커스텀 XPath | ERR-008 취지 |
| LOG-003 | MUST | SEC-010 대상 값(비밀번호·토큰·개인정보)을 로그 인자로 직접 전달하지 않는다. 민감 값을 담는 타입은 `toString()`을 마스킹하며, `record`의 자동 생성 `toString()`도 대상이다 | 리뷰·테스트 | OWASP |

## 3. Security: Java 보안 코드 규칙

`SEC-*`는 위험 유형을 나타내며, 보안 게이트의 강제 범위는 노출 경로와 위협 모델을 바탕으로 확정한다.

| ID | 수준 | 규칙 | 검사/증거 | 참조 |
|---|---|---|---|---|
| SEC-001 | MUST | 외부 입력이 SQL 텍스트에 직접 연결되지 않도록 Parameter Binding 등 사용 | SAST, SQL 테스트 | CWE-89, OWASP |
| SEC-002 | MUST | 사용자 제어 경로에 대한 정규화·허용 경로 검사 | SAST, 경로 공격 테스트 | CWE-22 |
| SEC-003 | MUST | 암호·API Key·토큰을 소스에 하드코딩하지 않는다 | Secret Scanner | CWE-798 |
| SEC-004 | MUST | 사용자 제어 Shell 인자를 안전하게 처리하고 명령 문자열 결합을 피한다 | SAST, 프로세스 호출 테스트 | CWE-78 |
| SEC-005 | MUST | HTML·스크립트 출력 컨텍스트에 맞는 인코딩/이스케이프 적용 | SAST, 웹 테스트 | CWE-79, OWASP |
| SEC-006 | MUST | 신뢰할 수 없는 Java 직렬화 입력의 역직렬화를 원칙적으로 피하고 안전한 대안을 검토한다 | 코드 탐색·보안 테스트 | CWE-502, Oracle |
| SEC-007 | MUST | 인증 여부와 별개로 서버에서 작업별 권한 검사를 수행한다 | 접근제어 테스트 | CWE-862, OWASP |
| SEC-008 | MUST | 취약하거나 부적절한 암호 알고리즘·설정을 사용하지 않는다 | 암호 정책·SAST | CWE-327, OWASP |
| SEC-009 | MUST | 입력 크기·정규식 복잡도·압축 해제·XML 처리 등 자원 고갈 방지 | 제한값·부하 테스트 | Oracle DoS |
| SEC-010 | MUST | 로그/예외/응답에 비밀번호·토큰·민감 데이터가 노출되지 않게 한다 | 로그 검사·보안 테스트 | Oracle, OWASP |
| SEC-011 | SHOULD | 외부 통신에서 TLS 인증서/호스트 검증을 비활성화하지 않는다 | 구성·통합 테스트 | OWASP |
| SEC-012 | SHOULD | 최소 권한·안전한 기본값·권한 경계를 적용한다 | 인프라/코드 리뷰 | Oracle, OWASP |
| SEC-013 | SHOULD | 외부 입력은 타입·길이·형식·허용값을 경계에서 검증한다 | 계약·경계 테스트 | Oracle, OWASP |
| SEC-014 | SHOULD | 민감한 파일/스트림/네트워크 자원 소유권과 종료를 명확히 한다 | 정적 분석·통합 테스트 | Oracle, CERT |
| SEC-015 | MUST | 직접·전이 의존성의 알려진 취약점 스캔(SCA)을 필수 게이트로 실행하고, 차단 기준(심각도·수정 버전 존재 여부)과 예외 승인·만료 절차를 정의한다 | SCA 도구(예: OWASP Dependency-Check, OSV-Scanner, Trivy) | [CWE-1395] |
| SEC-016 | SHOULD | Gradle dependency locking과 dependency verification(`verification-metadata.xml`)으로 해석 결과와 아티팩트 무결성을 고정한다 | Gradle 빌드 | [GRADLE-DV] |

### 3.1 데이터 접근 선택 프로파일: MyBatis

| ID | 수준 | 규칙 | 검사 |
|---|---|---|---|
| MB-001 | PROFILE-MUST | SQL에 들어가는 데이터 값은 `#{}` 바인딩만 사용한다. `${}`는 MB-003의 Allowlist 식별자에만 허용한다 | XML Mapper 검사·SQL 테스트 |
| MB-002 | PROFILE-MUST | `${}`에 무검증 사용자 입력을 넣지 않는다 | 매퍼·호출 경로 리뷰, SAST |
| MB-003 | PROFILE-SHOULD | 식별자(정렬 컬럼·테이블명) 동적 구성은 명시적 Allowlist 매핑으로 제한한다 | 입력 검증·SQL 테스트 |
| MB-004 | PROFILE-SHOULD | Mapper 구현과 DB 예외가 상위 도메인 계약에 누수되지 않게 한다 | ArchUnit·API 리뷰 |
| MB-005 | PROFILE-SHOULD | 구식·Deprecated Mapper 요소의 신규 사용을 피한다 | XML 검사·버전 문서 |
| MB-006 | PROFILE-MUST | `record` 결과 매핑 방식을 프로젝트 단위로 하나로 지정한다: `<constructor>` 명시 매핑 또는 이름 기반 생성자 자동 매핑(`argNameBasedConstructorAutoMapping`, MyBatis 3.5.10+). 이름 기반을 택하면 `-parameters` 컴파일 옵션을 필수로 한다 | 설정 검사·Mapper 통합 테스트 |

## 4. Framework: Spring 선택 프로파일

**Spring 사용 프로젝트에서는 조건에 맞는 규칙을 적용한다.** 프레임워크 공식 문서의 주의사항과 이 SPEC 작성자의 강제 정책을 구별한다.

| ID | 수준 | 규칙/검토항목 | 검사 |
|---|---|---|---|
| SPR-001 | PROFILE-MUST | Field Injection(`@Autowired` 필드 직접 주입)을 금지하고 필수 의존성은 생성자 주입(Constructor Injection)을 우선 사용한다. (1) `final` 키워드를 통한 의존성 불변성 보장 및 초기화 이후 변경 차단, (2) Spring 컨테이너 부팅 시점에서의 순환 참조(Circular Dependency) 조기 탐지, (3) DI 프레임워크나 리플렉션 없이 순수 Java `new` 호출만으로 단위 테스트 작성 가능, (4) 생성자 시그니처 강제를 통한 필수 의존성 누락 컴파일 단계 방지를 목적으로 한다. 선택적 의존성이나 재설정이 필요한 제한적 예외에만 Setter/메서드 주입을 허용한다 | ArchUnit `GeneralCodingRules.NO_CLASSES_SHOULD_USE_FIELD_INJECTION`·리뷰 (Setter/메서드 주입 예외는 9.4로 표기) |
| SPR-002 | PROFILE-SHOULD | 순환 Bean 의존을 피한다 | ApplicationContext 부팅 테스트 |
| SPR-003 | PROFILE-MUST | 프록시 기반 `@Transactional`의 Self-invocation을 트랜잭션 보장으로 간주하지 않는다 | 통합 테스트·호출 그래프 리뷰 |
| SPR-004 | PROFILE-SHOULD | 트랜잭션 경계·예외 Rollback 정책을 명시한다 | 실제 DB 통합 테스트 |
| SPR-005 | PROFILE-SHOULD | `@Async` 등 비동기 경계에서 ThreadLocal/트랜잭션 전파를 가정하지 않는다 | 실행 테스트 |
| SPR-006 | PROFILE-SHOULD | Controller와 업무 로직, DB Adapter 책임을 분리한다 | ArchUnit·설계 리뷰 |
| SPR-007 | PROFILE-SHOULD | 공개된 Bean·API의 인증, 권한, 입력 검증 누락을 검사한다 | Spring Security 테스트 |

## 5. Runtime Verification: 실행·회귀 검증 요구사항

| ID | 수준 | 요구사항 | 실행/증거 |
|---|---|---|---|
| VER-001 | MUST | 변경 범위의 단위 테스트를 실행하고 실제 결과를 보고한다 | JUnit/테스트 리포트 |
| VER-002 | MUST | 결함 수정 시 가능하면 실패를 재현하는 회귀 테스트를 추가한다 | 회귀 케이스 |
| VER-003 | MUST | 아키텍처 의존성 변경 시 해당 아키텍처 테스트를 실행한다 | ArchUnit |
| VER-004 | SHOULD | DB/메시징/외부 서비스 변경 시 실제 연동을 검증한다 | Testcontainers·통합 테스트 |
| VER-005 | SHOULD | TCP/파일/HTTP Wire Format 변경 시 Conformance Test를 수행한다 | 규격 벡터·프로토콜 테스트 |
| VER-006 | SHOULD | 장애·재시도·Timeout·중복 요청·동시성 시나리오를 검증한다 | 실패 주입·경계 테스트 |
| VER-007 | SHOULD | 핵심 로직의 테스트가 결함을 탐지하는지 Mutation Testing으로 평가한다 | PIT 리포트 |
| VER-008 | SHOULD | JUnit Jupiter의 `@ParameterizedTest`·`@MethodSource`·`@RepeatedTest`와 Instancio의 데이터 생성 기능을 활용해 보안·상태 불변식을 다양한 입력으로 검증한다. 중요한 경계값은 별도 명시한다 | JUnit Jupiter + Instancio |
| VER-009 | MUST | 검증을 실행하지 않았으면 PASS라고 보고하지 않는다 | 실제 Task 로그 |
| VER-010 | MUST | 필수 검사 실패를 테스트 비활성화/검사 제외로 무단 우회하지 않는다 | CI 설정 변경 리뷰 |
| VER-011 | SHOULD | 검증 증거에 커밋 SHA·환경·명령·결과·시각을 기록한다 | CI Artifact |
| VER-012 | SHOULD | Testcontainers 컨테이너 재사용/병렬화는 테스트 격리와 재현성 검증 후 선택한다 | CI 안정성 측정 |
| VER-013 | SHOULD | 필수 게이트와 비용이 큰 심층 테스트를 분리한다 | CI 정책 |
| VER-014 | MUST | 무작위 데이터 기반 검증 실패는 Seed 또는 실패 입력을 기록하여 재현 가능해야 한다. Instancio 사용 테스트는 TST-003을 따른다 | 테스트 로그·실패 리포트, ArchUnit(Instancio 사용 테스트 클래스의 확장 적용 검사) |
| VER-015 | MUST | 무작위 데이터 생성만으로 모든 경계 조건이 충족된다고 가정하지 않는다. 최소·최대 길이, Null, 인코딩 등 명시적 경계 테스트를 유지한다 | JUnit `@ParameterizedTest`·`@MethodSource` |

### 5.1 프로토콜/메시징 시스템의 검증 속성 예시 (선택)

- `decode(encode(message)) == message` — 유효한 메시지 범위 내 Round-trip.
- 하나의 프레임이 임의의 TCP Chunk로 분할되어도 최종 파싱 결과가 동일함.
- 잘못된 길이·인코딩·체크섬·버전·메시지 타입은 정의된 오류로 처리됨.
- 중복 전송·재시도·Timeout·연결 중단 시 업무 계약에 맞는 결과가 나옴.
- 상태 전이의 허용/거부, 동시 처리에서의 정합성 확인.

위 항목은 프로젝트의 실제 프로토콜과 계약에 맞게 **명세화하기 전까지 예시**이며 보편적 성질로 가정하지 않는다.

## 6. Formal Verification 및 상태 기반 검증

| ID | 수준 | 내용 | 선택 기준 |
|---|---|---|---|
| FORM-001 | SHOULD | JUnit Jupiter + Instancio를 이용한 생성 데이터 반복 테스트로 입력 공간과 업무 불변식을 폭넓게 탐색한다. 이는 자동 Shrinking을 포함하는 전문 Property-Based Testing 엔진과 동등하지 않다 | 파서·VO·정규화·상태 처리 |
| FORM-002 | MAY | State Machine/Statechart 기반 테스트로 불법 전이·교착·누락 경로 탐색 | 상태가 복잡한 업무 |
| FORM-003 | MAY | TLA+로 동시성·분산 상태 모델과 안전성/활성 속성 검증 | 메시지 중복·재시도·리더 선출·정합성 |
| FORM-004 | MAY | Lean/JML 등 형식적 명세·증명을 특정 고위험 알고리즘에 적용 | 결함 비용 매우 높음 |
| FORM-005 | MUST | 속성 기반 실행 테스트와 수학적 증명을 동일한 보장으로 설명하지 않는다 | 보고서/품질 문서 |
| FORM-006 | MUST | 형식 모델이 실제 구현과 일치한다는 연결 근거 없이 제품 전체가 증명됐다고 주장하지 않는다 | 형식 검증 도입 시 |

### 6.1 생성 데이터 기반 검증 정책

| ID | 수준 | 규칙 |
|---|---|---|
| TST-001 | MUST | 신규 Java 테스트는 JUnit Jupiter를 기본 플랫폼으로, 복잡한 테스트 객체 생성에는 Instancio를 사용한다. 기존 테스트의 일괄 전환은 별도 승인 없이 수행하지 않는다 |
| TST-002 | SHOULD | 명시 사례는 `@ParameterizedTest` + `@MethodSource`로, 반복 생성 데이터 시나리오는 `@RepeatedTest` 및 Instancio JUnit 연동(필요 시 `@InstancioSource`)으로 구성한다. 지원 API는 채택한 Instancio 버전의 공식 문서로 확인한다 |
| TST-003 | MUST | Instancio로 무작위 데이터를 생성하는 테스트 클래스는 `@ExtendWith(InstancioExtension.class)`를 적용해 실패 시 Seed가 출력되게 한다. 재현은 `@Seed`로 고정하며, 재현된 입력을 TST-004의 명시 경계 사례로 승격할지 검토한다 |
| TST-004 | MUST | Null, 빈 값, 최대 길이, 정수 범위, UTF-8 멀티바이트 등 위험한 경계는 생성기 확률에 맡기지 않고 별도 테스트한다 |
| TST-005 | MUST | JUnit + Instancio가 자동 Shrinking·생성기 조합·상태 기반 속성 검증을 제공하는 전문 PBT 엔진과 기능적으로 동등하다고 주장하지 않는다 |
| TST-006 | MAY | 최소 반례 축소가 중요한 경우 별도 승인된 도구 또는 자체 축소 전략을 검토한다 |

## 7. Prose: AGENTS.md / 프로젝트 지침

AGENTS.md는 **자동화할 수 없는 맥락과 도구 사용법을 전달**하는 용도로 유지한다. 컴파일러/검사기가 이미 보장하는 규칙을 문장만으로 반복 강제하지 않는다.

### 7.1 최소 AI 작업 계약

| ID | 수준 | 규칙 |
|---|---|---|
| AGT-001 | MUST | 시작 전에 저장소/모듈 구조, 테스트·검사 스크립트, 기존 스타일과 활성 정책을 확인한다 |
| AGT-002 | MUST | 아키텍처 경계를 침범하거나 검증 실패를 제거하기 위해 무관한 의존성을 추가하지 않는다 |
| AGT-003 | SHOULD | 변경은 가능한 한 작고 목적 중심으로 유지한다 |
| AGT-004 | MUST | 오류 수정은 원인 분석 → 재현 테스트 → 수정 → 재검증 순서로 수행한다 |
| AGT-005 | MUST | 금지 규칙의 예외는 9.4 계약으로 표기한다. 어노테이션을 붙일 수 없는 대상은 PR에 동일 4요소를 기록한다 |
| AGT-006 | MUST | 새로운 정적 분석 도구를 도입하기 전에 기존 도구의 동일 기능 여부를 확인한다 |
| AGT-007 | MUST | 실제로 수행하지 않은 검증을 수행한 것처럼 쓰지 않는다 |
| AGT-008 | MUST | 불완전한 실행 환경에서는 **미검증**과 제한 사항을 명시한다 |
| AGT-009 | MUST | 작업을 마치면 변경 내용, 아키텍처 영향, 실행 명령, 결과, 남은 위험을 11.2 형식으로 요약한다 |

### 7.2 진단 전용/변경 허가 구분

- **AUDIT 모드**: 소스·설정 변경 금지. 정보 수집·안전한 조회·실행 가능한 검사만 수행.
- **APPLY 모드**: 승인된 범위에서 변경. 단계별 검사 수행. 보호 설정·예외 정책 완화는 사전 승인.
- **MIGRATE 모드**: 대량 변환은 AI 에이전트/IDE 또는 별도 일회성 마이그레이션 절차로 수행하며 dry-run·샘플 검증·롤백 준비 후 시행. OpenRewrite·Refaster는 기본 툴체인에서 제외한다.

## 8. 도구 매핑 및 책임 분리

| 도구 | 1차 책임 | 기본 전략 | 주의 |
|---|---|---|---|
| Java compiler + `-Xlint` | 타입/컴파일 경고 | CI 빌드 실패 | 모든 경고를 일괄 에러로 바꾸기 전 기존 부채 조사 |
| Gradle/Maven 모듈 경계 | 물리적 의존성 제약 | 허용 의존 목록 | 빌드 파일 변경으로 우회 가능하므로 리뷰 필요 |
| ArchUnit | 패키지·클래스·계층·순환 의존성, 프로젝트 규칙 기반 DTO `record`·생성자 주입·`@PolicyException` 만료 검증 | 아키텍처 및 구조 테스트 | 업무 의미 판별 한계, `SOURCE` 보존 어노테이션(Lombok, `@SuppressWarnings`) 검출 불가 |
| Error Prone | Java AST/타입 기반 오류 | 공식 검사 우선 | Java/JDK·플러그인 호환성 확인 |
| NullAway + JSpecify | Nullness 계약 | 선택적 도입 후 점진 확대 | 적용 패키지와 Annotation 일관성 중요 |
| PMD 7 | AST/디자인/커스텀 XPath 규칙 | 프로젝트 특화 정책 | XPath로 주석 검사 불가(STYLE-011은 Java 규칙으로 구현), 의미론적 보장 불가 |
| PMD Custom Rule | 주석 정책(`getComments()` 순회), 억제 짝 검사, Lombok·Nullness import, 로깅 형식, record 복사 등 사용자 정책 | 필요한 규칙만 구현 | 포맷·Import 정렬은 Spotless에 맡김 |
| Spotless | 포맷·Import 정리 | `spotlessCheck`로 CI 검사, `spotlessApply`는 명시적 수정 단계에서만 | 의미적 Lint로 오인하지 않음 |
| Secret Scanner | 하드코딩된 비밀정보 | 필수 보안 게이트 | 오탐·기밀 로그 노출 방지 |
| SCA | 의존성 취약점 | 필수 보안 게이트(SEC-015) | 전이 의존성 포함, 예외는 만료 기한 필수 |
| JUnit / Testcontainers | 실제 단위·연동 동작 | 재현성 있는 실행 | 격리/리소스 비용 |
| PIT | Mutation Testing | 핵심 로직·정기/조건부 | 비용이 큰 검증 |
| JUnit Jupiter | 매개변수화·반복·회귀·속성 유사 실행 검증 | 기본 테스트 플랫폼 | `@ParameterizedTest`, `@MethodSource`, `@RepeatedTest` 활용 |
| Instancio | 재현 가능한 무작위 객체·데이터 생성 | JUnit Jupiter와 조합 | Seed 관리 및 경계값 별도 테스트; 자동 Shrinking 제공을 전제하지 않음 |
| TLA+ 등 | 고위험 상태/동시성 모델 검증 | 선택적 심층 검증 | 모델과 구현 연결 필요 |
| Forgejo/GitHub CI | 게이트·증거·병합 차단 | 실제 실행 필수 | CI 파일 존재만으로 활성화 단정 금지 |

### 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책)

| ID | 수준 | 규칙 |
|---|---|---|
| TOOL-001 | MUST | **기본 필수 검사**: Java compiler (`-Xlint`), Gradle/Maven 의존성 정책, ArchUnit(계층형 프로젝트 및 프로젝트 규칙 기반 DTO `record` 검증), PMD 7, Error Prone + NullAway/JSpecify(호환성 검증 후), Spotless, JUnit Jupiter + Instancio, Secret Scanner, SCA. Testcontainers·PIT는 적용 대상에 따라 선택한다 |
| TOOL-002 | MUST | **제외**: OpenRewrite와 Refaster를 기본 구성·상시 CI·자동 수정 의존성에서 사용하지 않는다. 기존 코드에 남아 있으면 사용처 확인 후 별도 승인된 제거 작업으로 전환한다 |
| TOOL-003 | MUST | **기본 미도입**: Checkstyle·SpotBugs는 신규 프로젝트의 기본 검사기로 추가하지 않는다. 이 SPEC에 인용된 해당 도구의 탐지 사례는 *참고 패턴*이며 실제 감지를 보장하지 않는다. 고유 검사가 필요하면 PMD Custom Rule, Error Prone Custom BugChecker 또는 실행 테스트로 검증 가능한지 확인하고, 불가능하면 공백을 보고한다 |
| TOOL-004 | MUST | **자동 수정**: Spotless는 형식만 자동 수정한다. 코드 의미가 바뀌는 수정은 AI 에이전트가 작은 패치로 작성하고 필수 검증을 다시 통과해야 한다 |
| TOOL-005 | MUST | **성능**: Gradle 증분 빌드·캐시는 호환성 검증 후 적용하고, PMD 캐시는 입력·규칙 변경에 따른 무효화와 주기적 클린 분석을 확인한다. 검출을 빠르게 보이게 하려고 심각도나 검사 범위를 줄이지 않는다 |
| TOOL-006 | MUST | MUST·PROFILE-MUST 규칙에 매핑된 Error Prone 체크는 빌드 설정에서 `-Xep:<CheckName>:ERROR`로 명시한다. 기본 심각도(WARNING 또는 기본 비활성)에 의존하지 않는다. 경고 전체의 일괄 에러 승격은 기존 부채 조사 후 결정한다 |
| TOOL-007 | MUST | PMD·Error Prone·NullAway·ArchUnit·Instancio 버전을 version catalog 등으로 고정하고, 버전 변경 시 커스텀 규칙 테스트(9.3)를 재실행한다. 커스텀 PMD 규칙은 고정 버전의 API 문서를 기준으로 작성한다 |
| TOOL-008 | MUST | 위 구성은 단일 Gradle convention plugin(`build-logic`) 또는 Maven parent POM에서 관리한다. 모듈별 빌드 파일에서 검사 비활성화·심각도 하향·대상 제외를 금지한다(STYLE-005) |

## 9. 커스텀 Lint 작성 및 운영 SPEC

### 9.1 커스텀 규칙 도입 조건

새로운 규칙은 다음 질문에 모두 답할 수 있을 때 도입한다.

1. 어떤 **실제 결함** 또는 반복되는 문제를 막는가?
2. 적용할 파일/패키지/계층이 정확히 정의됐는가?
3. 허용할 정상 패턴과 예외가 정해졌는가?
4. 기존 도구에 같은 검사가 이미 존재하는가?
5. 정적 검사로 판별할 수 있는가, 실행 테스트가 필요한가?
6. 오탐 비용이 도입 효과보다 크지 않은가?
7. 실패 메시지에 **문제 원인 + 권장 수정**이 제시되는가?

### 9.2 규칙 정의 템플릿

```yaml
id: JAVA-CUSTOM-001
name: ExampleRule
status: proposed       # proposed | warning | enforced | deprecated
level: SHOULD          # MUST | SHOULD | PROFILE
scope:
  includes: ["src/main/java/**"]
  excludes: ["**/generated/**"]
condition: "검출 조건을 구현 가능한 형태로 기술"
why: "반복 결함과 위험 설명"
source: "공식 기술 근거(선택) 또는 작성자 정책 결정 근거"
detector: "ArchUnit | PMD | PMD Custom Rule | ErrorProne | CI"
positive_examples: ["정상 동작 샘플"]
negative_examples: ["위반 샘플"]
exceptions: "허용 사례 및 승인 절차"
remediation: "사용자가 취할 수정 방법"
ci_gate: false
owner: "규칙 관리 책임 팀"
```

### 9.3 승격 프로세스

`제안 → 샘플/오탐 테스트 → 경고 모드 → 기준선 확정 → 신규 위반 차단 → 기존 위반 단계적 제거 → 필수 CI 게이트`

- 새 규칙 적용으로 무관한 레거시 코드 전체를 즉시 실패시키지 않는다. Baseline/Changed-code 게이트를 검토한다.
- 규칙 자체에 **위반 샘플은 실패, 정상·예외 샘플은 성공**하는 테스트를 작성한다.
- 규칙 미준수 시 무조건 숨김 주석으로 회피하지 않는다. 승인된 Suppression에는 사유와 만료·재검토 시점을 포함한다.
- 도구 버전 변경 후 규칙 ID 변경·동작 변화·성능·오탐을 재검증한다.
- 파일·모듈 이름을 근거로 의미론적 비즈니스 규칙 준수를 확정하지 않는다.

### 9.4 검사 억제(Suppression) 표기 계약

억제는 다음 두 요소의 짝으로만 허용한다(STYLE-005).

1. 도구가 기본 인식하는 `@SuppressWarnings`: PMD는 `"PMD.<RuleName>"`, Error Prone은 체크명, javac는 lint 키.
2. 같은 선언에 부착한 프로젝트 어노테이션 `@PolicyException`.

| 속성 | 의미 |
|---|---|
| `rule` | 억제 대상 규칙(SPEC ID 또는 도구 체크명). `@SuppressWarnings`의 각 값과 1:1 대응 |
| `reason` | 결함이 아닌 이유 또는 수용한 위험 |
| `approvedBy` | 승인 주체 |
| `reviewBy` | 재검토 기한(ISO-8601 날짜) |

- **정의**: `@Retention(CLASS)`, `@Target({TYPE, METHOD, CONSTRUCTOR, FIELD})`. 복수 규칙은 `@Repeatable` 컨테이너로 표현한다. 지역 변수 어노테이션은 클래스 파일에 남지 않으므로 지역 변수 단위 억제는 허용하지 않고 둘러싼 메서드로 올린다.
- **짝 검사**: `@SuppressWarnings`는 `SOURCE` 보존이므로 PMD 커스텀 규칙으로 각 값에 대응하는 `@PolicyException` 존재를 검사한다.
- **만료 검사**: ArchUnit 테스트가 `reviewBy`가 지난 `@PolicyException`을 실패로 처리한다. 기한 연장은 재승인으로만 한다.
- **레거시 위반**: 억제 어노테이션을 대량 부착하지 않고 ArchUnit `FreezingArchRule`과 changed-code 게이트(9.3)로 관리한다.
- **어노테이션 불가 대상**: 빌드·CI 설정 등은 PR에 동일 4요소를 기록한다.

## 10. CI Gate SPEC

| 단계 | 트리거 | 필수 여부 | 예시 검증 |
|---|---|---|---|
| Fast Gate | 모든 PR | MUST | Compile, Format Check, ArchUnit, 정적 분석, Unit Test, Secret Scan, SCA |
| Integration Gate | 인프라/DB/프로토콜 변경 | 관련 변경 시 MUST | Testcontainers, Conformance Test |
| Deep Gate | 핵심 로직·일정·릴리스 | 조건부 | PIT, Property, E2E, 보안 테스트 |
| Release Gate | 배포 전 | 프로젝트 지정 | 버전 호환성·증거·운영 설정 점검 |

SCA는 PR 외에 정기 실행으로 신규 공개 취약점을 추적한다. 병합 규칙은 **필수 검증이 실제 성공했는지** 확인해야 하며, 성공한 척하는 빈 Job이나 무조건 허용하는 설정을 사용하지 않는다. 자동 병합은 신뢰할 만한 Inner Loop와 보호 정책이 구축된 후 **낮은 위험의 변경 유형부터** 적용한다.

### 10.1 검증 실패시 AI의 행동

1. 오류 메시지와 실패 테스트를 보존한다.
2. 재현 여부를 확인하고 근본 원인을 구분한다.
3. 제품 결함, 테스트 결함, 환경 결함 중 어느 유형인지 근거를 제시한다.
4. 구현을 최소 범위에서 수정하고 회귀 테스트를 보강한다.
5. 같은 실패가 다시 생길 수 있다면 API/아키텍처/정적 검사 강화 가능성을 제안한다.
6. 같은 검증을 재실행하고 결과를 보고한다.
7. 해결하지 못했으면 실패·제약·미검증 사항을 숨지 않는다.

## 11. 적용 순서 및 산출물 계약

### 11.1 적용 순서

**Phase A — Inventory/Audit**
- JDK, Gradle/Maven, 모듈 그래프, 프레임워크, 언어 기능, 테스트 소스셋, CI 워크플로 파악.
- 기존 PMD/Error Prone/NullAway/ArchUnit/Spotless/보안 도구 현황 조사. 레거시 SpotBugs/Checkstyle가 있는 경우 대체되지 않은 규칙을 파악한다.
- 규칙별 적용 가능성·검증 증거·예외·오탐을 식별.

**Phase B — Architecture First**
- 물리적 모듈 의존성·순환 의존·도메인 기술 의존부터 차단.
- 오용 가능한 API·불변 상태·공유 상태의 근본적 설계 개선 검토.

**Phase C — Static & Security Gates**
- 기존 도구를 우선 구성하고 커스텀 Lint는 빈틈에만 추가.
- 신규 위반 차단과 기존 위반 정리 계획을 분리.

**Phase D — Runtime & Deep Verification**
- 테스트 재현성·실제 시스템 경로·프로토콜 검증 강화.
- Mutation·속성·형식 검증은 고위험 영역부터 단계적 적용.

**Phase E — Agent Workflow & Regression Loop**
- AI 에이전트가 로컬 및 CI에서 동일한 검증 명령 사용.
- PR에 검증 증거 첨부, 실패 시 수정·재검증 반복.
- Collect → Compare → Diagnose → Prevent → Verify → Promote 회귀 방지 루프를 적용하고, Inner Loop 안정화 후 제한적 자동 병합을 확대한다.

### 11.2 AI 결과 보고 형식

```markdown
# Java Quality SPEC 적용 결과

## 프로젝트 현황
- JDK / 빌드 / 모듈 / 프레임워크 / CI:

## 적용성 판정
| 규칙 ID | 적용/미적용/예외/미확인 | 근거 파일:라인 | 기존 검증 | 보완 조치 |
|---|---|---|---|---|

## 실제 발견 사항
| 심각도 | 규칙 ID | 파일:라인 | 재현/검출 결과 | 수정 권고 |
|---|---|---|---|---|

## 수행한 검사
| 명령 | 실행 여부 | 결과 | 로그 또는 증거 |
|---|---|---|---|

## 검증 공백·오탐·예외
- ...

## 단계별 적용 제안
- 아키텍처/타입 설계 → 정적 검사 → 실행 검증 → 문서

## PR 요약(변경 허용 시)
- 변경 이유 및 영향
- 테스트/검증 결과
- 남은 위험과 미검증 항목
```

## 12. 공식 근거 자료 및 추적 링크

아래 링크는 **원문 규칙의 근거 카탈로그**다. 각 도구의 규칙은 버전에 따라 이동·변경될 수 있다. 적용 시 사용 중인 버전의 문서를 우선한다.

| 코드 | 기관/공식 자료 | 주소 |
|---|---|---|
| [ORACLE] | Oracle Secure Coding Guidelines for Java SE | https://www.oracle.com/java/technologies/javase/seccodeguide.html |
| [CERT] | SEI CERT Oracle Coding Standard for Java | https://cmu-sei.github.io/secure-coding-standards/sei-cert-oracle-coding-standard-for-java/ |
| [CWE] | MITRE Common Weakness Enumeration | https://cwe.mitre.org/ |
| [OWASP] | OWASP Secure Coding Practices | https://owasp.org/www-project-secure-coding-practices-quick-reference-guide/ |
| [OWASP-SQL] | OWASP SQL Injection Prevention | https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html |
| [EP] | Google Error Prone Bug Patterns | https://errorprone.info/bugpatterns |
| [SB] | SpotBugs Bug Descriptions | https://spotbugs.readthedocs.io/en/stable/bugDescriptions.html |
| [PMD] | PMD Java Rules | https://docs.pmd-code.org/latest/pmd_rules_java.html |
| [GSTYLE] | Google Java Style Guide | https://google.github.io/styleguide/javaguide.html |
| [JDK25] | Oracle Java SE 25 Documentation | https://docs.oracle.com/en/java/javase/25/ |
| [VTHREAD] | Java 25 Virtual Threads | https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html |
| [SPRING] | Spring Framework Reference | https://docs.spring.io/spring-framework/reference/ |
| [MYBATIS] | MyBatis 3 Mapper XML | https://mybatis.org/mybatis-3/sqlmap-xml.html |
| [ARCH] | ArchUnit User Guide | https://www.archunit.org/userguide/html/000_Index.html |
| [NULLAWAY] | NullAway | https://github.com/uber/NullAway |
| [PIT] | PIT Mutation Testing | https://pitest.org/ |
| [JUNIT] | JUnit Jupiter User Guide | https://docs.junit.org/current/user-guide/ |
| [INSTANCIO] | Instancio User Guide | https://www.instancio.org/user-guide/ |
| [JSPECIFY] | JSpecify User Guide | https://jspecify.dev/docs/user-guide/ |
| [MODULITH] | Spring Modulith Reference | https://docs.spring.io/spring-modulith/reference/ |
| [JMOLECULES] | jMolecules Integrations (ArchUnit) | https://github.com/xmolecules/jmolecules-integrations |
| [GRADLE-DV] | Gradle Dependency Verification | https://docs.gradle.org/current/userguide/dependency_verification.html |
| [CWE-1395] | CWE-1395 Dependency on Vulnerable Third-Party Component | https://cwe.mitre.org/data/definitions/1395.html |

### 12.1 작성자 강제 정책과 외부 표준의 구분

- `DTO record 적용(프로젝트 패키지·네이밍 규칙에 따른 유연 적용 및 강제 방법론 적용)`, `Lombok 금지`, `일반 구현 주석 금지`, `주석 기반 억제 금지 및 @PolicyException 짝 억제`, `record 방어적 복사·배열 컴포넌트 금지`, `JSpecify 단일화`, `시스템 시계 직접 호출 금지`, `SLF4J 파라미터화 로깅`은 **업계 보편 의무가 아니지만 본 SPEC 작성자가 의도적으로 강제하는 더 엄격한 정책**이다. 명시된 범위와 예외에 따라 검사·차단한다. `Setter 전면 금지`, `모든 catch(Exception) 금지`, `모든 Thread.sleep 금지`는 이 문서가 일괄 강제하지 않는 규칙이므로 개별 적용 조건을 따른다.
- `Domain은 프레임워크 독립`, `Application은 Adapter에 의존 금지`는 **해당 아키텍처를 채택했다면 작성자가 강제하는 규칙**이다.
- Spring Constructor Injection은 일반적 권고이자 불변성·순환참조 방지·테스트 용이성을 위한 핵심 설계 기준이지만, 선택적 의존성이나 재설정이 필요한 제한적 경우의 모든 Setter Injection이 오류인 것은 아니다.
- Java 24(JEP 491) 이후 가상 스레드의 `synchronized` 고정(Pinning) 문제가 해소되었으므로 규칙 설계자는 `synchronized` 일괄 금지 규칙을 만들지 않는다(구 CON-010).
- PMD/Error Prone/NullAway는 규칙별 활성 상태·심각도·오탐이 다르다. SpotBugs는 기존 규칙의 출처 참고로만 남긴다.
- 코드 커버리지, Mutation Score, PR 되돌림 비율은 단독으로 코드의 완전한 정확성을 보증하지 않는다.

### 12.2 회귀 방지 참고 출처

- ArchUnit FreezingArchRule / ViolationStore: https://www.archunit.org/userguide/html/000_Index.html
- PMD 7 Incremental Analysis: https://docs.pmd-code.org/pmd-doc-7.29.0/pmd_userdocs_incremental_analysis.html
- PMD 7 CLI cache: https://docs.pmd-code.org/pmd-doc-7.29.0/pmd_userdocs_cli_reference.html
- OASIS SARIF 2.1.0: https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.pdf
- Gradle Build Cache: https://docs.gradle.org/current/userguide/build_cache.html

## 13. 최종 수용 기준 (Definition of Done)

프로젝트가 이 SPEC을 수용했다고 보고하려면 다음 조건을 충족해야 한다.

- [ ] 프로젝트 기술 스택과 적용 프로파일을 확정했다.
- [ ] 각 규칙에 적용 여부·근거·예외·검증 담당을 기록했다.
- [ ] 아키텍처/타입 설계로 예방할 수 있는 문제부터 처리했다.
- [ ] 실제 작동하는 컴파일/정적 분석/테스트 CI 게이트가 존재한다.
- [ ] 새로운 커스텀 규칙에 정상·위반·예외 테스트가 있다.
- [ ] MUST 규칙에 매핑된 검사가 ERROR 심각도로 빌드를 실패시킴을 위반 샘플로 확인했다.
- [ ] 모든 검사 억제가 `@SuppressWarnings` + `@PolicyException` 짝이며 만료된 예외가 없다.
- [ ] 외부 자료에 근거한 일반 권고와 작성자의 추가 강제 정책을 구분했으며, 적용 대상에는 작성자의 강제 요구사항이 작동한다.
- [ ] 실행 검증의 커밋별 증거를 확인할 수 있다.
- [ ] 실패 원인 분석과 재발 방지 루프가 CI·PR에서 실제 동작한다.
- [ ] 결함 수정마다 재현 가능한 회귀 테스트 또는 정적 방어 장치를 확인했다.
- [ ] 신규 위반 Baseline 비교와 무단 Baseline 재생성 방지가 적용돼 있다.
- [ ] 신규 Lint 강제 승격은 위반/정상/예외 검증 및 승인 후 수행한다.
- [ ] 문서만 늘리지 않고 자동화 가능한 규칙은 실제 검사로 옮겼다.
- [ ] 미검증·도입 보류·예외 사항을 명확히 보고한다.

---

**최종 원칙:** 이 SPEC은 업계 표준을 수동적으로 요약한 문서가 아니라, 작성자가 더 엄격한 개발 품질을 확보하기 위해 정의한 **강제 정책**이다. AI는 이를 임의로 완화하지 말고, 잘못된 코드가 작성·병합되기 어려운 구조와 검증 시스템으로 실현해야 한다.