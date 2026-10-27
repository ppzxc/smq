# Maven Central 공개 요건과 Gradle 공개 경로

- 조사일: 2026-10-27
- 관련 문서: [MAP #84](https://github.com/ppzxc/smq/issues/84), [RESEARCH #85](https://github.com/ppzxc/smq/issues/85), 저장소 `build.gradle.kts`
- 범위: 저장소 코드는 건드리지 않는다. 이 문서는 조사 결과만 담는다. 모든 주장에 1차 출처를 붙였고, 확인하지 못한 것은 "확인 못함"으로 적었다. "실험"으로 표시한 항목은 저장소 사본(scratchpad)에서 실제로 빌드해 확인한 결과다(Gradle 9.8.2, JDK 25).

## 요약(결론 5줄 이내)

1. `io.github.ppzxc` 네임스페이스는 GitHub 계정 `ppzxc`로 Central Portal에 가입하면 자동으로 검증된다. 다른 방법으로 가입했다면 검증 키 이름의 공개 저장소를 만들어 증명한다.
2. 요건: 모든 파일에 `.asc` PGP 서명(공개키는 keyserver.ubuntu.com, keys.openpgp.org, pgp.mit.edu 중 하나에 올림), `.md5`와 `.sha1` 체크섬, sources/javadoc jar, POM의 `name`, `description`, `url`, `licenses`, `developers`, `scm(connection, developerConnection, url)`. 버전은 `-SNAPSHOT`이면 안 된다.
3. 가장 단순한 경로는 vanniktech `com.vanniktech.maven.publish` 0.37.0이다. 현재 `build.gradle.kts`에서 플러그인 한 줄, `publishing {}` 블록을 `mavenPublishing {}`(pom 메타데이터 포함)으로 바꾸는 일, 버전 변경만 하면 된다. 실험으로 POM, `.module`, 서명이 정상 생성되는 것을 확인했다.
4. `USER_MANAGED`(기본) 배포는 검증을 통과한 `VALIDATED` 상태에서 Bearer 토큰을 붙인 `https://central.sonatype.com/api/v1/publisher/deployments/download/` 저장소로 소비자 프로젝트가 의존성으로 받아 볼 수 있다. 공개 전에는 drop할 수 있다. 공개(PUBLISHED) 뒤에는 바꿀 수 없고, 그 버전 번호는 다시 쓸 수 없다.
5. CI 비밀값은 `ORG_GRADLE_PROJECT_mavenCentralUsername`, `mavenCentralPassword`, `signingInMemoryKey`(armored 전체), `signingInMemoryKeyPassword` 네 개다. GitHub Environment secret에 두고, 태그 push나 `workflow_dispatch`로만 실행한다.

권고: vanniktech 플러그인 + `publishToMavenCentral()`(자동 공개 끔). 업로드 → VALIDATED에서 외부 소비자 테스트(Linux/Windows) → 사람이 Portal에서 Publish를 누르는 순서로 0.0.1을 낸다.

---

## 1. 네임스페이스 `io.github.ppzxc` 검증

| 사실 | 출처 |
|---|---|
| GitHub 계정으로 가입하면 `io.github.<username>` 네임스페이스가 자동 검증된다 | [register/namespace](https://central.sonatype.org/register/namespace/): "if you sign up with a GitHub account, you will automatically receive a verified namespace in the form `io.github.<github username>`" |
| 자동 생성은 가입한 사용자 이름만 대상이다. 조직 이름은 대상이 아니다 | 같은 문서 |
| GitHub 로그인으로 가입했는데 네임스페이스가 없으면 Central Support에 메일을 보낸다 | 같은 문서 |
| GitHub 외 방법으로 가입했다면 Portal이 준 Verification Key 이름으로 공개 저장소를 만들어 소유권을 증명하고, 검증 뒤 그 저장소는 지워도 된다 | 같은 문서("For Code Hosting Services with Personal groupId"). 문서 예시는 GitLab이고 GitHub도 같은 방식이라는 것은 추론이다 |

smq의 `group = "io.github.ppzxc"`는 이미 이 형태와 일치한다. 가입 방법: https://central.sonatype.com 에서 "Sign in with GitHub".

## 2. 서명과 체크섬

| 요건 | 출처 |
|---|---|
| 배포하는 모든 파일에 GPG/PGP 서명(`.asc`)이 있어야 한다 | [requirements](https://central.sonatype.org/publish/requirements/): "All files deployed need to be signed with GPG/PGP" |
| `.md5`, `.sha1` 필수. `.sha256`, `.sha512`는 선택. `.asc`에는 체크섬이 필요 없다 | 같은 문서: "`.md5` and `.sha1` are required", "`.sha256` and `.sha512` are supported but not mandatory" |
| 공개키는 지원 키서버 중 하나에 올린다: keyserver.ubuntu.com, keys.openpgp.org, pgp.mit.edu | [requirements/gpg](https://central.sonatype.org/publish/requirements/gpg/) |
| 서명은 primary key로 한다. sub key로 서명하면 문제가 된다 | 같은 문서: "Sub keys are a problem if you use them to sign and deploy artifacts to the Central Repository." |
| 키 종류는 명시되어 있지 않다. 문서 예시는 RSA 3072 | 같은 문서 |
| 키 기본 유효기간은 2년. 만료 전에 `gpg --edit-key` → `expire`로 연장하고 공개키를 다시 올린다 | 같은 문서 |

권고: RSA 3072 이상, 서명 전용 primary key 하나로 만든다(`gpg --quick-gen-key "<이름> <메일>" rsa3072 sign 2y`). 최신 gpg 기본값은 ed25519다. Gradle 서명(BouncyCastle)이 ed25519 키를 처리하는지와 Central 검증기가 받아 주는지는 확인 못함이다. 그래서 문서 예시와 같은 RSA를 쓴다.

실험: RSA 3072 임시 키를 `gpg --export-secret-keys --armor`로 내보내 `ORG_GRADLE_PROJECT_signingInMemoryKey`로 넘기자 jar, sources, javadoc, pom, module 다섯 파일에 `.asc`가 생겼다. `gpg --verify`는 "Good signature"였다.

체크섬은 직접 만들 필요가 없다. vanniktech 플러그인은 Gradle이 만든 체크섬 중 md5와 sha1만 업로드한다([central 문서](https://vanniktech.github.io/gradle-maven-publish-plugin/central/), [CHANGELOG 0.37.0](https://github.com/vanniktech/gradle-maven-publish-plugin/blob/main/CHANGELOG.md): "redundant checksum files are now excluded by default").

## 3. POM 필수 항목과 sources/javadoc jar

| 요건 | 출처 |
|---|---|
| `groupId`, `artifactId`, `version` 필수. 버전은 `-SNAPSHOT`으로 끝나면 안 된다 | [requirements](https://central.sonatype.org/publish/requirements/) |
| `name`, `description`, `url` 필수 | 같은 문서 |
| `licenses`, `developers` 필수 | 같은 문서 |
| `scm`의 `connection`, `developerConnection`, `url` 필수. URL은 공개일 필요가 없다 | 같은 문서: "the URL itself does not need to be public" |
| jar 패키징이면 `-sources.jar`, `-javadoc.jar` 필수. 없으면 빈 placeholder jar도 허용 | 같은 문서: "we allow uploading placeholder `.jar` files in order to pass validation" |
| `<repositories>` 선언은 권장하지 않는다 | 같은 문서 |

현재 빌드는 `withSourcesJar()`, `withJavadocJar()`로 두 jar를 이미 만든다. `pom {}` 메타데이터는 전혀 없어서 지금 상태로는 검증에 실패한다. 라이선스는 저장소 `LICENSE`가 MIT다.

실험: 현재 빌드의 `./gradlew javadoc`은 `module-info`가 있는 Java 25에서 성공한다(누락 주석 경고 100개, 오류 없음). 그래서 실제 javadoc jar를 낼 수 있다.

참고: 생성된 POM에서 `sqlite-jdbc`는 `implementation`이므로 `<scope>runtime</scope>`이다(실험으로 확인). 공개 API가 sqlite-jdbc 타입을 노출하지 않는다면 맞는 설정이다.

## 4. Gradle 공개 경로 비교

배경: Sonatype 공식 Gradle 플러그인은 없다. [publish-portal-gradle](https://central.sonatype.org/publish/publish-portal-gradle/): "Currently, there is no official Gradle plugin for publishing to Maven Central via the Central Publishing Portal." 이 문서는 JReleaser를 자세히 설명하고, vanniktech, GradleUp/nmcp 등 커뮤니티 플러그인을 "NOT SUPPORTED by Sonatype"이라는 단서를 붙여 나열한다. OSSRH는 종료되었다([vanniktech CHANGELOG 0.34.0](https://github.com/vanniktech/gradle-maven-publish-plugin/blob/main/CHANGELOG.md): "Sonatype OSSRH since it has been shut down").

| 경로 | 하는 일 | 현재 빌드에서 바꿀 양 | 비고 |
|---|---|---|---|
| A. Publisher API 직접 호출 | `maven-publish`로 로컬 디렉터리에 출판하고, `signing` 플러그인으로 서명하고, md5/sha1만 남겨 zip으로 묶은 뒤 `curl`로 `POST /api/v1/publisher/upload` | `signing` 플러그인, `pom {}`, 로컬 repo, zip 작업, 업로드 스크립트(상태 폴링 포함) | 의존성은 없지만 직접 짜는 코드가 가장 많다. 상태 폴링과 오류 처리도 직접 해야 한다. 문서: [publish-portal-api](https://central.sonatype.org/publish/publish-portal-api/) |
| B. vanniktech `com.vanniktech.maven.publish` 0.37.0 | Portal 업로드, 서명, 체크섬 선별, 검증 대기(`VALIDATED`)를 플러그인이 한다 | 플러그인 1줄, `publishing {}` → `mavenPublishing {}`, `pom {}`, version | 최소 Gradle 9.0, JDK 17. 테스트된 Gradle은 9.6.0과 9.7.0-milestone-1까지다([CHANGELOG](https://github.com/vanniktech/gradle-maven-publish-plugin/blob/main/CHANGELOG.md)). 저장소의 9.8.2에서는 로컬 출판과 서명까지만 실험으로 확인했고, 실제 Portal 업로드는 확인 못함 |
| C. JReleaser 1.26.0 | `maven-publish`로 `build/staging-deploy`에 출판하고, `jreleaserDeploy`가 서명, 체크섬, POM 규칙 검사, 업로드를 한다 | `pom {}`, staging repo, `jreleaser {}` 블록(deploy, signing), `JRELEASER_*` 비밀값 | GitHub Release 생성 같은 기능이 더 있지만 0.0.1에는 과하다. [JReleaser 예시](https://jreleaser.org/guide/latest/examples/maven/maven-central.html) |
| D. GradleUp nmcp 1.6.2 | `maven-publish` 출판물을 묶어 Portal에 올린다 | `signing` 플러그인 설정 + `pom {}` + nmcp 설정 | 서명은 직접 구성한다. 상세 비교는 하지 않았다(확인 못함) |

버전 최신값은 GitHub releases 기준이다(2026-10-27 조회: vanniktech 0.37.0, JReleaser v1.26.0, nmcp v1.6.2).

### 4.1 경로 B의 실제 변경(실험으로 확인)

플러그인이 `JavaLibrary`를 구성하면 `maven`이라는 이름의 publication을 직접 만든다(소스 [Platform.kt 0.37.0](https://github.com/vanniktech/gradle-maven-publish-plugin/blob/0.37.0/plugin/src/main/kotlin/com/vanniktech/maven/publish/Platform.kt): `PUBLICATION_NAME = "maven"`, `publications.create(PUBLICATION_NAME, ...)`). 그래서 기존 `publishing { publications { create<MavenPublication>("maven") ... } }` 블록은 지워야 한다. 두면 이름이 겹친다(추론). `SourcesJar.Sources()`는 내부에서 `java.withSourcesJar()`를 부른다. `JavadocJar.Javadoc()`은 `plainJavadocJar` 작업을 따로 만든다([JavadocJar.kt](https://github.com/vanniktech/gradle-maven-publish-plugin/blob/0.37.0/plugin/src/main/kotlin/com/vanniktech/maven/publish/tasks/JavadocJar.kt)). `JavaLibrary`의 javadoc 기본값은 `JavadocJar.Empty()`다. 기존 `withJavadocJar()`를 유지하려면 `JavadocJar.None()`을 주어야 javadoc jar가 두 개 붙지 않는다.

```kotlin
import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SourcesJar

plugins {
    `java-library`
    id("com.vanniktech.maven.publish") version "0.37.0"   // maven-publish, signing을 함께 적용한다
}

version = "0.0.1"

// java { withSourcesJar(); withJavadocJar() }는 그대로 둔다.

mavenPublishing {
    configure(JavaLibrary(javadocJar = JavadocJar.None(), sourcesJar = SourcesJar.Sources()))
    publishToMavenCentral()          // automaticRelease 기본 false → USER_MANAGED
    signAllPublications()
    coordinates("io.github.ppzxc", "smq", version.toString())
    pom {
        name = "smq"
        description = "..."
        url = "https://github.com/ppzxc/smq"
        licenses { license { name = "MIT License"; url = "https://opensource.org/licenses/MIT" } }
        developers { developer { id = "ppzxc"; name = "ppzxc"; url = "https://github.com/ppzxc" } }
        scm {
            url = "https://github.com/ppzxc/smq"
            connection = "scm:git:https://github.com/ppzxc/smq.git"
            developerConnection = "scm:git:ssh://git@github.com/ppzxc/smq.git"
        }
    }
}
// 기존 publishing { ... } 블록은 삭제한다.
```

실험 결과(scratchpad 사본, `publishToMavenLocal -x test`):
- 산출물: `smq-0.0.1.jar`, `-sources.jar`, `-javadoc.jar`, `.pom`, `.module`. 서명 키를 주면 각각 `.asc`도 생긴다.
- POM에 name, description, url, licenses, developers, scm 세 항목이 모두 들어갔다.
- `.module`에 `apiElements`, `runtimeElements`, `sourcesElements`, `javadocElements` 네 변형이 있다.
- `verifyJarExcludesBenchmark`가 참조하는 `sourcesJar` 작업은 그대로 남는다.

`signAllPublications()`를 무조건 켜면 키 없이 `publishToMavenLocal`을 돌릴 때 서명 작업이 실패할 수 있다(확인 못함). 실험에서는 `signingInMemoryKey` 속성이 있을 때만 켜는 조건을 썼다. `check`는 서명을 부르지 않는다.

작업: `./gradlew publishToMavenCentral`은 업로드만 하고 Portal에서 손으로 공개한다. `./gradlew publishAndReleaseToMavenCentral`은 업로드 뒤 곧바로 공개한다([central 문서](https://vanniktech.github.io/gradle-maven-publish-plugin/central/)). 0.36.0부터 업로드 작업은 기본으로 `VALIDATED` 상태가 될 때까지 기다린다(CHANGELOG: "The default is now to just wait for the `VALIDATED` state.").

## 5. 공개 전 스테이징 소비와 되돌리기

### 5.1 배포 상태와 publishingType

| 사실 | 출처 |
|---|---|
| `USER_MANAGED`(기본): 검증 뒤 사용자가 Portal UI에서 직접 공개해야 한다. `AUTOMATIC`: 검증을 통과하면 자동으로 공개된다 | [publish-portal-api](https://central.sonatype.org/publish/publish-portal-api/) |
| 상태: PENDING → VALIDATING → VALIDATED(사용자 공개 대기) → PUBLISHING → PUBLISHED, 또는 FAILED | 같은 문서 |
| `VALIDATED`나 `FAILED` 배포는 `DELETE /api/v1/publisher/deployment/<id>`로 drop할 수 있다 | 같은 문서 |
| Deployments 탭의 배포는 90일 뒤 정리된다 | [publish-portal-guide](https://central.sonatype.org/publish/publish-portal-guide/), [FAQ](https://central.sonatype.org/faq/what-happened-to-my-deployments/) |

### 5.2 VALIDATED 배포를 소비자 프로젝트에서 받기: 가능

[publish-portal-api](https://central.sonatype.org/publish/publish-portal-api/)의 manual testing 절: "Once a bundle has been validated it can be referenced as a dependency in your build tool of choice."

- `/api/v1/publisher/deployments/download/<relativePath>`는 요청한 파일이 들어 있는 검증된 배포 아무 것에서나 받는다. `/api/v1/publisher/deployment/<deploymentId>/download/<relativePath>`는 특정 배포 하나에서 받는다.
- 인증은 Portal user token을 `base64(username:password)`로 만든 `Authorization: Bearer <값>` 헤더다.
- 문서의 Gradle 예시:

```kotlin
repositories {
    maven {
        name = "centralManualTesting"
        url = uri("https://central.sonatype.com/api/v1/publisher/deployments/download/")
        credentials(HttpHeaderCredentials::class)
        authentication { create<HttpHeaderAuthentication>("header") }
    }
    mavenCentral()
}
// gradle.properties(또는 ORG_GRADLE_PROJECT_ 환경 변수)
// centralManualTestingAuthHeaderName=Authorization
// centralManualTestingAuthHeaderValue=Bearer <base64 토큰>
```

(문서 원문은 Groovy DSL이다. 위는 Kotlin DSL로 옮긴 것이다.)

한계:
- `AUTOMATIC`(`publishAndReleaseToMavenCentral`)은 VALIDATED에서 멈추지 않으므로 이 테스트를 할 틈이 없다. 공개 전 배포 테스트를 하려면 `publishToMavenCentral()`(USER_MANAGED)을 써야 한다.
- 소비자 빌드에도 Bearer 토큰이 필요하다. 그래서 CI의 배포 테스트 잡(Linux/Windows)도 Portal 토큰 비밀값을 받아야 한다.
- 이 저장소가 Gradle module metadata(`.module`)를 정상으로 내려주는지는 문서에 없고 실험하지 않았다(확인 못함). 안 된다면 Gradle이 POM으로 대신 해석한다.
- VALIDATED 배포가 90일보다 먼저 정리되는지는 문서에 없다(확인 못함).

### 5.3 공개 후 되돌리기: 불가

[FAQ can-i-change-a-component](https://central.sonatype.org/faq/can-i-change-a-component/): "Once a component has been released and published to the Central Repository, it cannot be altered." 잘못된 것은 새 버전으로 고친다. 그래서 `0.0.1`은 한 번 공개하면 그 번호를 영원히 쓴다. 공개 전 VALIDATED 단계에서는 drop하고 같은 버전으로 다시 올릴 수 있다(drop 가능은 5.1 출처. 같은 버전 재업로드가 된다는 것은 추론이다).

### 5.4 SNAPSHOT(참고)

네임스페이스 설정에서 "Enable SNAPSHOTs"를 켜면 `https://central.sonatype.com/repository/maven-snapshots/`로 SNAPSHOT을 출판하고 받을 수 있다. 90일 뒤 정리된다([publish-portal-snapshots](https://central.sonatype.org/publish/publish-portal-snapshots/)). 공개 전 배포 테스트는 5.2로 충분하므로 0.0.1에 필수는 아니다.

## 6. CI(GitHub Actions) 비밀값

| 항목 | 내용 | 출처 |
|---|---|---|
| Portal 자격 | 로그인 비밀번호가 아니라 user token(username/password 쌍)을 쓴다. https://central.sonatype.com/usertoken 에서 만들고, 만들 때 만료를 정한다. 창을 닫으면 다시 볼 수 없다 | [generate-portal-token](https://central.sonatype.org/publish/generate-portal-token/): "Tokens cannot be retrieved once the modal closes." |
| vanniktech 속성 | `mavenCentralUsername`, `mavenCentralPassword`, `signingInMemoryKey`, `signingInMemoryKeyId`(선택), `signingInMemoryKeyPassword`. CI에서는 `ORG_GRADLE_PROJECT_` 접두사를 붙인 환경 변수로 넘긴다 | [vanniktech central](https://vanniktech.github.io/gradle-maven-publish-plugin/central/) |
| 서명 키 형식 | `gpg --export-secret-keys --armor <key id>` 출력 전체(BEGIN/END 줄 포함, 여러 줄)를 그대로 넣는다 | 같은 문서: "Make sure to copy this string in its entirety." |
| fork PR | fork에서 시작한 워크플로에는 secret이 전달되지 않는다 | [GitHub Docs: use secrets](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets): "secrets are not passed to the runner when a workflow is triggered from a forked repository" |
| 마스킹 | secret이 아닌 민감값은 `::add-mask::`로 가린다 | 같은 문서 |
| Environment secret | 개인 계정 저장소에서 environment secret은 저장소 소유자가 만든다 | 같은 문서 |

권고 구성(문서들을 조합한 설계 제안이다):
- 공개 워크플로는 `push: tags: ['v*']` 또는 `workflow_dispatch`로만 실행한다. `pull_request`로는 실행하지 않는다.
- 네 비밀값은 `release` 같은 GitHub Environment에 두고, 공개 잡만 그 environment를 쓴다. environment의 required reviewers 같은 보호 규칙은 이 조사에서 문서를 확인하지 않았다(확인 못함).
- 배포 테스트 잡에는 Portal 토큰으로 만든 Bearer 값만 준다. 서명 키는 주지 않는다.
- 업로드 잡은 Linux 러너 하나에서 돌린다. 산출물이 플랫폼 독립이기 때문이다.
- 토큰 만료와 GPG 키 만료(기본 2년)를 기록해 두고 갱신한다.

## 7. 0.0.1까지 사람이 할 일(체크리스트)

1. central.sonatype.com에 GitHub `ppzxc`로 가입하고 Namespaces에서 `io.github.ppzxc`가 Verified인지 확인한다.
2. RSA 3072 서명 키를 만들고 공개키를 keyserver.ubuntu.com(그리고 keys.openpgp.org)에 올린다. 폐기 인증서는 따로 보관한다.
3. Portal user token을 만든다.
4. GitHub Environment에 비밀값 네 개를 등록한다.
5. `build.gradle.kts`에 4.1의 변경을 넣는다.
6. `publishToMavenCentral`로 업로드 → VALIDATED에서 외부 소비자 테스트 → Portal에서 Publish를 누른다. 실패하면 drop하고 다시 올린다.

## 확인 못함 목록

- vanniktech 0.37.0의 Gradle 9.8.2에서의 실제 Portal 업로드(로컬 출판과 서명은 확인)
- ed25519 키가 Gradle 서명과 Central 검증에서 되는지
- `/deployments/download/`가 `.module` 파일도 내려주는지
- VALIDATED 배포의 보존 기간이 90일보다 짧은지
- drop 뒤 같은 버전을 다시 올릴 수 있는지(가능하다고 추론)
- nmcp 경로의 상세 설정
- GitHub Environment 보호 규칙(required reviewers)이 개인 저장소에서 되는지
