import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.SourcesJar
import java.util.zip.ZipFile
import org.gradle.plugins.signing.SigningExtension
import org.gradle.api.tasks.options.Option
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway

plugins {
    `java-library`
    jacoco
    id("com.vanniktech.maven.publish") version "0.37.0"
    id("net.ltgt.errorprone") version "5.1.1"
    id("net.ltgt.nullaway") version "3.3.0"
    id("com.diffplug.spotless") version "8.10.4"
}

group = "io.github.ppzxc"
// 버전은 하드코딩한다. main은 항상 다음 패치-SNAPSHOT이고, 릴리스 PR에서만 바꾼다.
version = "0.0.2-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

// 벤치 하네스. 배포 jar에는 들어가지 않고, main 출력과 sqlite-jdbc에만 의존한다.
val benchmark: SourceSet = sourceSets.create("benchmark") {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

configurations[benchmark.implementationConfigurationName].extendsFrom(configurations.implementation.get())
configurations[benchmark.runtimeOnlyConfigurationName].extendsFrom(configurations.runtimeOnly.get())

// 러너·집계·비교기 같은 순수 로직은 test 소스셋에서 단위 테스트한다.
sourceSets.test {
    compileClasspath += benchmark.output
    runtimeClasspath += benchmark.output
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.xerial:sqlite-jdbc:3.53.4.0")
    compileOnlyApi("org.jspecify:jspecify:1.0.1")

    errorprone("com.google.errorprone:error_prone_core:2.51.0")
    errorprone("com.uber.nullaway:nullaway:0.14.3")

    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    val isMainCompilation = name == "compileJava"
    options.encoding = "UTF-8"
    options.release = 25
    options.errorprone {
        nullaway {
            onlyNullMarked.set(true)
            if (isMainCompilation) {
                error()
                requireExplicitNullMarking { error() }
            } else {
                disable()
            }
        }
        if (!isMainCompilation) {
            // 테스트 시나리오의 한국어 식별자는 명세 역할을 하므로 경고로만 보고한다.
            warn("UnicodeInCode")
        }
    }
    if (isMainCompilation) {
        options.compilerArgs.add("-Werror")
    }
}

spotless {
    java {
        target("src/*/java/**/*.java", "deploy-test/**/src/**/*.java")
        palantirJavaFormat("2.103.0")
    }
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:all,-missing", "-quiet")
        addBooleanOption("Werror", true)
        encoding = "UTF-8"
    }
}

val minimumCoverage = "0.90".toBigDecimal()

jacoco {
    toolVersion = "0.8.13"
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test)
    violationRules {
        rule {
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = minimumCoverage
            }
            limit {
                counter = "BRANCH"
                value = "COVEREDRATIO"
                minimum = minimumCoverage
            }
        }
    }
}

val benchmarkBaseline = layout.projectDirectory.file("benchmark/baseline.json")

fun JavaExec.benchmarkRunner(vararg extra: String, main: String = "io.github.ppzxc.smq.benchmark.BenchMain") {
    group = "benchmark"
    // smq jar와 그 런타임 의존성은 모듈 경로에, 하네스(benchmark 출력)만 클래스패스에 둔다.
    // 자식 JVM은 부모의 jdk.module.path와 java.class.path를 그대로 물려받는다.
    dependsOn(tasks.jar)
    classpath = benchmark.output
    mainClass = main
    val modulePath = files(tasks.jar, configurations.runtimeClasspath)
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        // ModuleGuard.childJvmArgs와 같은 인자 목록이다. 한쪽을 바꾸면 다른 쪽도 맞춘다.
        listOf("--enable-native-access=ALL-UNNAMED", "--enable-native-access=org.xerial.sqlitejdbc",
            "--module-path", modulePath.asPath, "--add-modules", "io.github.ppzxc.smq")
    })
    javaLauncher = javaToolchains.launcherFor(java.toolchain)
    args(*extra)
}

tasks.register<JavaExec>("benchmarkSmoke") {
    description = "짧은 스모크 실행. 임계값 없이 하네스가 끝까지 도는지만 확인한다."
    benchmarkRunner("--mode", "smoke")
}

abstract class FullBenchmark : JavaExec() {
    @get:Input
    @get:Optional
    @get:Option(option = "repeat", description = "비율 검사의 S1 조건을 재는 횟수 (기본 3, 1이면 단발)")
    abstract val repeat: Property<String>
}

tasks.register<FullBenchmark>("benchmark") {
    description = "전체 벤치. 기준 환경에서 수동으로 돌린다. --repeat 옵션, 그 밖의 옵션은 -Pbenchmark.args=\"...\"로 더한다."
    benchmarkRunner("--mode", "full", "--baseline", benchmarkBaseline.asFile.path,
            "--output", layout.buildDirectory.file("benchmark/result.json").get().asFile.path)
    (findProperty("benchmark.args") as String?)?.let { args(it.split(" ").filter(String::isNotBlank)) }
    doFirst { repeat.orNull?.let { args("--repeat", it) } }
}

// 무작위 블랙박스 하네스. 옵션은 `./gradlew randomBench --seed 42 --budget 10m`처럼 태스크 옵션으로 준다.
abstract class RandomBench : JavaExec() {
    @get:Input
    @get:Optional
    @get:Option(option = "seed", description = "이 시드 하나만 다시 돈다")
    abstract val seed: Property<String>

    @get:Input
    @get:Optional
    @get:Option(option = "budget", description = "새 실행을 시작할 수 있는 세션 시간. 예: 30m, 90s (기본 30m)")
    abstract val budget: Property<String>

    @get:Input
    @get:Optional
    @get:Option(option = "load", description = "부하 구간 길이. 예: 10s (기본 10s)")
    abstract val load: Property<String>

    @get:Input
    @get:Optional
    @get:Option(option = "master-seed", description = "실행 시드를 뽑을 마스터 시드 (기본 현재 시각)")
    abstract val masterSeed: Property<String>

    @get:Input
    @get:Optional
    @get:Option(option = "heap-dump", description = "자식 JVM의 OutOfMemoryError에서 힙 덤프를 실행 디렉터리에 남긴다 (기본 끔)")
    abstract val heapDump: Property<Boolean>
}

val randomBenchMain = "io.github.ppzxc.smq.benchmark.random.RandomBenchMain"
val randomBenchOut = layout.buildDirectory.dir("random-bench").get().asFile.path

tasks.register<RandomBench>("randomBench") {
    description = "무작위 워크로드와 불변식 검사기. 수동 실행. 첫 위반에서 멈추고 비정상 종료한다. --seed, --budget 옵션."
    benchmarkRunner("--out-dir", randomBenchOut, main = randomBenchMain)
    doFirst {
        seed.orNull?.let { args("--seed", it) }
        budget.orNull?.let { args("--budget", it) }
        load.orNull?.let { args("--load", it) }
        masterSeed.orNull?.let { args("--master-seed", it) }
        if (heapDump.orNull == true) args("--heap-dump")
    }
}

// check에는 넣지 않는다. CI Linux 잡이 별도 단계로 돌린다.
tasks.register<JavaExec>("randomBenchSmoke") {
    description = "고정 시드 몇 개를 실행당 약 2초씩 돈다. 하네스와 검사기가 끝까지 도는지, 위반이 없는지 확인한다."
    benchmarkRunner("--smoke", "--out-dir", randomBenchOut, main = randomBenchMain)
}

// benchmark 소스셋이 배포 jar와 소스 jar에 섞이지 않았는지 jar 내용으로 확인한다.
val verifyJarExcludesBenchmark = tasks.register("verifyJarExcludesBenchmark") {
    group = "verification"
    val jars = tasks.jar.get().archiveFile
    val sources = tasks.named<Jar>("sourcesJar").get().archiveFile
    dependsOn(tasks.jar, tasks.named("sourcesJar"))
    doLast {
        listOf(jars.get().asFile, sources.get().asFile).forEach { jar ->
            val entries = ZipFile(jar).use { z -> z.entries().toList().map { it.name } }
            check(entries.any { it.endsWith("smq/Smq.class") || it.endsWith("smq/Smq.java") }) { "${jar.name}에 main 항목이 없다" }
            val bad = entries.filter { it.contains("smq/benchmark/") }
            check(bad.isEmpty()) { "${jar.name}에 benchmark 항목이 들어 있다: $bad" }
        }
    }
}
tasks.check { dependsOn(verifyJarExcludesBenchmark, tasks.jacocoTestCoverageVerification, tasks.javadoc) }

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

// 스키마 견본을 다시 만드는 수동 작업. 평소 빌드와 테스트는 이 작업을 돌리지 않는다.
tasks.register<JavaExec>("generateSchemaSample") {
    group = "verification"
    description = "src/test/resources/schema-samples/schema-v1.db 견본을 다시 만든다"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "io.github.ppzxc.smq.SchemaSampleGenerator"
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

// 서명은 항상 켜 두되(키가 있으면 로컬이든 CI든 서명한다), 키가 없을 때 실패하는 곳은 Central 업로드 경로뿐이다.
// 그래야 릴리스 버전(비 SNAPSHOT)에서도 키 없는 publishToMavenLocal이 성공한다.
afterEvaluate {
    extensions.configure<SigningExtension> {
        setRequired(provider {
            gradle.taskGraph.allTasks.any { it.name.contains("MavenCentral") }
        })
    }
}

mavenPublishing {
    configure(JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = SourcesJar.Sources()))
    // USER_MANAGED: 업로드까지만 하고 공개(Publish)는 사람이 Portal에서 누른다.
    publishToMavenCentral(automaticRelease = false)
    // 조건 없이 켠다. 로컬 signing.* 키링과 CI signingInMemoryKey를 모두 받는다.
    signAllPublications()

    pom {
        name = "smq"
        description = "청크로 꺼내 ACK/NACK하는 재시도 가능한 내구성 큐 라이브러리"
        url = "https://github.com/ppzxc/smq"
        licenses {
            license {
                name = "MIT License"
                url = "https://opensource.org/licenses/MIT"
                distribution = "repo"
            }
        }
        developers {
            developer {
                id = "ppzxc"
                name = "ppzxc"
                url = "https://github.com/ppzxc"
            }
        }
        scm {
            url = "https://github.com/ppzxc/smq"
            connection = "scm:git:git://github.com/ppzxc/smq.git"
            developerConnection = "scm:git:ssh://git@github.com/ppzxc/smq.git"
        }
    }
}
