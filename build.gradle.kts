import java.util.zip.ZipFile

plugins {
    `java-library`
    `maven-publish`
}

group = "io.github.ppzxc"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
    withJavadocJar()
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

    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release = 25
}

val benchmarkBaseline = layout.projectDirectory.file("benchmark/baseline.json")

fun JavaExec.benchmarkRunner(vararg extra: String) {
    group = "benchmark"
    // smq jar와 그 런타임 의존성은 모듈 경로에, 하네스(benchmark 출력)만 클래스패스에 둔다.
    // 자식 JVM은 부모의 jdk.module.path와 java.class.path를 그대로 물려받는다.
    dependsOn(tasks.jar)
    classpath = benchmark.output
    mainClass = "io.github.ppzxc.smq.benchmark.BenchMain"
    val modulePath = files(tasks.jar, configurations.runtimeClasspath)
    jvmArgumentProviders.add(CommandLineArgumentProvider {
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

tasks.register<JavaExec>("benchmark") {
    description = "전체 벤치. 기준 환경에서 수동으로 돌린다. -Pbenchmark.args=\"...\"로 옵션을 더한다."
    benchmarkRunner("--mode", "full", "--baseline", benchmarkBaseline.asFile.path,
            "--output", layout.buildDirectory.file("benchmark/result.json").get().asFile.path)
    (findProperty("benchmark.args") as String?)?.let { args(it.split(" ").filter(String::isNotBlank)) }
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
tasks.check { dependsOn(verifyJarExcludesBenchmark) }

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

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
