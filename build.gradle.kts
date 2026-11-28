import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.SourcesJar
import java.util.zip.ZipFile
import org.gradle.plugins.signing.SigningExtension
import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
    `java-library`
    jacoco
    id("com.vanniktech.maven.publish") version "0.37.0"
    id("smq.java-quality")
    id("org.cyclonedx.bom") version "3.5.1"
}

group = "io.github.ppzxc"
// 버전은 하드코딩한다. main은 항상 다음 패치-SNAPSHOT이고, 릴리스 PR에서만 바꾼다.
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    pmd("net.sourceforge.pmd:pmd-ant:7.29.0")
    pmd(project(":pmd-rules"))
    implementation("org.xerial:sqlite-jdbc:3.53.4.0")
    compileOnlyApi("org.jspecify:jspecify:1.0.1")

    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("net.jqwik:jqwik:1.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:all,-missing", "-quiet")
        addBooleanOption("Werror", true)
        encoding = "UTF-8"
    }
}

tasks.jar {
    manifest.attributes["Implementation-Version"] = project.version
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

// The application artifact must not include the standalone test harness or its test dependencies.
val verifyPublishedJarBoundary = tasks.register("verifyPublishedJarBoundary") {
    group = "verification"
    val jars = tasks.jar.get().archiveFile
    val sources = tasks.named<Jar>("sourcesJar").get().archiveFile
    dependsOn(tasks.jar, tasks.named("sourcesJar"))
    doLast {
        listOf(jars.get().asFile, sources.get().asFile).forEach { jar ->
            val entries = ZipFile(jar).use { z -> z.entries().toList().map { it.name } }
            check(entries.any { it.endsWith("smq/Smq.class") || it.endsWith("smq/Smq.java") }) { "${jar.name}에 main 항목이 없다" }
            val bad = entries.filter {
                it.contains("smq/harness/")
                        || it.startsWith("org/junit/")
                        || it.startsWith("org/xerial/")
            }
            check(bad.isEmpty()) { "${jar.name}에 harness 또는 test 항목이 들어 있다: $bad" }
        }
    }
}
tasks.check { dependsOn(verifyPublishedJarBoundary, tasks.jacocoTestCoverageVerification, tasks.javadoc) }

val smokeHarnessCheck = tasks.register<GradleBuild>("smokeHarnessCheck") {
    group = "verification"
    description = "독립 Gradle 하네스의 공개 API 스모크 테스트를 실행한다."
    dir = layout.projectDirectory.dir("smoke-harness").asFile
    tasks = listOf("test")
    dependsOn("jar")
}
tasks.check { dependsOn(smokeHarnessCheck) }

tasks.register<GradleBuild>("smokeHarness") {
    group = "verification"
    description = "두 내구성 수준과 단건·배치 공개 API 스모크를 실행한다. -Psmoke.args로 옵션을 넘긴다."
    dir = layout.projectDirectory.dir("smoke-harness").asFile
    tasks = listOf("run")
    dependsOn("jar")
    findProperty("smoke.args")?.toString()?.let { startParameter.projectProperties["smoke.args"] = it }
}

val commentRules = layout.projectDirectory.file("config/pmd/comments.xml")
// PMD 7.29 cannot parse both requires modifiers. Keep the line length for comment locations.
val moduleCommentSource = tasks.register("prepareModuleCommentSource") {
    val module = layout.projectDirectory.file("src/main/java/module-info.java")
    val output = layout.buildDirectory.file("pmd-module/module-info.java")
    inputs.file(module)
    outputs.file(output)
    doLast {
        val target = output.get().asFile
        target.parentFile.mkdirs()
        val trivia = "(?:\\s|/\\*[\\s\\S]*?\\*/|//[^\\r\\n]*(?:\\r?\\n|$))+"
        val modifiers = Regex("\\brequires${trivia}(?:static${trivia}(transitive)|(transitive)${trivia}static)\\b")
        target.writeText(module.asFile.readText().replace(modifiers) { match ->
            val transitive = match.groups[1] ?: match.groups[2]!!
            val start = transitive.range.first - match.range.first
            match.value.replaceRange(start, start + transitive.value.length, " ".repeat(transitive.value.length))
        })
    }
}
listOf("main", "test").forEach { sourceName ->
    val sourceSet = sourceSets[sourceName]
    val task = tasks.register<org.gradle.api.plugins.quality.Pmd>("pmd${sourceName.replaceFirstChar(Char::uppercase)}Comments") {
        group = "verification"
        if (sourceName == "main") {
            source(sourceSet.allJava.matching { exclude("**/module-info.java") })
            source(moduleCommentSource)
        } else {
            source(sourceSet.allJava)
        }
        exclude("**/generated/**", "**/generated-sources/**")
        classpath = files(sourceSet.output, sourceSet.compileClasspath)
        ruleSetFiles = files(commentRules)
        ruleSets = emptyList()
        ignoreFailures = false
        reports.xml.required.set(true)
        doLast {
            val report = reports.xml.outputLocation.get().asFile
            val factory = javax.xml.parsers.DocumentBuilderFactory.newInstance()
            factory.isNamespaceAware = true
            val document = factory.newDocumentBuilder().parse(report)
            check(document.getElementsByTagNameNS("*", "error").length == 0) {
                "PMD could not parse a source file; see $report"
            }
        }
    }
    tasks.check { dependsOn(task) }
}
tasks.check { dependsOn(":pmd-rules:test") }

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

// 스키마 견본을 다시 만드는 수동 작업. 평소 빌드와 테스트는 이 작업을 돌리지 않는다.
tasks.register<JavaExec>("generateSchemaSample") {
    group = "verification"
    description = "현재 스키마 버전의 견본을 다시 만든다"
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
