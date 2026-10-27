// smq 빌드와 분리된 외부 소비자. 입력은 -PsmqVersion(필수)과 -PrepoUrl(기본 mavenLocal)이다.
plugins {
    application
}

val smqVersion = providers.gradleProperty("smqVersion").get()
val repoUrl = providers.gradleProperty("repoUrl").orElse("mavenLocal").get()

repositories {
    if (repoUrl == "mavenLocal") mavenLocal() else maven { url = uri(repoUrl) }
    mavenCentral()
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

sourceSets.main { java.srcDir("../shared/src/main/java") }
    // 모듈 경로 소비자만 module-info를 얹는다.
    sourceSets.main { java.srcDir("../shared/module") }

dependencies {
    implementation("io.github.ppzxc:smq:$smqVersion")
}

application {
    mainModule = "smq.deploytest"
    mainClass = "smqdeploytest.Main"
    applicationDefaultJvmArgs = listOf("--enable-native-access=org.xerial.sqlitejdbc")
}
