import java.util.jar.JarFile

plugins {
    application
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

val smqJar = providers.gradleProperty("smqJar")
    .map { file(it) }
    .orElse(layout.projectDirectory.file("../build/libs/smq-0.1.0-SNAPSHOT.jar").asFile)
val smqVersion = providers.provider<String> {
    JarFile(smqJar.get()).use { jar ->
        jar.manifest.mainAttributes.getValue("Implementation-Version") ?: "unknown"
    }
}

dependencies {
    implementation(files(smqJar))
    compileOnly("org.jspecify:jspecify:1.0.1")
    runtimeOnly("org.xerial:sqlite-jdbc:3.53.4.0")

    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainModule.set("io.github.ppzxc.smq.harness")
    mainClass.set("io.github.ppzxc.smq.harness.Main")
}

tasks.test {
    useJUnitPlatform()
    val sqlite = configurations.runtimeClasspath
    val modulePath = files(sourceSets.main.get().output.classesDirs, smqJar, sqlite).asPath
    systemProperty("smoke.modulePath", modulePath)
    systemProperty("smoke.smqVersion", smqVersion.get())
}

tasks.run {
    jvmArgs("--enable-native-access=org.xerial.sqlitejdbc")
    systemProperty("smq.version", smqVersion.get())
    providers.gradleProperty("smoke.args").orNull?.let { arguments ->
        args(arguments.split(Regex("\\s+")).filter(String::isNotBlank))
    }
}
