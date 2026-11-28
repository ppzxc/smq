import java.util.jar.JarFile

plugins {
    application
}

repositories {
    mavenCentral()
}

java {
    modularity.inferModulePath = true
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

val smqJar = providers.gradleProperty("smqJar")
    .map { file(it) }
    .orElse(layout.projectDirectory.file("../build/libs/smq-0.1.0-SNAPSHOT.jar").asFile)
val kafkaOriginal = configurations.create("kafkaOriginal") {
    isCanBeConsumed = false
    isCanBeResolved = true
    isTransitive = false
}
val kafkaClientModuleJar = tasks.register<Jar>("kafkaClientModuleJar") {
    from({ zipTree(kafkaOriginal.singleFile) })
    archiveFileName.set("kafka-clients-4.4.0.jar")
    destinationDirectory.set(layout.buildDirectory.dir("kafka-module"))
    manifest.attributes["Automatic-Module-Name"] = "kafka.clients"
}
val smqVersion = providers.provider<String> {
    JarFile(smqJar.get()).use { jar ->
        jar.manifest.mainAttributes.getValue("Implementation-Version") ?: "unknown"
    }
}

dependencies {
    implementation(files(smqJar))
    implementation("com.rabbitmq:amqp-client:5.36.0")
    implementation(files(kafkaClientModuleJar))
    add(kafkaOriginal.name, "org.apache.kafka:kafka-clients:4.4.0")
    compileOnly("org.jspecify:jspecify:1.0.1")
    runtimeOnly("org.slf4j:slf4j-nop:1.7.36")
    runtimeOnly("org.xerial:sqlite-jdbc:3.53.4.0")
    runtimeOnly("com.github.luben:zstd-jni:1.5.7-17")
    runtimeOnly("at.yawk.lz4:lz4-java:1.11.2")
    runtimeOnly("org.xerial.snappy:snappy-java:1.1.10.7")
    runtimeOnly("org.slf4j:slf4j-api:1.7.36")

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
    val modulePath = files(sourceSets.main.get().output.classesDirs, configurations.runtimeClasspath).asPath
    systemProperty("smoke.modulePath", modulePath)
    systemProperty("smoke.smqVersion", smqVersion.get())
}

tasks.run {
    jvmArgs("--enable-native-access=org.xerial.sqlitejdbc")
    systemProperty("smq.version", smqVersion.get())
    systemProperty(
        "smoke.modulePath",
        files(sourceSets.main.get().output.classesDirs, configurations.runtimeClasspath).asPath
    )
    providers.gradleProperty("smoke.args").orNull?.let { arguments ->
        args(arguments.split(Regex("\\s+")).filter(String::isNotBlank))
    }
}
