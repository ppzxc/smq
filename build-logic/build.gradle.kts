plugins {
    `kotlin-dsl`
}

dependencies {
    implementation("net.ltgt.gradle:gradle-errorprone-plugin:5.1.1")
    implementation("net.ltgt.gradle:gradle-nullaway-plugin:3.3.0")
    implementation("com.diffplug.spotless:spotless-plugin-gradle:8.10.4")
}

gradlePlugin {
    plugins {
        register("javaQuality") {
            id = "smq.java-quality"
            implementationClass = "io.github.ppzxc.gradle.JavaQualityPlugin"
        }
    }
}
