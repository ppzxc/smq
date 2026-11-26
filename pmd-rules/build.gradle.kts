plugins { java }
repositories { mavenCentral() }
dependencies {
    implementation("net.sourceforge.pmd:pmd-java:7.29.0")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
tasks.test { useJUnitPlatform() }
