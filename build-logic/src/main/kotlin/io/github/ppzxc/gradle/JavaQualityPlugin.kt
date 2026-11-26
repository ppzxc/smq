package io.github.ppzxc.gradle

import com.diffplug.gradle.spotless.SpotlessExtension
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.quality.Pmd
import org.gradle.api.plugins.quality.PmdExtension
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.compile.JavaCompile

class JavaQualityPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.pluginManager.apply("net.ltgt.errorprone")
        project.pluginManager.apply("net.ltgt.nullaway")
        project.pluginManager.apply("com.diffplug.spotless")
        project.pluginManager.apply("pmd")

        project.tasks.withType(Pmd::class.java).configureEach {
            exclude("**/module-info.java")
        }

        configurePmd(project)

        project.dependencies.add("errorprone", "com.google.errorprone:error_prone_core:2.51.0")
        project.dependencies.add("errorprone", "com.uber.nullaway:nullaway:0.14.3")

        project.tasks.withType(JavaCompile::class.java).configureEach {
            val isMainCompilation = name == "compileJava"
            options.encoding = "UTF-8"
            options.release.set(25)
            options.compilerArgs.addAll(listOf("-Xlint:unchecked", "-Xlint:rawtypes"))

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
                MUST_ERROR_PRONE_CHECKS.forEach { check(it, CheckSeverity.ERROR) }
                if (!isMainCompilation) {
                    // Test scenario names are Korean specifications, so report these without failing compilation.
                    warn("UnicodeInCode")
                }
            }

            if (isMainCompilation) {
                options.compilerArgs.add("-Werror")
            }
        }

        project.extensions.configure(SpotlessExtension::class.java) {
            java {
                target("src/*/java/**/*.java", "deploy-test/**/src/**/*.java")
                palantirJavaFormat("2.103.0")
            }
        }
    }

    private fun configurePmd(project: Project) {
        val mustRules = project.rootProject.file("config/pmd/must.xml")
        val shouldRules = project.rootProject.file("config/pmd/should.xml")

        project.extensions.configure(PmdExtension::class.java) {
            toolVersion = PMD_VERSION
            ruleSetFiles = project.files(mustRules)
            ruleSets = emptyList()
            setIgnoreFailures(false)
        }

        val sourceSets = project.extensions.getByType(SourceSetContainer::class.java)
        sourceSets.configureEach {
            if (name !in PMD_SOURCE_SETS) return@configureEach

            val sourceSet = this
            val taskName = "pmd${name.replaceFirstChar(Char::uppercase)}Should"
            val shouldTask = project.tasks.register(taskName, Pmd::class.java) {
                group = "verification"
                description = "Reports PMD SHOULD violations in the ${sourceSet.name} Java sources."
                source(sourceSet.allJava)
                classpath = project.files(sourceSet.output, sourceSet.compileClasspath)
                ruleSetFiles = project.files(shouldRules)
                ruleSets = emptyList()
                setIgnoreFailures(true)
                setConsoleOutput(true)
                reports {
                    xml.required.set(true)
                    html.required.set(true)
                }
            }
            project.tasks.named("check").configure { dependsOn(shouldTask) }
        }
    }

    private companion object {
        const val PMD_VERSION = "7.29.0"
        val PMD_SOURCE_SETS = setOf("main", "test", "benchmark")

        val MUST_ERROR_PRONE_CHECKS = listOf(
            "ArrayEquals",
            "ArrayToString",
            "EqualsIncompatibleType",
            "EqualsHashCode",
            "ModifyCollectionInEnhancedForLoop",
            "ModifySourceCollectionInStream",
            "NarrowCalculation",
            "NarrowingCompoundAssignment",
        )
    }
}
