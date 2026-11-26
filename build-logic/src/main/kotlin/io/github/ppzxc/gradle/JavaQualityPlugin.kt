package io.github.ppzxc.gradle

import com.diffplug.gradle.spotless.SpotlessExtension
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.compile.JavaCompile

class JavaQualityPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.pluginManager.apply("net.ltgt.errorprone")
        project.pluginManager.apply("net.ltgt.nullaway")
        project.pluginManager.apply("com.diffplug.spotless")

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

    private companion object {
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
