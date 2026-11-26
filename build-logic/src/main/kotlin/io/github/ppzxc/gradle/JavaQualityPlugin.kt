package io.github.ppzxc.gradle

import com.diffplug.gradle.spotless.SpotlessExtension
import com.github.spotbugs.snom.SpotBugsExtension
import com.github.spotbugs.snom.SpotBugsTask
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.quality.Pmd
import org.gradle.api.plugins.quality.PmdExtension
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.compile.JavaCompile
import java.util.concurrent.atomic.AtomicBoolean
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.stream.StreamSource

class JavaQualityPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.pluginManager.apply("net.ltgt.errorprone")
        project.pluginManager.apply("net.ltgt.nullaway")
        project.pluginManager.apply("com.diffplug.spotless")
        project.pluginManager.apply("pmd")

        project.tasks.withType(Pmd::class.java).configureEach {
            if (!name.endsWith("Comments")) exclude("**/module-info.java")
            if (name in setOf("pmdMain", "pmdTest", "pmdBenchmark")) {
                inputs.property("policyReviewDate", java.time.LocalDate.now().toString())
                incrementalAnalysis.set(false)
            }
        }

        configurePmd(project)
        configureSpotBugs(project)

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
                if (isMainCompilation || name == "compileBenchmarkJava") {
                    // Tests intentionally capture interrupts to assert the caller contract.
                    check("InterruptedExceptionSwallowed", CheckSeverity.ERROR)
                }
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

    private fun configureSpotBugs(project: Project) {
        project.pluginManager.apply("com.github.spotbugs-base")
        project.extensions.configure(SpotBugsExtension::class.java) {
            toolVersion.set("4.10.5")
            excludeFilter.set(project.rootProject.file("config/spotbugs/exclude.xml"))
        }

        val main = project.extensions.getByType(SourceSetContainer::class.java).named("main")
        val spotbugsMain = project.tasks.register("spotbugsMain", SpotBugsTask::class.java) {
            excludeFilter.set(project.rootProject.layout.projectDirectory.file("config/spotbugs/exclude.xml"))
            sourceDirs.from(main.map { it.allJava.sourceDirectories })
            classDirs.from(main.map { it.output.classesDirs })
            auxClassPaths.from(main.map { it.compileClasspath })
            setIgnoreFailures(true)
            reports {
                create("html") {
                    required.set(true)
                    outputLocation.set(project.layout.buildDirectory.file("reports/spotbugs/main.html"))
                }
            }
        }
        project.tasks.named("check").configure { dependsOn(spotbugsMain) }
    }

    private fun configurePmd(project: Project) {
        val mustRules = project.rootProject.file("config/pmd/must.xml")
        val shouldRules = project.rootProject.file("config/pmd/should.xml")

        project.extensions.configure(PmdExtension::class.java) {
            toolVersion = PMD_VERSION
            ruleSetFiles = project.files(mustRules)
            ruleSets = emptyList()
            rulesMinimumPriority.set(5)
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
                setIgnoreFailures(sourceSet.name != "main")
                setConsoleOutput(true)
                reports {
                    xml.required.set(true)
                    html.required.set(true)
                }
            }
            project.tasks.named("check").configure { dependsOn(shouldTask) }
        }

        project.afterEvaluate {
            for (sourceSet in PMD_SOURCE_SETS) {
                for (suffix in listOf("", "Should")) {
                    val taskName = "pmd${sourceSet.replaceFirstChar(Char::uppercase)}$suffix"
                    val pmdTask = project.tasks.named(taskName, Pmd::class.java)
                    val started = AtomicBoolean(false)
                    val htmlTask = project.tasks.register("${taskName}HtmlReport") {
                        group = "verification"
                        description = "Renders $taskName XML with rule IDs and violation count."
                        doLast {
                            val task = pmdTask.get()
                            val xml = task.reports.xml.outputLocation.get().asFile
                            val html = task.reports.html.outputLocation.get().asFile
                            if (!started.get() && !task.state.upToDate && task.state.executed) {
                                xml.delete()
                                html.delete()
                            } else if (xml.exists()) {
                                html.parentFile.mkdirs()
                                val stylesheet = project.rootProject.file("config/pmd/report.xsl")
                                TransformerFactory.newInstance().newTransformer(StreamSource(stylesheet))
                                    .transform(StreamSource(xml), StreamResult(html))
                            } else if (started.get() && task.state.failure == null) {
                                throw GradleException("$taskName did not produce a PMD XML report")
                            }
                        }
                    }
                    pmdTask.configure {
                        reports.html.required.set(false)
                        doFirst {
                            started.set(true)
                            reports.xml.outputLocation.get().asFile.delete()
                            reports.html.outputLocation.get().asFile.delete()
                        }
                        finalizedBy(htmlTask)
                    }
                }
            }
        }
    }

    private companion object {
        const val PMD_VERSION = "7.29.0"
        val PMD_SOURCE_SETS = setOf("main", "test", "benchmark")

        val MUST_ERROR_PRONE_CHECKS = listOf(
            "ArrayEquals",
            "ArrayToString",
            "CatchAndPrintStackTrace",
            "DefaultCharset",
            "DefaultLocale",
            "EnumOrdinal",
            "EqualsIncompatibleType",
            "EqualsHashCode",
            "FloatingPointLiteralPrecision",
            "FutureReturnValueIgnored",
            "IntLongMath",
            "JavaTimeDefaultTimeZone",
            "LockNotBeforeTry",
            "MissingFail",
            "ModifyCollectionInEnhancedForLoop",
            "ModifySourceCollectionInStream",
            "NarrowCalculation",
            "NarrowingCompoundAssignment",
            "NonAtomicVolatileUpdate",
            "SynchronizeOnNonFinalField",
            "TimeUnitConversionChecker",
            "UnusedException",
            "WaitNotInLoop",
        )
    }
}
