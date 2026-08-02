import io.gitlab.arturbosch.detekt.Detekt
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.3.21" apply false
    `java-library`
    `maven-publish`

    id("io.gitlab.arturbosch.detekt") version "1.23.8" apply false
    // Held back at 12.1.1 (Dependabot proposed 14.2.0, two majors): the
    // ktlintCheck gate is already red pre-existing (checkpoint-file and
    // others; see docs/HISTORY.md); the 14.x engine adds ~19% more findings
    // across more modules and was never actually run before this bump would
    // have landed. Re-evaluate once the existing ktlint debt is cleaned up.
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1" apply false
    id("org.jetbrains.kotlin.kapt") version "2.3.21" apply false
    id("org.sonarqube") version "5.1.0.4882"
    id("com.github.ben-manes.versions") version "0.54.0"
    id("com.gorylenko.gradle-git-properties") version "2.4.2" apply false
    id("com.avast.gradle.docker-compose") version "0.17.12"
    jacoco
}

apply(plugin = "docker-compose")

allprojects {
    group = "br.com.fltech.outbox"
    // Wave 7 — Multi-artifact Maven publish. Bump 0.0.11 → 0.1.0 was
    // a breaking change for consumers: the legacy coordinate
    // `cdc-outbox-event-producer` is no longer published; each Gradle
    // module ships under its own coordinate `cdc-outbox-<module>`.
    //
    // Round 21 (dependency batch, on top of the Round 20 group move):
    // 0.1.0 → 0.2.0, also breaking. Two independent reasons: (1) the
    // group itself moved in Round 20 —
    // br.com.fltech.cdc.outbox → br.com.fltech.outbox; (2) the Kotlin
    // toolchain moved 1.9.25 → 2.3.21 and Micrometer 1.12.13 → 1.16.5,
    // both ahead of what the Spring Boot 3.3.5 BOM this library
    // targeted at the time would resolve on its own — a consumer on
    // plain Boot 3.3.5 dependency management could otherwise silently downgrade
    // kotlin-stdlib/micrometer-core underneath classes compiled
    // against the newer versions. Same version, two groups, would
    // have hidden that.
    //
    // Round 22 — Spring Boot 4: 0.2.0 → 0.3.0, the biggest breaking
    // change yet. Spring Boot 3.3.5 -> 4.0.6 / Spring Framework 7 /
    // spring-kafka 4 / spring-rabbit 4 / spring-cloud-aws 4, and the
    // JVM baseline itself moved 17 -> 21 (Boot 4's own minimum).
    // Consumers still on JRE 17 or a Boot 3.x classpath cannot use
    // this version at all.
    version = "0.3.0"

    repositories {
        mavenCentral()
    }
}

// The `:bom` module is a `java-platform` (POM-only) — it does NOT
// get the Kotlin/Java/Detekt/Jacoco apply{} below. Wired separately
// further down.
subprojects {
    if (name == "bom") return@subprojects

    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "io.gitlab.arturbosch.detekt")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "jacoco")

    apply {
        from("${rootProject.rootDir}/config/detekt.gradle")
    }

    extensions.configure<JavaPluginExtension> {
        // Round 22 — Spring Boot 4 requires JDK 21 as its own minimum
        // baseline, so this library's target moved 17 -> 21 alongside it.
        // Breaking for any consumer still on JRE 17.
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
        // Wave 7 — publish `-sources.jar` and `-javadoc.jar` alongside
        // the main jar. Sources are valuable for IDE jump-to-definition
        // for downstream consumers; the empty javadoc.jar satisfies the
        // Maven Central convention even though Kotlin code uses KDoc.
        withSourcesJar()
        withJavadocJar()
    }

    // Pins the JDK actually used to COMPILE this module's Kotlin/Java
    // sources to 21, via Gradle's toolchain resolution — independent
    // of whatever JDK happens to be on PATH. This does NOT fix
    // CONTRIBUTING.md's documented failure mode (Gradle 8.10.2's
    // OWN embedded Kotlin DSL compiler can't parse a bare
    // `java -version` like `26.0.1` on JDK 24+, so the daemon that
    // would evaluate this very toolchain block never starts in the
    // first place) — that half still requires `JAVA_HOME` pointed at
    // JDK ≤23 before invoking `./gradlew` at all, same as always.
    // What this DOES fix: once the daemon is up, compilation is
    // reproducible regardless of which JDK ≤23 happens to be ambient.
    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(21)
    }

    tasks.withType<KotlinCompile> {
        compilerOptions {
            freeCompilerArgs.add("-Xjsr305=strict")
            // Kotlin 2.x is moving the default annotation use-site target from
            // param-only ("first-only") to param+field ("param-property"),
            // planned as the default in 2.4. Pin "first-only" explicitly so
            // @JsonProperty on constructor vals (wal2json row-change models,
            // DlqEnvelope) keeps targeting only the constructor parameter
            // Jackson actually binds against, not also the backing field.
            freeCompilerArgs.add("-Xannotation-default-target=first-only")
            jvmTarget.set(JvmTarget.JVM_21)
        }
    }

    tasks.withType<Detekt>().configureEach {
        reports {
            html.required.set(true)
            xml.required.set(true)
            txt.required.set(false)
            sarif.required.set(false)
        }
    }

    tasks.withType<org.jlleitschuh.gradle.ktlint.tasks.BaseKtLintCheckTask> {
        workerMaxHeapSize.set("512m")
    }

    tasks.withType<Test> {
        useJUnitPlatform()

        // Propagate Testcontainers env vars (DOCKER_API_VERSION,
        // RUN_TESTCONTAINERS, etc.) into the test JVM. Gradle 8 does
        // not inherit env by default; docker-java reads `api.version`
        // as a JVM system property and uses it as the floor when
        // negotiating with the daemon — OrbStack requires >= 1.40.
        listOf(
            "DOCKER_HOST",
            "DOCKER_API_VERSION",
            "TESTCONTAINERS_RYUK_DISABLED",
            "TESTCONTAINERS_CHECKS_DISABLE",
            "RUN_TESTCONTAINERS",
        ).forEach { name ->
            System.getenv(name)?.let { value -> environment(name, value) }
        }
        System.getenv("DOCKER_API_VERSION")?.let { systemProperty("api.version", it) }

        // Test.environment is NOT tracked as a task input by Gradle's
        // up-to-date check, so flipping RUN_TESTCONTAINERS between
        // runs (the switch that gates every `*IT.kt`/`*E2EIT.kt` via
        // `@EnabledIfEnvironmentVariable`) leaves this task UP-TO-DATE
        // against a stale result — `RUN_TESTCONTAINERS=1 ./gradlew
        // test` after a plain `./gradlew test` silently reports the
        // CACHED non-Testcontainers result instead of actually running
        // the gated suites, unless the caller remembers `--rerun`.
        // Declaring it explicitly as an input closes that gap.
        inputs.property("runTestContainers", System.getenv("RUN_TESTCONTAINERS") ?: "")
    }

    dependencies {
        "testImplementation"(kotlin("test"))
        "testImplementation"("io.mockk:mockk:1.13.13")
        "testRuntimeOnly"("org.slf4j:slf4j-simple:2.0.18")
    }

    // Wave 7 — per-module Maven publication. Each Gradle subproject
    // ships under the coordinate `br.com.fltech.outbox:cdc-outbox-<name>`.
    // The `from(components["java"])` wiring is what carries the
    // correct dependency scopes through to the published POM
    // (implementation → compile, compileOnly → provided, etc.).
    extensions.configure<PublishingExtension> {
        repositories {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/fernandoofj/cdc-outbox-event-producer")
                credentials {
                    username = System.getenv("GITHUB_ACTOR")
                    password = System.getenv("GITHUB_TOKEN")
                }
            }
        }
        publications {
            register<MavenPublication>("library") {
                from(components["java"])
                artifactId = "cdc-outbox-${project.name}"
                pom {
                    name.set("cdc-outbox-${project.name}")
                    description.set("CDC outbox producer — ${project.name} module")
                    url.set("https://github.com/fernandoofj/cdc-outbox-event-producer")
                    licenses {
                        license {
                            name.set("MIT License")
                            url.set("https://opensource.org/licenses/MIT")
                        }
                    }
                    developers {
                        developer {
                            id.set("fernandoofj")
                            name.set("Fernando")
                        }
                    }
                    scm {
                        connection.set(
                            "scm:git:https://github.com/fernandoofj/cdc-outbox-event-producer.git",
                        )
                        developerConnection.set(
                            "scm:git:ssh://github.com:fernandoofj/cdc-outbox-event-producer.git",
                        )
                        url.set("https://github.com/fernandoofj/cdc-outbox-event-producer")
                    }
                }
            }
        }
    }
}

// Root-only tasks. The docker-compose plugin is rooted here so a
// single `gradlew startDockerCompose` in the parent brings up the
// shared stack used by `:legacy` integration tests.
tasks.register("startDockerCompose") {
    doLast {
        exec {
            commandLine("docker-compose", "-f", "docker-compose.yml", "up", "-d")
        }
    }
}

tasks.register("stopDockerCompose") {
    doLast {
        exec {
            commandLine("docker-compose", "-f", "docker-compose.yml", "down", "-v")
        }
    }
}
