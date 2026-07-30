import io.gitlab.arturbosch.detekt.Detekt
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.25" apply false
    `java-library`
    `maven-publish`

    id("io.gitlab.arturbosch.detekt") version "1.23.8" apply false
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0" apply false
    id("org.jetbrains.kotlin.kapt") version "2.3.21" apply false
    id("org.sonarqube") version "5.1.0.4882"
    id("com.github.ben-manes.versions") version "0.51.0"
    id("com.gorylenko.gradle-git-properties") version "2.4.2" apply false
    id("com.avast.gradle.docker-compose") version "0.17.12"
    jacoco
}

apply(plugin = "docker-compose")

allprojects {
    group = "br.com.fltech.outbox"
    // Wave 7 — Multi-artifact Maven publish. Bump 0.0.11 → 0.1.0 is
    // a breaking change for consumers: the legacy coordinate
    // `cdc-outbox-event-producer` is no longer published; each Gradle
    // module ships under its own coordinate `cdc-outbox-<module>`.
    version = "0.1.0"

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
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        // Wave 7 — publish `-sources.jar` and `-javadoc.jar` alongside
        // the main jar. Sources are valuable for IDE jump-to-definition
        // for downstream consumers; the empty javadoc.jar satisfies the
        // Maven Central convention even though Kotlin code uses KDoc.
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions {
            freeCompilerArgs = listOf("-Xjsr305=strict")
            jvmTarget = "17"
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
    }

    dependencies {
        "testImplementation"(kotlin("test"))
        "testImplementation"("io.mockk:mockk:1.13.13")
        "testRuntimeOnly"("org.slf4j:slf4j-simple:2.0.16")
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
