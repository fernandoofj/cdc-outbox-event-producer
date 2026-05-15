import io.gitlab.arturbosch.detekt.Detekt
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.25" apply false
    `java-library`
    `maven-publish`

    id("io.gitlab.arturbosch.detekt") version "1.23.7" apply false
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1" apply false
    id("org.jetbrains.kotlin.kapt") version "1.9.25" apply false
    id("org.sonarqube") version "5.1.0.4882"
    id("com.github.ben-manes.versions") version "0.51.0"
    id("com.gorylenko.gradle-git-properties") version "2.4.2" apply false
    id("com.avast.gradle.docker-compose") version "0.17.12"
    jacoco
}

apply(plugin = "docker-compose")

allprojects {
    group = "br.com.fltech.cdc.outbox"
    version = "0.0.11"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "java-library")
    apply(plugin = "io.gitlab.arturbosch.detekt")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "jacoco")

    apply {
        from("${rootProject.rootDir}/config/detekt.gradle")
    }

    extensions.configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
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
