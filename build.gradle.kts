import io.gitlab.arturbosch.detekt.Detekt
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    `java-library`
    `maven-publish`

    id("io.gitlab.arturbosch.detekt") version "1.20.0"
    id("org.jlleitschuh.gradle.ktlint") version "11.0.0"
    id("org.jetbrains.kotlin.kapt") version "1.7.20"
    id("org.sonarqube") version "4.2.1.3168"
    id("com.github.ben-manes.versions") version "0.42.0"
    id("com.gorylenko.gradle-git-properties") version "2.4.1"
    id("com.avast.gradle.docker-compose") version "0.17.5"
    jacoco
}

apply(plugin = "docker-compose")

group = "shop.inventa"
version = "0.0.7"

repositories {
    mavenCentral()
}

apply {
    from("${rootProject.rootDir}/config/detekt.gradle")
    from("${rootProject.rootDir}/config/tests.gradle")
    from("${rootProject.rootDir}/config/jacoco.gradle")
    from("${rootProject.rootDir}/config/sonar.gradle")
}

dependencies {
    implementation("org.springframework:spring-messaging:6.0.13")
    implementation("com.amazonaws:aws-java-sdk-sts:1.12.566")
    implementation("io.awspring.cloud:spring-cloud-aws-messaging:2.4.4")
    implementation("software.amazon.awssdk:sts:2.21.1")
    implementation("org.postgresql:postgresql:42.6.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.3")

    testImplementation(kotlin("test"))
    testImplementation("io.mockk:mockk:1.12.8")
    testImplementation("org.testcontainers:testcontainers:1.19.1")
    testImplementation("org.testcontainers:junit-jupiter:1.19.1")
    testImplementation("org.testcontainers:postgresql:1.19.1")
    testImplementation("org.testcontainers:localstack:1.19.1")
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

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
}

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

tasks.withType<Test> {
    dependsOn("startDockerCompose")
    finalizedBy("stopDockerCompose")
}

tasks.withType<Jar> {
    archiveFileName.set("kotlin-postgres-cdc-to-sns-module.jar")
    destinationDirectory.set(file("build/libs"))
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/inventa-shop/kotlin-postgres-cdc-to-sns-module")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
    publications {
        register<MavenPublication>("gpr") {
            from(components["java"])
        }
    }
}
