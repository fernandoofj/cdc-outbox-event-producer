import io.gitlab.arturbosch.detekt.Detekt
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    `java-library`
    `maven-publish`

    id("io.gitlab.arturbosch.detekt") version "1.20.0"
    id("org.jlleitschuh.gradle.ktlint") version "11.0.0"
    id("org.jetbrains.kotlin.kapt") version "1.7.20"
    id("org.sonarqube") version "3.5.0.2730"
    jacoco
}

group = "shop.inventa"
version = "0.0.1"

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

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
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
