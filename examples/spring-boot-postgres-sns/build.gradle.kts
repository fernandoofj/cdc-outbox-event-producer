import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    kotlin("plugin.jpa") version "2.3.21"
    id("org.springframework.boot") version "4.0.6"
}

group = "com.example"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    // `cdc-outbox-*` isn't on Maven Central yet (see the root repo's
    // CONTRIBUTING.md § "Maven Central publish") — run
    // `./gradlew publishToMavenLocal` from the repo root first so these
    // coordinates resolve from ~/.m2.
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.0.6"))
    implementation(platform("br.com.fltech.outbox:cdc-outbox-bom:0.3.0"))

    // The library: starter + the one source/sink pair this sample wires
    // (README's "Setup mínimo — Postgres → SNS").
    implementation("br.com.fltech.outbox:cdc-outbox-spring-boot-starter")
    implementation("br.com.fltech.outbox:cdc-outbox-source-postgres")
    implementation("br.com.fltech.outbox:cdc-outbox-sink-aws")

    // Brought by the app, not the starter (spring-boot-starter declares
    // `spring-cloud-aws-sns` `compileOnly` so consumers who don't need
    // SNS never pull it in transitively) — the `-starter-` coordinate,
    // not the base library, is what's needed here: it carries Spring
    // Cloud AWS's own auto-configuration that turns `spring.cloud.aws.*`
    // properties into an actual `SnsTemplate` bean. The bare
    // `spring-cloud-aws-sns` module only provides the class the
    // library's `@ConditionalOnClass` checks for — without the
    // autoconfiguration half, no `SnsTemplate` bean ever gets created
    // and `CdcOutboxSinkAutoConfiguration` has nothing to wire.
    implementation("io.awspring.cloud:spring-cloud-aws-starter-sns:4.0.2")

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
}

tasks.withType<KotlinCompile> {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}
