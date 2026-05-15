import io.gitlab.arturbosch.detekt.Detekt
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.25"
    `java-library`
    `maven-publish`

    id("io.gitlab.arturbosch.detekt") version "1.23.7"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1"
    id("org.jetbrains.kotlin.kapt") version "1.9.25"
    id("org.sonarqube") version "5.1.0.4882"
    id("com.github.ben-manes.versions") version "0.51.0"
    id("com.gorylenko.gradle-git-properties") version "2.4.2"
    id("com.avast.gradle.docker-compose") version "0.17.12"
    jacoco
}

apply(plugin = "docker-compose")

group = "br.com.fltech.cdc.outbox"
version = "0.0.10"

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
    implementation("org.postgresql:postgresql:42.6.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.3")
    implementation("org.json:json:20240205")

    // Wave 1 — pool, observability, retry
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("io.micrometer:micrometer-core:1.12.13")

    // Wave 2 — Spring Boot starter. compileOnly so the library remains
    // usable without Spring Boot on the classpath; Boot apps pick these up
    // through their own starter chain.
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:3.3.5")
    compileOnly("org.springframework.boot:spring-boot-actuator:3.3.5")
    compileOnly("org.springframework.boot:spring-boot-actuator-autoconfigure:3.3.5")
    kapt("org.springframework.boot:spring-boot-configuration-processor:3.3.5")

    // Wave 2b — Spring Cloud AWS 3.x. SCA 3 uses AWS SDK v2 and
    // exposes `SnsTemplate` / `SqsTemplate` instead of the EOL
    // `NotificationMessagingTemplate` / `QueueMessagingTemplate` from
    // SCA 2. compileOnly so consumers without Boot still compile.
    compileOnly("io.awspring.cloud:spring-cloud-aws-sns:3.2.1")
    compileOnly("io.awspring.cloud:spring-cloud-aws-sqs:3.2.1")
    implementation("software.amazon.awssdk:sns:2.27.21")
    implementation("software.amazon.awssdk:sqs:2.27.21")
    implementation("software.amazon.awssdk:sts:2.27.21")

    // Wave 4 — Kafka and RabbitMQ sink adapters. compileOnly so users
    // that only need SNS/SQS don't pay for Kafka/AMQP transitive deps.
    // The EventSinkRegistry only registers a sink for a scheme when the
    // corresponding template bean is on the classpath.
    compileOnly("org.springframework.kafka:spring-kafka:3.2.4")
    compileOnly("org.springframework.amqp:spring-rabbit:3.1.7")

    // Wave 3 — MySQL outbox-table source. mysql-connector-j is
    // compileOnly because the source only needs a `DataSource`; the
    // consumer brings its own driver. The Testcontainers dep for MySQL
    // is wired in the test scope below.
    compileOnly("com.mysql:mysql-connector-j:8.4.0")

    testImplementation(kotlin("test"))
    testImplementation("io.mockk:mockk:1.13.13")
    testImplementation("io.micrometer:micrometer-test:1.12.13")
    testImplementation("org.springframework.boot:spring-boot-autoconfigure:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-actuator:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-actuator-autoconfigure:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-test:3.3.5")
    testImplementation("io.awspring.cloud:spring-cloud-aws-sns:3.2.1")
    testImplementation("io.awspring.cloud:spring-cloud-aws-sqs:3.2.1")
    testImplementation("org.testcontainers:testcontainers:1.20.4")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testImplementation("org.testcontainers:localstack:1.20.4")
    testImplementation("org.awaitility:awaitility:4.2.2")

    // Wave 3+4 — broker / DB clients reach the test classpath so the
    // unit tests can mock them without consumer apps having to depend
    // on them transitively.
    testImplementation("org.springframework.kafka:spring-kafka:3.2.4")
    testImplementation("org.springframework.amqp:spring-rabbit:3.1.7")
    testImplementation("com.mysql:mysql-connector-j:8.4.0")

    // Wire an SLF4J binding for tests so Testcontainers' diagnostic
    // logging is visible during integration runs.
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

java {
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

    // Propagate Testcontainers-relevant env vars from the gradle-invoking
    // shell into the Test JVM. Gradle 8 does not inherit env by default,
    // so the Wave 2b integration tests (which spin up OrbStack/Colima
    // containers) would otherwise fail with "Could not find a valid Docker
    // environment". `DOCKER_API_VERSION` is essential when running against
    // OrbStack — the bundled `docker-java` negotiates API 1.32, but
    // OrbStack's server requires 1.40+. Fixing the floor here lets
    // negotiation succeed.
    listOf(
        "DOCKER_HOST",
        "DOCKER_API_VERSION",
        "TESTCONTAINERS_RYUK_DISABLED",
        "TESTCONTAINERS_CHECKS_DISABLE",
        "RUN_TESTCONTAINERS",
    ).forEach { name ->
        System.getenv(name)?.let { value -> environment(name, value) }
    }
    // docker-java reads `api.version` as a JVM system property (in addition
    // to the DOCKER_API_VERSION env var) and uses that as the floor when
    // negotiating with the daemon. OrbStack rejects anything below 1.40.
    System.getenv("DOCKER_API_VERSION")?.let { systemProperty("api.version", it) }
}

tasks.withType<Jar> {
    archiveFileName.set("cdc-outbox-event-producer.jar")
    destinationDirectory.set(file("build/libs"))
}

publishing {
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
        register<MavenPublication>("gpr") {
            from(components["java"])
        }
    }
}
