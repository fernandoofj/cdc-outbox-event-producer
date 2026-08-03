// Source-side replay tooling — operator-facing endpoints to
// re-emit historic events from MySQL binlog (or Postgres WAL, when
// the Postgres adapter graduates from stub). The replayer reuses
// the EventSinkRegistry publish path so downstream sinks see the
// same envelope shape as the live producer.
dependencies {
    api(project(":core"))
    api(project(":source-mysql"))
    api(project(":source-postgres"))

    implementation("com.zaxxer:HikariCP:7.0.2")
    compileOnly("com.mysql:mysql-connector-j:8.4.0")
    compileOnly("com.zendesk:mysql-binlog-connector-java:0.29.2")
    compileOnly("org.springframework.boot:spring-boot-actuator:4.0.6")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:4.0.6")
    compileOnly("org.springframework.security:spring-security-core:7.1.0")

    testImplementation("com.mysql:mysql-connector-j:8.4.0")
    testImplementation("com.zendesk:mysql-binlog-connector-java:0.29.2")
    testImplementation(project(":test-support"))
    testImplementation("org.springframework.boot:spring-boot-actuator:4.0.6")
    testImplementation("org.springframework.boot:spring-boot-test:4.0.6")
    testImplementation("org.springframework.security:spring-security-core:7.1.0")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testImplementation("org.awaitility:awaitility:4.2.2")
}
