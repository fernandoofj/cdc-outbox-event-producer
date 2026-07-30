// Wave 7 — Bill of Materials.
//
// POM-only artifact that pins versions for every published
// `cdc-outbox-*` module. Consumers import the BOM via
// `dependencies { implementation(platform("br.com.fltech.outbox:cdc-outbox-bom:0.3.0")) }`
// and then declare individual modules without versions —
// Gradle/Maven resolves the version from the BOM's `constraints`
// section.
//
// Same pattern used by `spring-boot-dependencies`,
// `software.amazon.awssdk:bom`, `io.awspring.cloud:spring-cloud-aws-dependencies`,
// etc. Lets the consumer upgrade everything in lock-step by
// changing the BOM version in a single line.
plugins {
    `java-platform`
    `maven-publish`
}

javaPlatform {
    // Allow constraints on internal :project dependencies as well as
    // external coordinates — needed so the BOM can pin sibling modules.
    allowDependencies()
}

dependencies {
    constraints {
        api(project(":core"))
        api(project(":checkpoint-file"))
        api(project(":source-postgres"))
        api(project(":source-mysql"))
        api(project(":source-stubs"))
        api(project(":sink-composition"))
        api(project(":sink-aws"))
        api(project(":sink-kafka"))
        api(project(":sink-rabbitmq"))
        api(project(":lag-probes"))
        api(project(":legacy"))
        api(project(":dlq-replay"))
        api(project(":replay-source"))
        api(project(":spring-boot-starter"))
        api(project(":test-support"))
    }
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
        register<MavenPublication>("bom") {
            from(components["javaPlatform"])
            artifactId = "cdc-outbox-bom"
            pom {
                name.set("cdc-outbox-bom")
                description.set(
                    "Bill of Materials for cdc-outbox-event-producer — pins versions of " +
                        "every cdc-outbox-* module so consumers can declare them without version.",
                )
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
