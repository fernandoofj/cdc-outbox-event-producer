# Examples

Standalone, runnable sample apps that consume `cdc-outbox-event-producer`
the way an external project would — via published Maven coordinates
(`implementation(platform("br.com.fltech.outbox:cdc-outbox-bom:..."))`),
not Gradle's `project(":...")`. Each sample is its own Gradle build
with its own wrapper; none of them are included in the root
`settings.gradle.kts`.

| Sample | Combo | What it shows |
|---|---|---|
| [`spring-boot-postgres-sns/`](spring-boot-postgres-sns/) | Postgres → SNS | Minimal producer: JPA insert + `pg_logical_emit_message` in one transaction, `cdc-outbox-spring-boot-starter` auto-config doing the read/publish side. |

See each sample's own `README.md` for prerequisites and run steps.
