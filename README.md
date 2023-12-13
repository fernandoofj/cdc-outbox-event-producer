# Kotlin Postgres CDC to SNS Module (pg2sns4k)
_Kotlin Postgres CDC to SNS Module_ is a library that provides resources to consume events recorded in the PostgreSQL transaction log (also known as WAL), posting the result as a message on AWS SNS.

## Dependency

Add the following to your **build.gradle.kts**:

```kotlin
repositories {
    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/inventa-shop/kotlin-postgres-cdc-to-sns-module")

        credentials {
            username = System.getenv("GITHUB_USERNAME")
            password = System.getenv("GITHUB_PASSWORD")
        }
    }
}

dependencies {
    implementation("shop.inventa:kotlin-postgres-cdc-to-sns-module:0.0.7")
}
```

**No other dependencies required.**

### CI/CD

Add the following to your **GitHub Actions Workflows** that build the application:

```yaml
env:
  GITHUB_USERNAME: ${{ github.actor }}
  GITHUB_PASSWORD: ${{ secrets.GIT_PAT }}
```

### Local

Set these two environment variables:

```sh
export GITHUB_USERNAME=my-github-username
export GITHUB_PASSWORD=git-hub-personal-access-token # the token only needs the read:packages permission
```

## Using

### Starting the CDC streamimg

**Observation**: The topic name is in the field prefix on transaction log record.

```kotlin
SlotReaderSNSProducer(
    // Provide database connection information from secrets
    PostgresConfiguration(host, port, database, username, password),
    // Provide the WAL slot name to listener
    ReplicationConfiguration(slotName),
    // Provide the notificationMessagingTemplate to send the message to SNS
    SNSTransactionalProducer(notificationMessagingTemplate)
).startStreaming()
```

### Posting the message to WAL manually

~~~~sql
SELECT pg_logical_emit_message(
    transactional -- true or false, 
    prefix -- destination topic name, 
    content -- serialized Payload
);
~~~~

### Posting the message to WAL using kotlin

**Repository**

```kotlin
@Repository
interface OutboxRepository : JpaRepository<Product, Long>, SaveOutboxMessagePort {

    @Query(
        nativeQuery = true,
        value = "SELECT CAST(pg_logical_emit_message(:transactional, :prefix, :content) AS VARCHAR)"
    )
    override fun emitLogicalMessage(
        @Param("transactional") transactional: Boolean,
        @Param("prefix") prefix: String,
        @Param("content") content: String
    ): String
}
```

**Outbox Producer Component**

```kotlin
package shop.inventa.catalogue.common.adapter.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import shop.inventa.catalogue.common.adapter.messaging.dto.SNSMessage
import shop.inventa.catalogue.common.adapter.messaging.dto.SNSMessageBody
import shop.inventa.catalogue.outbox.domain.port.SaveOutboxMessagePort
import java.time.format.DateTimeFormatter

@Component
class SNSTransactionalOutboxProducer(
    private val saveOutboxMessagePort: SaveOutboxMessagePort,
    private val objectMapper: ObjectMapper
) : SNSProducer {

    override fun <T : Any> send(topicName: String, message: SNSMessageBody<T>) {
        val headers = mapOf(
            Pair("eventType", message.eventType),
            Pair("eventTimestamp", message.eventTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
        )

        sendOutOfBoxMessage(topicName, message, headers)
    }

    private fun <T : Any> sendOutOfBoxMessage(
        topicName: String,
        body: SNSMessageBody<T>,
        headers: Map<String, String>
    ) {

        val snsMessage = SNSMessage(
            headers = headers,
            body = body
        )

        saveOutboxMessagePort.emitLogicalMessage(
            prefix = topicName,
            content = objectMapper.writeValueAsString(snsMessage)
        )
    }
}
```

**Calling Producer**

```kotlin
@Component
class EventSNSProducer(
    @Value("\${messaging.topic.topic-name-key}")
    private val topicName: String,
    private val snsTransactionalOutboxProducer: SNSTransactionalOutboxProducer 
) : ProduceEventPort {

    override fun send(/*...*/) {
        
        val payload = buildPayload(/*...*/)

        val messageBody = SNSMessageBody(
            eventType = event.name,
            domainId = payload.uuid.toString(),
            payload = payload
        )

        snsTransactionalOutboxProducer.send(topicName, messageBody)
    }
}
```
