# Sample: Postgres → SNS

A minimal Spring Boot app demonstrating `cdc-outbox-event-producer`'s
"Setup mínimo — Postgres → SNS" combo (see the root
[README § Quick start](../../README.md#quick-start)). It exposes one
endpoint, `POST /orders`, that inserts a row and emits a
`pg_logical_emit_message` WAL message in the same database
transaction — the **producer** side of the outbox pattern. The
`cdc-outbox-spring-boot-starter` auto-configuration wired into *this
same process* (see `application.yml`) is what actually reads the
replication slot and publishes to SNS: the app never calls SNS
directly, but the read/publish also isn't a separate deployment — it's
the same Spring context, started by the same `bootRun`.

This is a standalone Gradle project (its own `settings.gradle.kts`,
wrapper, `build.gradle.kts`) — it consumes `cdc-outbox-*` the same way
an external project would, via Maven coordinates, not
`project(":...")`.

## Prerequisites

The `cdc-outbox-*` artifacts aren't on Maven Central yet (see the root
repo's [`CONTRIBUTING.md`](../../CONTRIBUTING.md#maven-central-publish-nf11--not-started)).
Publish them to your local Maven repository first:

```bash
cd ../..
./gradlew publishToMavenLocal
```

## Run it

```bash
# from this directory
docker compose up -d          # Postgres (wal_level=logical) + LocalStack (SNS)
./gradlew bootRun
```

In another terminal, place an order:

```bash
curl -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"totalCents": 4590}'
```

That request commits an `orders` row and a WAL message in the same
transaction. The `cdc-outbox-spring-boot-starter` auto-configuration
wired into *this same app* (see `application.yml`) opens the
`orders_outbox_slot` replication slot, reads the message back, and
publishes it to the LocalStack `orders-events` SNS topic.

Check it landed — `shell-scripts/localstack/02_subscribe_queue.sh` also
creates and subscribes an `orders-events-check` SQS queue to the topic
on container startup, so you can read the message back directly:

```bash
docker exec -it $(docker compose ps -q localstack) \
  awslocal sqs receive-message \
    --queue-url http://localhost:4566/000000000000/orders-events-check
```

or simplest of all, tail the app logs — a successful publish logs at
INFO with the event's `domainId`.

## What to look at

  * [`OrderController.kt`](src/main/kotlin/com/example/ordersapp/OrderController.kt) —
    the producer side: insert + `pg_logical_emit_message`, one
    transaction, zero broker code.
  * [`OrderRepository.kt`](src/main/kotlin/com/example/ordersapp/OrderRepository.kt) —
    the native query that emits the WAL message.
  * [`application.yml`](src/main/resources/application.yml) — the
    `cdc.outbox.*` block that turns *this same Spring Boot process*
    into the reader/publisher for the slot it just wrote to.

## Cleaning up

```bash
docker compose down -v
```
