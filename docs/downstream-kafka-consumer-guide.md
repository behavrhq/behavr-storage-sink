# Downstream service: consuming Behavr Collector Kafka topics

This document is for **another Spring Boot application** that consumes messages produced by the **Behavr Collector API** (`behavr-api`). It describes the **contract** on the wire and a **recommended Spring Kafka** setup. It is not a dependency of this repo; copy or adapt the snippets into your consumer project.

## Topic and message contract

| Item | Value |
|------|--------|
| **Topic** | `behavr.events.raw` (configurable in the collector via `behavr.kafka.topic`; default is this name) |
| **Message key** | `siteId + ":" + eventId` (UTF-8 string), e.g. `site_123:8f3dd1c7-0b7c-4baf-91a8-8f41d9d7d4c1` |
| **Message value** | Single JSON object per message (**one event per Kafka record**), produced with **Jackson** and **no Kafka type headers** (`spring.json.add.type.headers: false`) |
| **Value JSON shape** | Java-style **camelCase** property names matching the collector’s `CollectedEvent` record (see table below) |

### Value payload fields (camelCase)

The collector serializes a structure equivalent to:

| Field | Type (logical) | Notes |
|-------|----------------|--------|
| `eventId` | string | Client-generated id |
| `eventType` | string | e.g. `page_view`, `search`, `purchase`, … |
| `siteId` | string | Must match batch `site_id` |
| `anonymousId` | string | |
| `sessionId` | string | |
| `occurredAt` | ISO-8601 instant | Client time |
| `receivedAt` | ISO-8601 instant | **Server** ingest time (trusted) |
| `batchSentAt` | ISO-8601 instant or null | From batch `sent_at` when present |
| `url`, `path`, `title`, `referrer`, `userAgent`, `browserLanguage`, `deviceType`, `sdkVersion` | string / null | As sent by the SDK |
| `utm` | object (string → value) | May be empty `{}` |
| `properties` | object | Event-specific payload |
| `serverContext` | object | Nested object (see below) |

**`serverContext`:**

| Field | Type | Notes |
|-------|------|--------|
| `ipAddress` | string / null | From the HTTP connection |
| `userAgent` | string / null | **Request** User-Agent (may differ from event `userAgent`) |
| `requestId` | string | Per HTTP request to the collector |

Your consumer should treat **unknown JSON properties** as ignorable so you can evolve the schema without breaking older consumers.

## Consumer responsibilities (recommended)

1. **Consumer group** — Use a **dedicated `group.id`** for your service (e.g. `behavr-clickhouse-sink`). All instances of the same logical service share the same group so partitions are load-balanced.
2. **Idempotency** — Keys are stable per `(siteId, eventId)`; use them for deduplication if your sink is at-least-once.
3. **Offset commit** — Prefer processing that allows **safe offset commits** after successful persistence (or use transactions / outbox patterns if you need exactly-once semantics across systems).
4. **Throughput** — Increase `concurrency` / partition count only after you understand lag and downstream capacity.
5. **Failures** — Decide policy: retry, **DLQ** topic, or skip + dead-letter store; do not block the poll loop indefinitely.

## Spring Boot consumer: dependencies

In the **consumer** `pom.xml`:

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-kafka</artifactId>
</dependency>
```

(Optional) Same stack as collector: WebFlux, Actuator, Micrometer, etc., depending on what your service does.

## Spring Boot consumer: configuration

### Local (PLAINTEXT to Docker Kafka)

Align `bootstrap.servers` with the collector (default host port **9092**). Example:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: behavr-raw-consumer
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "your.consumer.package"
        spring.json.use.type.headers: false
      auto-offset-reset: earliest
```

- **`spring.json.use.type.headers: false`** — Must match the producer (no type headers).
- **`spring.json.trusted.packages`** — Restrict packages Jackson is allowed to instantiate for security; point at the package where your **DTO classes** live.

### Confluent Cloud (SASL_SSL + PLAIN)

Use the **same** security settings as a producer to Confluent: `SASL_SSL`, `PLAIN`, JAAS with API key/secret. Prefer **environment variables** or a secret manager; do not commit secrets.

```yaml
spring:
  kafka:
    bootstrap-servers: ${CONFLUENT_BOOTSTRAP_SERVERS}
    consumer:
      group-id: behavr-raw-consumer
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.behavr.ingest.dto"
        spring.json.use.type.headers: false
      auto-offset-reset: earliest
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username='${CONFLUENT_API_KEY}' password='${CONFLUENT_API_SECRET}';
```

Grant the Confluent **API key** ACLs to **READ** the topic `behavr.events.raw` (and use the cluster). Topic may already exist from the collector or be created in the Confluent UI.

## DTO in the consumer project

Define a **read-only** DTO that mirrors the JSON (camelCase). Example:

```java
package com.example.behavr.ingest.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CollectedEventMessage(
    String eventId,
    String eventType,
    String siteId,
    String anonymousId,
    String sessionId,
    Instant occurredAt,
    Instant receivedAt,
    Instant batchSentAt,
    String url,
    String path,
    String title,
    String referrer,
    String userAgent,
    String browserLanguage,
    String deviceType,
    String sdkVersion,
    Map<String, Object> utm,
    Map<String, Object> properties,
    ServerContextPayload serverContext) {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record ServerContextPayload(String ipAddress, String userAgent, String requestId) {}
}
```

Configure the deserializer to use this type (see listener example below).

## Listener example (`@KafkaListener`)

```java
package com.example.behavr.ingest;

import com.example.behavr.ingest.dto.CollectedEventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class RawEventsListener {

  private static final Logger log = LoggerFactory.getLogger(RawEventsListener.class);

  @KafkaListener(
      topics = "${behavr.ingest.raw-topic:behavr.events.raw}",
      containerFactory = "kafkaListenerContainerFactory")
  public void onEvent(
      @Payload CollectedEventMessage event,
      @Header(KafkaHeaders.RECEIVED_KEY) String key) {

    // key format: siteId + ":" + eventId — useful for logging and dedupe
    log.info("Consumed site_id={} event_type={} event_id={}", event.siteId(), event.eventType(), event.eventId());

    // Persist to ClickHouse / S3 / etc.
  }
}
```

You need a **`ConsumerFactory`** / **`ConcurrentKafkaListenerContainerFactory`** bean that wires `JsonDeserializer<CollectedEventMessage>` with `addTrustedPackages(...)` and **`setUseTypeHeaders(false)`**, or rely on YAML `spring.json.*` properties as above.

Minimal **programmatic** deserializer setup (if you prefer Java config over YAML for the deserializer):

```java
JsonDeserializer<CollectedEventMessage> deserializer = new JsonDeserializer<>(CollectedEventMessage.class);
deserializer.addTrustedPackages("com.example.behavr.ingest.dto");
deserializer.setUseTypeHeaders(false);
```

## Topic name in the consumer

Either **hardcode** `behavr.events.raw` or externalize:

```yaml
behavr:
  ingest:
    raw-topic: behavr.events.raw
```

Keep the same default as the collector unless you intentionally fork topics per environment.

## Observability

- Expose **Micrometer** Kafka consumer metrics (Spring Boot auto-configures many).
- Log **site id**, **event type**, and **event id**; avoid logging full **URL** or **properties** at INFO in production (PII / volume).

## Future topics

If later you add **dead-letter**, **enriched**, or **partitioned-by-tenant** topics, document them in the collector repo and point consumers to the same `docs/` pattern so contracts stay discoverable.

---

**Reference in this repo:** producer value type `net.behavr.collector.model.CollectedEvent` and publisher key format in `net.behavr.collector.kafka.KafkaEventPublisher`.
