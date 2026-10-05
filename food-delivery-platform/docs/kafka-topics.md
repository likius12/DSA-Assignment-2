# Kafka Topics

The Food Delivery Platform uses Kafka as its event-driven backbone. Each microservice communicates asynchronously via topics rather than calling other services directly. This gives us loose coupling, fault tolerance, and horizontal scalability.

## Configuration

| Setting | Value |
|---|---|
| Broker | kafka:9092 (internal), localhost:29092 (external) |
| Partitions (default) | 3 |
| Replication factor | 1 (development) |
| Auto-create topics | Enabled |
| Consumer offset tracking | __consumer_offsets (internal) |

Partitions: 3 per topic means up to 3 consumers per consumer group can process messages in parallel.

---

## Topics

### 1. orders.created

| | |
|---|---|
| Producer | Order Service |
| Consumers | Payment Service, Delivery Service, Notification Service |
| Purpose | Fired when a customer places an order. The order starts in CREATED state. |
| Triggered by | POST /api/v1/orders |
| Payload | {_id, customerId, restaurantId, items, totalAmount, status: "CREATED", createdAt} |

---

### 2. orders.confirmed

| | |
|---|---|
| Producer | Order Service |
| Consumers | Notification Service |
| Purpose | Optional event when an order is confirmed. Kept as a topic placeholder for future use. |
| Triggered by | Order Service internal state transition (not published in current version) |

---

### 3. orders.cancelled

| | |
|---|---|
| Producer | Order Service |
| Consumers | Payment Service, Delivery Service, Notification Service |
| Purpose | Fired when an order is cancelled before delivery. |
| Triggered by | PATCH /api/v1/orders/{id}/status with {"status": "CANCELLED"} |
| Payload | {orderId, reason} |

---

### 4. payments.completed

| | |
|---|---|
| Producer | Payment Service |
| Consumers | Order Service, Delivery Service, Notification Service |
| Purpose | Fired when a payment is successfully processed for an order. |
| Triggered by | Consuming orders.created → simulating payment → publishing the result |
| Payload | {orderId, paymentId, transactionId, amount, status: "COMPLETED"} |

---

### 5. payments.failed

| | |
|---|---|
| Producer | Payment Service |
| Consumers | Order Service, Notification Service |
| Purpose | Fired when a payment fails. Order Service transitions to CANCELLED. |
| Triggered by | Payment Service on simulated payment failure |
| Payload | {orderId, reason} |

---

### 6. delivery.assigned

| | |
|---|---|
| Producer | Delivery Service |
| Consumers | Order Service, Notification Service |
| Purpose | Fired when a driver is assigned to an order. Order Service transitions to OUT_FOR_DELIVERY. |
| Triggered by | Consuming payments.completed → finding available driver → publishing assignment |
| Payload | {orderId, deliveryId, driverId} |

---

### 7. delivery.completed

| | |
|---|---|
| Producer | Delivery Service |
| Consumers | Order Service, Notification Service |
| Purpose | Fired when a driver completes the delivery. Order Service transitions to DELIVERED. |
| Triggered by | POST /api/v1/deliveries/{orderId}/complete |
| Payload | {orderId, deliveryId, driverId} |

---

### 8. restaurant.menu.updated

| | |
|---|---|
| Producer | Restaurant Service |
| Consumers | Notification Service |
| Purpose | Fired whenever a restaurant's menu changes (item added, updated, or removed). |
| Triggered by | POST /api/v1/restaurants/{id}/menu or PATCH /api/v1/restaurants/menu/{itemId} |
| Payload | {restaurantId, updatedAt} |

---

## Event Flow — A Single Order

One POST /orders triggers the entire chain:

1. Order Service publishes orders.created
2. Payment Service consumes it, processes payment, publishes payments.completed
3. Delivery Service consumes payments.completed, assigns a driver, publishes delivery.assigned
4. Order Service consumes payments.completed → status becomes CONFIRMED
5. Order Service consumes delivery.assigned → status becomes OUT_FOR_DELIVERY
6. User calls POST /api/v1/deliveries/{orderId}/complete
7. Delivery Service publishes delivery.completed
8. Order Service consumes delivery.completed → status becomes DELIVERED

---

## Partitioning Strategy

Each topic is created with 3 partitions using:

kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic <topic-name> --partitions 3 --replication-factor 1

Why 3 partitions?

- Up to 3 consumers per consumer group can consume in parallel, giving 3x throughput.
- Without a message key, Kafka uses sticky partitioning (batches to the same partition for efficiency).
- With a key (e.g., orderId), all messages for the same order go to the same partition, guaranteeing ordering.

For production, you would set orderId as the message key and increase replication factor to 3.

---

## Consumer Groups

Each service uses its own consumer group so multiple services can consume the same topic independently:

| Service | Consumer Group ID | Topics Consumed |
|---|---|---|
| Payment Service | payment-service-group | orders.created |
| Delivery Service | delivery-service-group | payments.completed |
| Order Service (payment) | order-service-payment-group | payments.completed, payments.failed |
| Order Service (delivery) | order-service-delivery-group | delivery.assigned, delivery.completed |
| Notification Service | notification-service-group | All 8 topics |

Different groups mean each service gets its own copy of every event.

---

## Viewing Topics

| Tool | URL | Purpose |
|---|---|---|
| Kafka UI | http://localhost:8080 | Browse topics, messages, partitions visually |
| CLI | docker exec fdp-kafka kafka-topics --bootstrap-server localhost:9092 --list | List all topics |

---

## Failure Handling

| Scenario | Behaviour |
|---|---|
| Consumer down when message published | Message waits in the topic — consumer reads when it comes back (Kafka retains 7 days by default) |
| Consumer crashes mid-processing | autoCommit is true, duplicates possible but operations are idempotent per orderId |
| Kafka broker down | Producers retry. Services continue running. |
| Invalid JSON in message | Consumer logs the error and continues with the next message. |