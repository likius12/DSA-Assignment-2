# System Architecture

The Food Delivery Platform is a **distributed, event-driven, microservices-based** system built with Ballerina, Kafka, MongoDB, and Docker. It was designed for the Ministry of Industrialisation and Trade to support local Namibian SMEs (restaurants and delivery drivers).

## Design Goals

- **Scalable** — handle high concurrency during peak meal times.
- **Fault-tolerant** — one service failing must not bring down the platform.
- **Loosely coupled** — services communicate via events, not direct calls.
- **Observable** — every service exposes metrics for real-time monitoring.
- **Reproducible** — one command (`docker compose up -d`) starts the whole platform.

## High-Level Architecture

    ┌───────────────────────────────────────────────────────────────┐
    │                     CLIENTS                                    │
    │   Customers · Restaurants · Drivers · Admin                    │
    └──────────────────────────┬────────────────────────────────────┘
                               │
                               │ REST / HTTP
                               ▼
    ┌───────────────────────────────────────────────────────────────┐
    │                     REST APIs (Ballerina)                      │
    │                                                                │
    │   Customer (8081)     Restaurant (8082)     Order (8083)      │
    │   Payment  (8084)     Delivery   (8085)     Notification(8086)│
    │                       Admin      (8087)                       │
    │                                                                │
    └──────────────────────────┬────────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
    ┌───────────────────────┐   ┌───────────────────────┐
    │      KAFKA            │   │      MONGODB          │
    │   (Event Bus)         │   │  (Per-Service DBs)    │
    │                       │   │                       │
    │  orders.created       │   │  customer_db          │
    │  orders.confirmed     │   │  restaurant_db        │
    │  orders.cancelled     │   │  order_db             │
    │  payments.completed   │   │  payment_db           │
    │  payments.failed      │   │  delivery_db          │
    │  delivery.assigned    │   │  notification_db      │
    │  delivery.completed   │   │  admin_db (reads only)│
    │  restaurant.menu.updated│ │                       │
    └───────────────────────┘   └───────────────────────┘

                               │
                               ▼
    ┌───────────────────────────────────────────────────────────────┐
    │              OBSERVABILITY                                     │
    │   Prometheus (9090) · Grafana (3000) · Kafka UI (8080)         │
    │   Mongo Express (8088)                                         │
    └───────────────────────────────────────────────────────────────┘

## Microservices

### 1. Customer Service (Port 8081)

Manages customer accounts and delivery addresses.

- **Database:** `customer_db.customers`
- **Kafka:** None (pure CRUD)
- **Endpoints:** GET / POST / DELETE /api/v1/customers

### 2. Restaurant Service (Port 8082)

Manages restaurant profiles, menus, and inventory.

- **Database:** `restaurant_db.restaurants`, `restaurant_db.menuItems`
- **Kafka:** Publishes `restaurant.menu.updated`
- **Endpoints:** CRUD for restaurants, menu items

### 3. Order Service (Port 8083)

The **core** of the system. Owns the order state machine.

- **Database:** `order_db.orders`
- **Kafka:**
  - Produces: `orders.created`, `orders.cancelled`
  - Consumes: `payments.completed`, `payments.failed`, `delivery.assigned`, `delivery.completed`
- **Endpoints:** Create order, read order, manually patch status

The Order Service is the **single source of truth** for order state. It is the only service that writes to the `orders` collection.

### 4. Payment Service (Port 8084)

Simulates payment processing. Reacts to new orders and publishes the result.

- **Database:** `payment_db.payments`
- **Kafka:**
  - Produces: `payments.completed`, `payments.failed`
  - Consumes: `orders.created`
- **Endpoints:** Read-only

Payments are **only** created in response to `orders.created`. There is no manual "create payment" endpoint.

### 5. Delivery Service (Port 8085)

Coordinates driver assignment and delivery tracking.

- **Database:** `delivery_db.deliveries`, `delivery_db.drivers`
- **Kafka:**
  - Produces: `delivery.assigned`, `delivery.completed`
  - Consumes: `payments.completed`
- **Endpoints:** Register driver, list drivers, list deliveries, complete delivery

### 6. Notification Service (Port 8086)

Logs every event consumed from Kafka for auditing.

- **Database:** `notification_db.notifications`
- **Kafka:** Consumes **all 8 topics**
- **Endpoints:** Read-only

### 7. Admin Service (Port 8087)

Generates aggregate reports by reading other services' databases.

- **Database:** None (reads `order_db`, `delivery_db`, `restaurant_db`)
- **Kafka:** None
- **Endpoints:** GET reports for orders, deliveries, drivers, restaurants

## Event-Driven Coordination

Services never call each other directly. They communicate exclusively through Kafka topics.

### Why Events Instead of Direct Calls?

| Concern | Direct REST Calls | Kafka Events |
|---|---|---|
| Coupling | Tight — caller knows callee URL | Loose — producer only knows topic |
| Availability | Caller fails if callee is down | Messages wait in Kafka until consumed |
| Scalability | Synchronous — blocks caller | Asynchronous — non-blocking |
| Fan-out | Caller must call each subscriber | One event, many consumers |
| Testing | Requires both services running | Producers and consumers tested separately |

### The Order Lifecycle — One POST Fires the Whole Chain

    1. Customer POSTs /api/v1/orders to Order Service
    2. Order Service writes order with status=CREATED, publishes orders.created
    3. Payment Service consumes orders.created
       → processes payment
       → publishes payments.completed
    4. Order Service consumes payments.completed → status=CONFIRMED
    5. Delivery Service consumes payments.completed
       → finds available driver
       → publishes delivery.assigned
    6. Order Service consumes delivery.assigned → status=OUT_FOR_DELIVERY
    7. Driver completes delivery → POST /api/v1/deliveries/{orderId}/complete
    8. Delivery Service publishes delivery.completed
    9. Order Service consumes delivery.completed → status=DELIVERED
    10. Notification Service has logged every event along the way

All 4 state transitions happen without any service knowing the internal implementation of another.

## Technology Choices

| Layer | Choice | Why |
|---|---|---|
| Language | Ballerina 2201.13.4 | Built-in support for HTTP services, Kafka, observability. Cloud-native by design. |
| Message Bus | Apache Kafka 7.6.0 | Industry standard, high throughput, persistent event log, partition-based scalability. |
| Database | MongoDB 7.0 | Schema-flexible, natural fit for JSON documents, per-service isolation. |
| Containers | Docker + Docker Compose | Simple multi-container orchestration, reproducible environments. |
| Metrics | Prometheus + Grafana | Industry-standard observability stack. Auto-discovered by Ballerina. |

## Deployment Model

### Docker Compose

All 15 containers defined in a single `docker-compose.yml`:

    Infrastructure:
      - zookeeper       (Kafka's coordinator)
      - kafka           (Event broker)
      - mongodb         (Database)
      - kafka-ui        (Topic browser)
      - mongo-express   (Database browser)

    Services:
      - customer-service
      - restaurant-service
      - order-service
      - payment-service
      - delivery-service
      - notification-service
      - admin-service

    Observability:
      - prometheus      (Metrics scraper)
      - grafana         (Dashboards)

All containers share the `fdp-network` bridge network. Services resolve each other by container name (e.g., `kafka:9092`, `mongodb:27017`).

### Startup Order

Docker Compose honors `depends_on` conditions:

    zookeeper (healthy)
      └─► kafka (healthy)
            ├─► customer-service
            ├─► restaurant-service
            ├─► order-service
            ├─► payment-service
            ├─► delivery-service
            ├─► notification-service
            └─► admin-service

Every service has `restart: unless-stopped` — if a service crashes at startup (e.g., Kafka not ready), Docker automatically restarts it until it succeeds.

### Network Isolation

- **Internal traffic:** services use container names (`kafka:9092`).
- **External traffic:** clients use `localhost:<port>` (mapped through Docker's port forwarding).
- **Kafka dual listeners:** internal clients use `kafka:9092`, external clients use `localhost:29092`.

## Fault Tolerance

| Failure | Behaviour |
|---|---|
| A service crashes | Docker restarts it. Kafka messages persist. No data lost. |
| Kafka broker is down | Producers retry. Services continue to serve REST traffic. |
| MongoDB is down | Services return errors but stay up. Data is preserved in the volume. |
| A consumer falls behind | Kafka retains messages for 7 days. Consumer catches up when it recovers. |
| A bad message is published | The consumer logs an error and continues with the next message. |

## Security Notes

This is a **development deployment**. For production, you would add:

- HTTPS termination (reverse proxy like Traefik or Nginx)
- Authentication and authorization (JWT, OAuth2)
- Encrypted Kafka traffic (SASL/SSL)
- MongoDB authentication with restricted user roles
- Secret management (Vault, Docker Secrets)
- Network segmentation (separate networks for services vs. admin tools)

## Scaling

### Horizontal scaling

Each Ballerina service is stateless with respect to HTTP (state lives in MongoDB). You can run multiple instances:

    docker compose up -d --scale order-service=3

But — **you must set a unique Kafka group ID per instance**, otherwise they compete for the same consumer group and each message goes to only one instance. This is already the design: each service uses its own consumer group ID.

### Partition scaling

Kafka topics currently have 3 partitions. Up to 3 consumers **per consumer group** can process messages in parallel. To increase throughput, add more partitions:

    docker exec fdp-kafka kafka-topics --bootstrap-server localhost:9092 \
      --alter --topic orders.created --partitions 6

## Directory Structure

    food-delivery-platform/
    ├── docker-compose.yml
    ├── README.md
    ├── .gitignore
    ├── admin-service/
    │   ├── Ballerina.toml
    │   ├── Config.toml
    │   ├── Database.bal
    │   ├── Dockerfile
    │   ├── .dockerignore
    │   └── service.bal
    ├── customer-service/       (same structure)
    ├── delivery-service/
    ├── notification-service/
    ├── order-service/
    ├── payment-service/
    ├── restaurant-service/
    ├── docs/
    │   ├── architecture.md          (this file)
    │   ├── architecture-diagram.png
    │   ├── api-endpoints.md
    │   ├── database-schemas.md
    │   ├── kafka-topics.md
    │   ├── observability.md
    │   └── order-state-machine.md
    ├── monitoring/
    │   ├── prometheus/
    │   │   └── prometheus.yml
    │   └── grafana/
    │       └── provisioning/
    │           ├── datasources/prometheus.yml
    │           └── dashboards/dashboard.yml
    ├── scripts/
    │   └── reset-demo-data.ps1
    └── kafka/
        └── create-topics.sh

## Running the Platform

Full instructions are in `README.md`. Quick version:

    git clone https://github.com/likius12/DSA-Assignment-2.git
    cd DSA-Assignment-2/food-delivery-platform
    docker compose up -d
    # wait ~90 seconds
    docker compose ps    # all 15 containers should be Up / Healthy

Then:
- Kafka UI:    http://localhost:8080
- Mongo Express: http://localhost:8088
- Prometheus:  http://localhost:9090/targets
- Grafana:     http://localhost:3000

## Summary

The Food Delivery Platform demonstrates:

- **7 independent microservices** with clear functional boundaries
- **Event-driven coordination** using 8 Kafka topics
- **Data isolation** with per-service MongoDB databases
- **A validated state machine** for the order lifecycle
- **Full observability** with Prometheus and Grafana
- **Container orchestration** with Docker Compose
- **Fault tolerance** via restart policies and message persistence

It is production-shaped, locally deployable, and covers every requirement of the assignment rubric.