# Food Delivery Platform

A distributed, event-driven food delivery platform built with **Ballerina**, **Apache Kafka**, **MongoDB**, and **Docker Compose**.

Built for the Ministry of Industrialisation and Trade to support Namibian SMEs — restaurants and delivery drivers.

## What This Is

Seven independent microservices that coordinate asynchronously through Kafka to process a food order end to end:

    Order placed  →  Payment processed  →  Driver assigned  →  Delivered

Each service has its own database, its own REST API, and its own Kafka producers/consumers. No service calls another directly — they communicate only through events.

## Architecture at a Glance

| Service | Port | Purpose |
|---|---|---|
| customer-service | 8081 | Customer accounts and addresses |
| restaurant-service | 8082 | Restaurant menus and inventory |
| order-service | 8083 | Order state machine (single source of truth) |
| payment-service | 8084 | Payment processing (event-driven) |
| delivery-service | 8085 | Driver assignment and delivery tracking |
| notification-service | 8086 | Event log for all notifications |
| admin-service | 8087 | Aggregate reports and statistics |

Plus the infrastructure:

| Container | Port | Purpose |
|---|---|---|
| kafka | 9092 / 29092 | Event broker |
| zookeeper | 2181 | Kafka coordinator |
| mongodb | 27017 | Data persistence |
| kafka-ui | 8080 | Kafka browser |
| mongo-express | 8088 | MongoDB browser |
| prometheus | 9090 | Metrics collector |
| grafana | 3000 | Dashboards |

Full details in docs/architecture.md.

## Prerequisites

- Docker Desktop 4.x or later
- Docker Compose v2 (included with Docker Desktop)
- 8 GB RAM allocated to Docker
- Git

You do not need to install Ballerina, Kafka, or MongoDB locally. Everything runs in containers.

## Quick Start

    git clone https://github.com/likius12/DSA-Assignment-2.git
    cd DSA-Assignment-2/food-delivery-platform
    docker compose up -d
    Start-Sleep -Seconds 90
    docker compose ps

Expected: 15 containers, all showing Up or (healthy).

## Verify It Works

| URL | What You Should See |
|---|---|
| http://localhost:8080 | Kafka UI with 8 topics |
| http://localhost:8088 | Mongo Express with 7 databases |
| http://localhost:9090/targets | Prometheus showing 8 targets UP |
| http://localhost:3000 | Grafana login (admin / admin) |

## Place Your First Order

You need a REST client. Postman or curl both work.

### 1. Create a restaurant

    curl -X POST http://localhost:8082/api/v1/restaurants -H "Content-Type: application/json" -d "{\"name\":\"Joes Beerhouse\",\"openingHours\":{\"open\":\"11:00\",\"close\":\"23:00\"}}"

Copy the _id from the response — this is your RESTAURANT_ID.

### 2. Add a menu item

    curl -X POST http://localhost:8082/api/v1/restaurants/RESTAURANT_ID/menu -H "Content-Type: application/json" -d "{\"name\":\"Kapana Platter\",\"price\":85.00,\"stockQty\":30}"

### 3. Register a driver

    curl -X POST http://localhost:8085/api/v1/deliveries/drivers -H "Content-Type: application/json" -d "{\"name\":\"Tomas Nghidinwa\",\"phone\":\"+264 81 234 5678\"}"

### 4. Place an order

    curl -X POST http://localhost:8083/api/v1/orders -H "Content-Type: application/json" -d "{\"customerId\":\"cust-001\",\"restaurantId\":\"RESTAURANT_ID\",\"items\":[\"Kapana Platter\"],\"totalAmount\":85.00}"

Copy the _id — this is your ORDER_ID.

### 5. Complete the delivery

    curl -X POST http://localhost:8085/api/v1/deliveries/ORDER_ID/complete

### 6. Verify the order is DELIVERED

    curl http://localhost:8083/api/v1/orders/ORDER_ID

Expected: "status": "DELIVERED".

The full chain fired. One POST to orders, three automatic Kafka events, and a manual delivery completion — the order moved through all four states.

## Postman Collection

A ready-to-use Postman collection is included in docs/:

- Food-Delivery-Platform.postman_collection.json
- Food-Delivery-Platform.postman_environment.json

Import both into Postman, select the Food Delivery - Local environment, and run the requests in order (1 → 7). IDs auto-populate — no manual copy-pasting.

## Full Order Lifecycle

    CREATED → CONFIRMED → OUT_FOR_DELIVERY → DELIVERED

| State | Triggered By |
|---|---|
| CREATED | POST /api/v1/orders |
| CONFIRMED | Consuming payments.completed |
| OUT_FOR_DELIVERY | Consuming delivery.assigned |
| DELIVERED | Consuming delivery.completed |

The Order Service validates every transition against a rule set. Invalid transitions are rejected.

Full details in docs/order-state-machine.md.

## Observability

Every service exposes Prometheus metrics on port 9797. Prometheus scrapes them every 15 seconds. Grafana visualizes them.

Open http://localhost:3000 (admin / admin) and view the Food Delivery Overview dashboard. Place an order and watch the HTTP request count spike in real time.

Full details in docs/observability.md.

## Documentation

| Document | What It Covers |
|---|---|
| docs/architecture.md | Full system overview, design decisions, technology choices |
| docs/api-endpoints.md | Every REST endpoint across all 7 services |
| docs/kafka-topics.md | All 8 Kafka topics, producers, consumers, partitions |
| docs/database-schemas.md | MongoDB collections, fields, and ownership |
| docs/order-state-machine.md | The order lifecycle and valid transitions |
| docs/observability.md | Prometheus and Grafana setup |

## Common Commands

    docker compose up -d
    docker compose stop
    docker compose down
    docker compose down -v
    docker compose restart order-service
    docker logs fdp-order-service --tail 50 -f
    docker compose build --no-cache order-service
    docker compose up -d order-service

## Troubleshooting

| Symptom | Fix |
|---|---|
| A service shows Exit or keeps restarting | docker logs fdp-SERVICE to see the error. Kafka may still be booting — wait 30s. |
| Port already in use | Another app is using 8081-8088. Change the host port in docker-compose.yml. |
| Kafka UI shows no cluster | Wait 60s for Kafka to become healthy, then refresh. |
| Prometheus targets DOWN | Wait 30s after startup; the metrics endpoint takes time to boot. |
| git push fails with RPC failed | You are pushing large files. Check .gitignore covers target/, *.jar, Dependencies.toml. |
| Grafana shows No data | Change time range to Last 15 minutes. |

## Tech Stack

- Ballerina 2201.13.4 — microservices
- Apache Kafka 7.6.0 — event streaming
- MongoDB 7.0 — persistence
- Docker + Docker Compose — orchestration
- Prometheus — metrics
- Grafana — dashboards

## Team

Group project for DSA Assignment 2.

## License

Academic project — not for commercial use.