# Observability

The Food Delivery Platform ships with full **observability** out of the box. Every service exposes Prometheus metrics, which are scraped by Prometheus and visualized in Grafana dashboards. This gives real-time visibility into the health and behaviour of the entire distributed system.

## Architecture

    ┌──────────────────────────────────────────────────────────────┐
    │                                                              │
    │   Ballerina Services (each on port 9797)                     │
    │                                                              │
    │   customer-service  ──┐                                      │
    │   restaurant-service ─┤                                      │
    │   order-service      ─┼──► /metrics endpoints                │
    │   payment-service    ─┤                                      │
    │   delivery-service   ─┤                                      │
    │   notification-service┤                                      │
    │   admin-service      ─┘                                      │
    │                                                              │
    └───────────────────────┬──────────────────────────────────────┘
                            │
                            │ HTTP pull every 15s
                            ▼
    ┌──────────────────────────────────────────────────────────────┐
    │                                                              │
    │   Prometheus (port 9090)                                     │
    │   - Scrapes /metrics from every service                      │
    │   - Stores time-series data                                  │
    │   - Provides PromQL query language                           │
    │                                                              │
    └───────────────────────┬──────────────────────────────────────┘
                            │
                            │ Query (PromQL)
                            ▼
    ┌──────────────────────────────────────────────────────────────┐
    │                                                              │
    │   Grafana (port 3000)                                        │
    │   - Dashboards with live charts                              │
    │   - Auto-refresh every 30s                                   │
    │                                                              │
    └──────────────────────────────────────────────────────────────┘

## Why Observability Matters

In a monolithic application, you debug by looking at log files. In a distributed microservices architecture with 7 services, you cannot grep 7 sets of logs to understand what is happening. Observability gives you:

- **Metric aggregation** — one dashboard shows all services.
- **Time-series history** — you can see trends and spikes over time.
- **Cross-service correlation** — see when Order Service spikes and Payment Service follows.
- **Early warnings** — spot memory growth, error rate changes, or consumer lag before they cause failures.

## How Metrics Get Enabled

Each Ballerina service requires **five coordinated pieces** to expose Prometheus metrics. Missing any one silently disables the endpoint.

### 1. Ballerina.toml — enable observability at build

    [build-options]
    observabilityIncluded = true

### 2. Ballerina.toml — add the Prometheus module

    [[dependency]]
    org = "ballerinax"
    name = "prometheus"
    version = "1.0.1"

### 3. service.bal — import the metrics reporter

    import ballerinax/prometheus as _;

### 4. Config.toml — configure the metrics exporter

    [ballerina.observe]
    metricsEnabled = true
    metricsReporter = "prometheus"

    [ballerinax.prometheus]
    port = 9797
    host = "0.0.0.0"

The `host = "0.0.0.0"` is critical — it makes the metrics endpoint reachable from other containers (Prometheus), not just from localhost.

### 5. Dockerfile — build with observability and copy the config

    FROM ballerina/ballerina:2201.13.4 AS build
    WORKDIR /home/ballerina
    COPY . .
    RUN bal build --observability-included

    FROM ballerina/ballerina:2201.13.4
    WORKDIR /home/ballerina
    COPY --from=build /home/ballerina/target/bin/<service>.jar .
    COPY Config.toml .
    EXPOSE <port>
    CMD ["bal", "run", "<service>.jar"]

The `COPY Config.toml .` line is essential — without it, the container has no metrics configuration.

## Prometheus Configuration

Prometheus scrapes each service using the config in `monitoring/prometheus/prometheus.yml`:

    global:
      scrape_interval: 15s
      evaluation_interval: 15s

    scrape_configs:
      - job_name: 'prometheus'
        static_configs:
          - targets: ['localhost:9090']

      - job_name: 'customer-service'
        static_configs:
          - targets: ['customer-service:9797']

      - job_name: 'restaurant-service'
        static_configs:
          - targets: ['restaurant-service:9797']

      - job_name: 'order-service'
        static_configs:
          - targets: ['order-service:9797']

      - job_name: 'payment-service'
        static_configs:
          - targets: ['payment-service:9797']

      - job_name: 'delivery-service'
        static_configs:
          - targets: ['delivery-service:9797']

      - job_name: 'notification-service'
        static_configs:
          - targets: ['notification-service:9797']

      - job_name: 'admin-service'
        static_configs:
          - targets: ['admin-service:9797']

Each target uses Docker's internal DNS — `service-name:9797` — which resolves to the container's IP on the `fdp-network`.

## Metrics Exposed

Every Ballerina service automatically exposes the following metric categories:

| Metric | Type | Description |
|---|---|---|
| inprogress_requests_value | gauge | Currently active HTTP requests |
| requests_total_value | counter | Total HTTP requests received |
| response_time_seconds | summary | Response time percentiles (P50, P95, P99) |
| kafka_publishers_value | gauge | Active Kafka producers |
| kafka_consumers_value | gauge | Active Kafka consumers |
| jvm_memory_used_bytes | gauge | JVM heap usage |
| process_cpu_usage | gauge | CPU usage (0.0 – 1.0) |

These metrics are exposed by the `ballerinax/prometheus` module without any custom code.

## Verifying Metrics Are Live

### Check the metrics endpoint directly

    docker run --rm --network food-delivery-platform_fdp-network curlimages/curl:latest -s http://order-service:9797/metrics | Select-Object -First 10

Expected output begins with:

    # HELP inprogress_requests_value In-progress requests
    # TYPE inprogress_requests_value gauge
    inprogress_requests_value{...} 1.0

### Check Prometheus targets

Open http://localhost:9090/targets. Expected: 8 targets, all **UP**.

### View a specific metric

Open http://localhost:9090/graph. In the query box, type:

    inprogress_requests_value

Then click **Execute**. You will see current values from every service.

## Grafana Dashboard

Grafana is pre-configured with a Prometheus data source via provisioning files in `monitoring/grafana/provisioning/`.

### Access

URL: http://localhost:3000

Credentials: `admin` / `admin`

### Dashboard: "Food Delivery Overview"

The dashboard contains these panels:

| Panel | Query | What It Shows |
|---|---|---|
| HTTP Requests | requests_total_value | Total request count per service |
| Kafka Publishers | kafka_publishers_value | Active producers per service |
| Kafka Consumers | kafka_consumers_value | Active consumers per service |
| Response Time | response_time_seconds | Response latency |

### Adding a New Panel

1. Open the dashboard.
2. Click **Add** → **Visualization**.
3. In the query box, enter a PromQL query (e.g., `rate(requests_total_value[1m])`).
4. Choose a visualization type (Time series, Stat, Gauge, etc.).
5. Give the panel a title.
6. Click **Apply**.

### Demo Scenario

To demonstrate observability during a live defence:

1. Open Grafana in one browser tab.
2. Open Postman in another.
3. Place an order via `POST /api/v1/orders`.
4. Refresh Grafana (or set auto-refresh to 5s).
5. Watch the `HTTP Requests` panel spike from the incoming requests.

This shows the metric pipeline working end-to-end: Ballerina service → Prometheus → Grafana.

## Docker Compose Additions

The observability stack runs as additional containers in `docker-compose.yml`:

    prometheus:
      image: prom/prometheus:latest
      container_name: fdp-prometheus
      ports:
        - "9090:9090"
      volumes:
        - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
        - prometheus-data:/prometheus

    grafana:
      image: grafana/grafana:latest
      container_name: fdp-grafana
      ports:
        - "3000:3000"
      environment:
        GF_SECURITY_ADMIN_USER: admin
        GF_SECURITY_ADMIN_PASSWORD: admin
      volumes:
        - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
        - grafana-data:/var/lib/grafana

Two named volumes (`prometheus-data` and `grafana-data`) preserve metrics and dashboards across container restarts.

## Environment Variables

Each Ballerina service in `docker-compose.yml` includes:

    JAVA_OPTS: "-Dballerina.observe.metrics.port=9797"

This tells the JVM to start the metrics listener on port 9797 when the service boots. The port is only exposed internally (not mapped to the host) since Prometheus scrapes it over the Docker network.

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Prometheus target DOWN — "connection refused" | Service did not expose metrics on 9797 | Check that all 5 requirements above are met, then rebuild with `--no-cache` |
| Prometheus target DOWN — "no such host" | Container is restarting (DNS entry temporarily missing) | Wait 30s; the `restart: unless-stopped` policy will recover it |
| Grafana shows "No data" | Time range too narrow | Change time range to "Last 15 minutes" |
| Metrics missing for one service | Config.toml not copied into container | Rebuild with `docker compose build --no-cache <service>` |

## Access URLs Summary

| Tool | URL |
|---|---|
| Prometheus | http://localhost:9090 |
| Prometheus Targets | http://localhost:9090/targets |
| Prometheus Graph | http://localhost:9090/graph |
| Grafana | http://localhost:3000 |
| Kafka UI | http://localhost:8080 |
| Mongo Express | http://localhost:8088 |