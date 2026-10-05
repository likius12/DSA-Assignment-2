# Order State Machine

The Order Service is the **single source of truth** for the state of every order. No other service changes an order's status directly. Instead, other services publish events to Kafka, and the Order Service reacts by transitioning the order's state.

## States

An order can be in exactly one of 7 states at any time:

| State | Description | Set By |
|---|---|---|
| CREATED | The order has been placed by a customer. | POST /api/v1/orders |
| CONFIRMED | Payment has been successfully processed. | Consuming payments.completed |
| PREPARING | The restaurant is preparing the food. (Manual — reserved for future use.) | PATCH /api/v1/orders/{id}/status |
| READY | The food is ready for pickup. (Manual — reserved for future use.) | PATCH /api/v1/orders/{id}/status |
| OUT_FOR_DELIVERY | A driver has been assigned and is en route. | Consuming delivery.assigned |
| DELIVERED | The order has been successfully delivered. | Consuming delivery.completed |
| CANCELLED | The order was cancelled before delivery. | Consuming payments.failed, or manual PATCH |

## Valid Transitions

Not every state can transition to every other state. The Order Service enforces this rule set:

    CREATED           → CONFIRMED, CANCELLED
    CONFIRMED         → PREPARING, CANCELLED
    PREPARING         → READY, CANCELLED
    READY             → OUT_FOR_DELIVERY, CANCELLED
    OUT_FOR_DELIVERY  → DELIVERED
    DELIVERED         → (terminal — no further transitions)
    CANCELLED         → (terminal — no further transitions)

## State Diagram

    ┌─────────┐
    │ CREATED │◄── POST /orders
    └────┬────┘
         │
         │ payments.completed
         ▼
    ┌───────────┐
    │ CONFIRMED │
    └─────┬─────┘
          │
          │ (manual, reserved)
          ▼
    ┌───────────┐
    │ PREPARING │
    └─────┬─────┘
          │ (manual, reserved)
          ▼
    ┌───────┐
    │ READY │
    └───┬───┘
        │
        │ delivery.assigned
        ▼
    ┌──────────────────┐
    │ OUT_FOR_DELIVERY │
    └────────┬─────────┘
             │
             │ delivery.completed
             ▼
        ┌───────────┐
        │ DELIVERED │  (terminal)
        └───────────┘

    CANCELLED (terminal) reachable from:
    CREATED, CONFIRMED, PREPARING, READY

## How Transitions Happen

### Automatic — via Kafka events

The Order Service runs two Kafka consumers that trigger transitions automatically:

| Kafka Event Consumed | Current State | New State |
|---|---|---|
| payments.completed | CREATED | CONFIRMED |
| payments.failed | CREATED | CANCELLED |
| delivery.assigned | CONFIRMED | OUT_FOR_DELIVERY |
| delivery.completed | OUT_FOR_DELIVERY | DELIVERED |

The consumer code looks up the order by `orderId` (extracted from the event payload), updates the status field, and logs the transition:

    Order 01f1c070-0fd9-1d40-a646-1d8b1a18e27d -> CONFIRMED
    Order 01f1c070-0fd9-1d40-a646-1d8b1a18e27d -> OUT_FOR_DELIVERY
    Order 01f1c070-0fd9-1d40-a646-1d8b1a18e27d -> DELIVERED

### Manual — via REST

For states that require human action (like cancelling), the Order Service exposes:

    PATCH /api/v1/orders/{id}/status
    Body: { "status": "CANCELLED" }

The service validates the transition against the rule set above. If the requested transition is not allowed, the API returns an error:

    { "error": "Invalid transition: DELIVERED -> CREATED" }

## Why This Design?

### Single source of truth

Only the Order Service writes to the `orders` collection. Other services (Payment, Delivery) react to events and publish their own events — they never touch order state directly.

This means:
- No conflicting updates.
- No distributed locking needed.
- Any service can be rebuilt and replay the same events without corrupting state.

### Event-driven coordination

Services don't call each other. The Order Service doesn't know that Payment Service exists — it just publishes `orders.created` and reacts to `payments.completed`. This makes the system:

- **Loosely coupled** — you can replace the Payment Service with a different implementation.
- **Scalable** — you can run multiple instances of any service.
- **Fault-tolerant** — if Payment Service is down, its messages wait in Kafka and are processed when it recovers.

### Validated transitions

Invalid transitions are rejected at both the API layer and (implicitly) the consumer layer. An order cannot jump from CREATED to DELIVERED, for example.

## Full Lifecycle — Timestamps From a Live Test

Here is a real trace from a live demo run:

    time=2026-10-05T03:51:36.571Z  Order created           → CREATED
    time=2026-10-05T03:51:36.857Z  Payment consumed        → CONFIRMED
    time=2026-10-05T03:51:37.054Z  Driver assigned         → OUT_FOR_DELIVERY
    time=2026-10-05T04:00:09.060Z  Delivery completed      → DELIVERED

All 4 transitions in the pipeline fire automatically **except the final manual "complete"** step, which is called by the driver (or the demo operator via Postman).

The end-to-end chain took ~3 seconds for the first 3 transitions (limited only by Kafka consumer polling interval), and the final delivery completed when the driver confirmed.

## States Not Currently Used

- **PREPARING** and **READY** are reserved for future integration with the Kitchen Service — where the restaurant would publish `kitchen.preparing` and `kitchen.ready` events. The state machine already supports them.

## Related Files

- `docs/kafka-topics.md` — which events cause which transitions
- `docs/api-endpoints.md` — the PATCH endpoint for manual transitions
- `order-service/service.bal` — the actual implementation