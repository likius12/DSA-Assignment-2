# Database Schemas

The Food Delivery Platform uses **MongoDB** for persistence. Every microservice owns its own database — no service reads or writes another service's collections directly. Cross-service coordination happens exclusively through Kafka events.

This gives us:

- **Data isolation** — a change to one service's schema never breaks another service.
- **Independent scaling** — each database can be scaled separately.
- **Clear ownership** — every field has exactly one service responsible for it.

## Database Overview

| Database | Owner | Collections |
|---|---|---|
| customer_db | Customer Service | customers |
| restaurant_db | Restaurant Service | restaurants, menuItems |
| order_db | Order Service | orders |
| payment_db | Payment Service | payments |
| delivery_db | Delivery Service | deliveries, drivers |
| notification_db | Notification Service | notifications |
| admin_db | Admin Service | (none — reads other DBs for reports) |

All databases live inside a single MongoDB instance (`fdp-mongodb`, port 27017) for development simplicity. In production, each would be a separate MongoDB cluster.

---

## customer_db

### Collection: customers

Stores customer accounts and delivery addresses.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Customer ID |
| name | string | yes | Full name |
| email | string | yes | Email address |
| phone | string | yes | Phone number |
| address | string | yes | Delivery address |
| createdAt | string | yes | Timestamp (or "now" placeholder in dev) |

Example document:

{
    "_id": "01f1c070-094b-15c8-8c19-72e34c906614",
    "name": "Ndapewa Johannes",
    "email": "ndapewa@example.na",
    "phone": "+264 81 234 5678",
    "address": "42 Independence Avenue, Windhoek CBD",
    "createdAt": "now"
}

---

## restaurant_db

### Collection: restaurants

Stores restaurant profiles and opening hours.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Restaurant ID |
| name | string | yes | Restaurant name |
| openingHours | object | yes | { open: "HH:MM", close: "HH:MM" } |
| isOpen | boolean | yes | Current open state |
| createdAt | string | yes | Timestamp |

Example document:

{
    "_id": "01f1c070-094b-15c8-8c19-72e34c906614",
    "name": "Joe's Beerhouse",
    "openingHours": { "open": "11:00", "close": "23:00" },
    "isOpen": true,
    "createdAt": "now"
}

### Collection: menuItems

Stores individual menu items per restaurant.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Menu item ID |
| restaurantId | string | yes | Foreign key to restaurants._id |
| name | string | yes | Item name |
| price | decimal | yes | Price in NAD (Namibian Dollar) |
| stockQty | int | yes | Current stock quantity |
| available | boolean | yes | Whether item is orderable |
| createdAt | string | yes | Timestamp |

Example document:

{
    "_id": "01f1c070-0d77-17a0-a267-53b35a9747c4",
    "restaurantId": "01f1c070-094b-15c8-8c19-72e34c906614",
    "name": "Kapana Platter",
    "price": 85.00,
    "stockQty": 30,
    "available": true,
    "createdAt": "now"
}

---

## order_db

### Collection: orders

The core of the system. Stores order lifecycle state.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Order ID |
| customerId | string | yes | Foreign key to customers._id |
| restaurantId | string | yes | Foreign key to restaurants._id |
| items | string[] | yes | List of item names |
| totalAmount | decimal | yes | Total price in NAD |
| status | string (enum) | yes | One of: CREATED, CONFIRMED, PREPARING, READY, OUT_FOR_DELIVERY, DELIVERED, CANCELLED |
| createdAt | string | yes | Timestamp |
| updatedAt | string | no | Last state change |

Example document:

{
    "_id": "01f1c070-0fd9-1d40-a646-1d8b1a18e27d",
    "customerId": "cust-windhoek-001",
    "restaurantId": "01f1c070-094b-15c8-8c19-72e34c906614",
    "items": ["Kapana Platter", "Windhoek Lager"],
    "totalAmount": 120.00,
    "status": "DELIVERED",
    "createdAt": "now"
}

**State machine constraint:** Only transitions defined in `docs/order-state-machine.md` are valid. The Order Service validates every transition.

---

## payment_db

### Collection: payments

Stores payment transactions. In this simulated environment, the Payment Service always succeeds, but the schema supports failure states.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Payment ID |
| orderId | string | yes | Foreign key to orders._id |
| customerId | string | yes | Foreign key to customers._id |
| amount | decimal | yes | Amount charged in NAD |
| status | string (enum) | yes | COMPLETED or FAILED |
| transactionId | string | yes | Simulated gateway transaction ID |
| createdAt | string | yes | Timestamp |

Example document:

{
    "_id": "pay_01f1c070-0ab1-1c2d-9e3f-4a5b6c7d8e9f",
    "orderId": "01f1c070-0fd9-1d40-a646-1d8b1a18e27d",
    "customerId": "cust-windhoek-001",
    "amount": 120.00,
    "status": "COMPLETED",
    "transactionId": "txn_01f1c070-1a2b-3c4d-5e6f-7a8b9c0d1e2f",
    "createdAt": "now"
}

---

## delivery_db

### Collection: drivers

Stores registered delivery drivers and their availability.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Driver ID |
| name | string | yes | Full name |
| phone | string | yes | Contact phone |
| available | boolean | yes | Whether driver can be assigned |
| createdAt | string | yes | Timestamp |

Example document:

{
    "_id": "01f1c070-0d77-17a0-a267-53b35a9747c4",
    "name": "Tomas Nghidinwa",
    "phone": "+264 81 234 5678",
    "available": true,
    "createdAt": "now"
}

**Assignment lifecycle:**
1. Driver registers with `available: true`.
2. On `payments.completed` event, Delivery Service picks the first available driver.
3. Driver's `available` is set to `false` (busy).
4. On delivery completion, driver's `available` is reset to `true`.

### Collection: deliveries

Stores delivery records — the assignment of a driver to an order.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Delivery ID |
| orderId | string | yes | Foreign key to orders._id |
| driverId | string | yes | Foreign key to drivers._id |
| status | string (enum) | yes | ASSIGNED or COMPLETED |
| assignedAt | string | yes | When driver was assigned |
| completedAt | string | no | When delivery was completed |

Example document:

{
    "_id": "01f1c070-0fd9-1d46-85b7-4b4a7ead5099",
    "orderId": "01f1c070-0fd9-1d40-a646-1d8b1a18e27d",
    "driverId": "01f1c070-0d77-17a0-a267-53b35a9747c4",
    "status": "COMPLETED",
    "assignedAt": "now",
    "completedAt": "now"
}

---

## notification_db

### Collection: notifications

An append-only log of every event that the Notification Service has processed. Used for auditing and debugging.

| Field | Type | Required | Description |
|---|---|---|---|
| _id | string (UUID v1) | yes | Notification ID |
| topic | string | yes | Kafka topic that triggered this notification |
| payload | object | yes | The event's JSON payload |
| channel | string | yes | Delivery channel (SMS, EMAIL, PUSH) |
| sentAt | string | yes | Timestamp |

Example document:

{
    "_id": "01f1c070-2a1b-3c4d-5e6f-7a8b9c0d1e2f",
    "topic": "payments.completed",
    "payload": {
        "orderId": "01f1c070-0fd9-1d40-a646-1d8b1a18e27d",
        "paymentId": "pay_01f1c070-...",
        "status": "COMPLETED"
    },
    "channel": "SMS",
    "sentAt": "now"
}

---

## admin_db

The Admin Service does **not** store its own data. It reads from the other six databases (order_db, delivery_db, restaurant_db) to generate aggregate reports. This keeps the admin service stateless — a pure query layer.

---

## Data Isolation Rule

Every service owns its database. **No service directly reads or writes another service's collections.** When a service needs to react to another service's data change, it does so via Kafka events.

Example:

- Order Service needs to know when a payment is done → it consumes `payments.completed`.
- Delivery Service needs to know when an order is placed → it consumes `payments.completed` (not `orders.created`) so it only assigns drivers to paid orders.

This is the **database-per-service** pattern — one of the defining characteristics of a microservices architecture.

---

## Backup and Reset

During development and demos, reset all data:

    docker exec fdp-mongodb mongosh -u admin -p admin123 --eval "
      db.getSiblingDB('order_db').orders.deleteMany({});
      db.getSiblingDB('delivery_db').deliveries.deleteMany({});
      db.getSiblingDB('delivery_db').drivers.deleteMany({});
      db.getSiblingDB('payment_db').payments.deleteMany({});
      db.getSiblingDB('notification_db').notifications.deleteMany({});
      db.getSiblingDB('restaurant_db').restaurants.deleteMany({});
      db.getSiblingDB('restaurant_db').menuItems.deleteMany({});
      print('Cleared');
    "

Inspect any database via Mongo Express at http://localhost:8088.