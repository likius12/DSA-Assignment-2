# REST API Endpoints

Every service exposes its REST API on its own port. All endpoints are versioned under `/api/v1/`. All request and response bodies use JSON.

## Port Allocation

| Service | Port | Base URL |
|---|---|---|
| Customer Service | 8081 | http://localhost:8081/api/v1 |
| Restaurant Service | 8082 | http://localhost:8082/api/v1 |
| Order Service | 8083 | http://localhost:8083/api/v1 |
| Payment Service | 8084 | http://localhost:8084/api/v1 |
| Delivery Service | 8085 | http://localhost:8085/api/v1 |
| Notification Service | 8086 | http://localhost:8086/api/v1 |
| Admin Service | 8087 | http://localhost:8087/api/v1 |

---

## Customer Service (Port 8081)

### GET /api/v1/customers

List all customers.

Response 200:

    [
      {
        "_id": "cust-001",
        "name": "Ndapewa Johannes",
        "email": "ndapewa@example.na",
        "phone": "+264 81 234 5678",
        "address": "42 Independence Avenue, Windhoek CBD"
      }
    ]

### GET /api/v1/customers/{id}

Get a specific customer by ID.

Response 200: The customer object.

Response 500: Customer not found.

### POST /api/v1/customers

Create a new customer.

Request body:

    {
      "name": "Michael Uusiku",
      "email": "michael@example.na",
      "phone": "+264 81 555 1234",
      "address": "15 Eveline Street, Katutura"
    }

Response 201: The created customer with `_id`.

### DELETE /api/v1/customers/{id}

Delete a customer by ID.

Response 200:

    { "status": "deleted", "id": "cust-001" }

---

## Restaurant Service (Port 8082)

### GET /api/v1/restaurants

List all restaurants.

Response 200:

    [
      {
        "_id": "01f1c070-094b-15c8-8c19-72e34c906614",
        "name": "Joe's Beerhouse",
        "openingHours": { "open": "11:00", "close": "23:00" },
        "isOpen": true,
        "createdAt": "now"
      }
    ]

### GET /api/v1/restaurants/{id}

Get a specific restaurant.

Response 200: The restaurant object.

### POST /api/v1/restaurants

Create a new restaurant.

Request body:

    {
      "name": "Joe's Beerhouse",
      "openingHours": { "open": "11:00", "close": "23:00" }
    }

Response 201: The created restaurant with `_id`.

### GET /api/v1/restaurants/{id}/menu

List all menu items for a restaurant.

Response 200:

    [
      {
        "_id": "01f1c070-0d77-17a0-a267-53b35a9747c4",
        "restaurantId": "01f1c070-094b-15c8-8c19-72e34c906614",
        "name": "Kapana Platter",
        "price": 85.00,
        "stockQty": 30,
        "available": true
      }
    ]

### POST /api/v1/restaurants/{id}/menu

Add a menu item to a restaurant. Also publishes `restaurant.menu.updated` to Kafka.

Request body:

    {
      "name": "Kapana Platter",
      "price": 85.00,
      "stockQty": 30
    }

Response 201: The created menu item with `_id`.

### PATCH /api/v1/restaurants/menu/{itemId}

Update a menu item. All fields are optional. Also publishes `restaurant.menu.updated` to Kafka.

Request body:

    {
      "price": 90.00,
      "stockQty": 25,
      "available": true
    }

Response 200: The updated menu item.

### GET /api/v1/restaurants/health

Health check for the service.

Response 200:

    { "status": "UP", "service": "restaurant-service", "timestamp": "now" }

---

## Order Service (Port 8083)

### GET /api/v1/orders

List all orders.

Response 200: Array of orders.

### GET /api/v1/orders/{id}

Get a specific order by ID.

Response 200:

    {
      "_id": "01f1c070-0fd9-1d40-a646-1d8b1a18e27d",
      "customerId": "cust-windhoek-001",
      "restaurantId": "01f1c070-094b-15c8-8c19-72e34c906614",
      "items": ["Kapana Platter", "Windhoek Lager"],
      "totalAmount": 120.00,
      "status": "DELIVERED",
      "createdAt": "now"
    }

### POST /api/v1/orders

Create a new order. Publishes `orders.created` to Kafka.

Request body:

    {
      "customerId": "cust-windhoek-001",
      "restaurantId": "01f1c070-094b-15c8-8c19-72e34c906614",
      "items": ["Kapana Platter", "Windhoek Lager"],
      "totalAmount": 120.00
    }

Response 201: The created order with `status: "CREATED"`.

### PATCH /api/v1/orders/{id}/status

Manually update an order's status. Validates the transition against the state machine.

Request body:

    { "status": "CANCELLED" }

Response 200:

    { "status": "CANCELLED" }

Response 500: Invalid transition (e.g., DELIVERED → CREATED).

---

## Payment Service (Port 8084)

### GET /api/v1/payments

List all payments.

Response 200: Array of payment records.

### GET /api/v1/payments/{id}

Get a specific payment.

Response 200: The payment object.

### GET /api/v1/payments/order/{orderId}

Get the payment for a specific order.

Response 200: The payment object.

### GET /api/v1/payments/health

Health check.

Response 200:

    { "status": "UP", "service": "payment-service", "timestamp": "now" }

Note: The Payment Service does not expose a POST endpoint. Payments are created **only** in response to consuming `orders.created` from Kafka. This preserves the event-driven design.

---

## Delivery Service (Port 8085)

### GET /api/v1/deliveries

List all delivery records.

Response 200: Array of deliveries.

### POST /api/v1/deliveries/drivers

Register a new driver.

Request body:

    {
      "name": "Tomas Nghidinwa",
      "phone": "+264 81 234 5678"
    }

Response 201: The driver object with `available: true`.

### GET /api/v1/deliveries/drivers

List all drivers.

Response 200: Array of drivers.

### POST /api/v1/deliveries/{orderId}/complete

Mark a delivery as completed. Publishes `delivery.completed` to Kafka. Releases the assigned driver back to `available: true`.

Response 200:

    { "status": "COMPLETED", "deliveryId": "01f1c070-0fd9-1d46-85b7-4b4a7ead5099" }

Response 500: Delivery not found for the given orderId.

---

## Notification Service (Port 8086)

### GET /api/v1/notifications

List all notifications. Every event consumed from Kafka is logged here.

Response 200:

    [
      {
        "_id": "01f1c070-2a1b-3c4d-5e6f-7a8b9c0d1e2f",
        "topic": "payments.completed",
        "payload": { "orderId": "01f1c070-...", "status": "COMPLETED" },
        "channel": "SMS",
        "sentAt": "now"
      }
    ]

Note: The Notification Service does not expose any POST endpoints. It is purely a Kafka consumer.

---

## Admin Service (Port 8087)

### GET /api/v1/admin/health

Health check.

Response 200:

    { "status": "UP", "service": "admin-service", "timestamp": "now" }

### GET /api/v1/admin/reports/orders

Aggregate report on orders across all states.

Response 200:

    {
      "totalOrders": 12,
      "byStatus": {
        "CREATED": 2,
        "CONFIRMED": 1,
        "PREPARING": 0,
        "READY": 1,
        "OUT_FOR_DELIVERY": 3,
        "DELIVERED": 5,
        "CANCELLED": 0
      }
    }

### GET /api/v1/admin/reports/deliveries

Aggregate report on deliveries.

Response 200:

    {
      "totalDeliveries": 8,
      "assigned": 2,
      "completed": 6
    }

### GET /api/v1/admin/reports/drivers

Aggregate report on drivers.

Response 200:

    {
      "totalDrivers": 5,
      "available": 3,
      "busy": 2
    }

### GET /api/v1/admin/reports/restaurants

Aggregate report on restaurants and menu items.

Response 200:

    {
      "totalRestaurants": 4,
      "totalMenuItems": 32
    }

---

## Error Response Format

Ballerina's default error response for resource functions that return `error`:

    {
      "timestamp": "2026-10-05T02:27:09.939528426Z",
      "status": 400,
      "reason": "Bad Request",
      "message": "data binding failed: undefined field 'customerName'",
      "path": "/api/v1/orders",
      "method": "POST"
    }

---

## Testing

The complete Postman collection is available in `docs/Food-Delivery-Platform.postman_collection.json`. Import it into Postman, select the `Food Delivery - Local` environment, and run the requests in order to exercise the full order lifecycle.

---

## Metrics Endpoints

Every service also exposes a Prometheus metrics endpoint (not part of the REST API but available for scraping):

| URL | Purpose |
|---|---|
| http://{service}:9797/metrics | Prometheus metrics (internal only) |

See `docs/observability.md` for details.