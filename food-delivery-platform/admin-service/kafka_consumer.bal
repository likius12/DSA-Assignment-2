import ballerina/log;
import ballerinax/kafka;

final kafka:ConsumerConfiguration consumerConfig = {
    groupId: kafkaGroupId,
    topics: [
        "orders.created",
        "orders.confirmed",
        "orders.cancelled",
        "payments.completed",
        "delivery.assigned",
        "delivery.completed"
    ],
    pollingInterval: 1,
    autoCommit: true
};
public function startKafkaConsumer() returns error? {
    kafka:Consumer consumer = check new (kafkaBootstrapServers, consumerConfig);
    log:printInfo("Kafka consumer started",
        bootstrap = kafkaBootstrapServers,
        groupId = kafkaGroupId);

    // Spawn a detached strand for the polling loop, then return immediately
    _ = start pollLoop(consumer);
}

function pollLoop(kafka:Consumer consumer) returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check consumer->poll(1);
        foreach var rec in records {
            string topic = rec.offset.partition.topic;

            // Kafka delivers raw bytes — decode to JSON
            json payload;
            anydata rawValue = rec.value;
            if rawValue is byte[] {
                string text = check string:fromBytes(rawValue);
                payload = check text.fromJsonString();
            } else if rawValue is string {
                payload = check rawValue.fromJsonString();
            } else if rawValue is json {
                payload = rawValue;
            } else {
                log:printError("Unsupported Kafka payload type");
                continue;
            }

            log:printInfo("Admin event", topic = topic);
            dispatch(topic, payload);
        }
    }
}

function dispatch(string topic, json payload) {
    if topic == "orders.created" {
        handleOrderCreatedEvent(payload);
    } else if topic == "orders.confirmed" {
        handleOrderConfirmedEvent(payload);
    } else if topic == "orders.cancelled" {
        handleOrderCancelledEvent(payload);
    } else if topic == "payments.completed" {
        handlePaymentCompletedEvent(payload);
    } else if topic == "delivery.assigned" {
        handleDeliveryAssignedEvent(payload);
    } else if topic == "delivery.completed" {
        handleDeliveryCompletedEvent(payload);
    }
}

//---- Handlers ----------
function handleOrderCreatedEvent(json p) {
    OrderCreatedEvent|error e = p.cloneWithType(OrderCreatedEvent);
    if e is error {
        log:printError("Bad orders.created payload", e);
        return;
    }
    error? r = recordOrderCreated(e);
    if r is error {
        log:printError("recordOrderCreated failed", r);
    }
}

function handleOrderConfirmedEvent(json p) {
    OrderConfirmedEvent|error e = p.cloneWithType(OrderConfirmedEvent);
    if e is error {
        log:printError("Bad orders.confirmed payload", e);
        return;
    }
    error? r1 = updateOrderStatus(e.orderId, "CONFIRMED");
    error? r2 = upsertRestaurantStats(e.restaurantId, 1, e.totalAmount, false, false);
    if r1 is error { log:printError("updateOrderStatus failed", r1); }
    if r2 is error { log:printError("upsertRestaurantStats failed", r2); }
}

function handleOrderCancelledEvent(json p) {
    OrderCancelledEvent|error e = p.cloneWithType(OrderCancelledEvent);
    if e is error {
        log:printError("Bad orders.cancelled payload", e);
        return;
    }
    error? r1 = updateOrderStatus(e.orderId, "CANCELLED");
    error? r2 = upsertRestaurantStats(e.restaurantId, 1, 0, false, true);
    if r1 is error { log:printError("updateOrderStatus failed", r1); }
    if r2 is error { log:printError("upsertRestaurantStats failed", r2); }
}

function handlePaymentCompletedEvent(json p) {
    PaymentCompletedEvent|error e = p.cloneWithType(PaymentCompletedEvent);
    if e is error {
        log:printError("Bad payments.completed payload", e);
        return;
    }
    error? r = recordDailyRevenue(e.amount, false);
    if r is error { log:printError("recordDailyRevenue failed", r); }
}

function handleDeliveryAssignedEvent(json p) {
    DeliveryAssignedEvent|error e = p.cloneWithType(DeliveryAssignedEvent);
    if e is error {
        log:printError("Bad delivery.assigned payload", e);
        return;
    }
    error? r = updateOrderStatus(e.orderId, "OUT_FOR_DELIVERY", e.driverId);
    if r is error { log:printError("updateOrderStatus failed", r); }
}

function handleDeliveryCompletedEvent(json p) {
    DeliveryCompletedEvent|error e = p.cloneWithType(DeliveryCompletedEvent);
    if e is error {
        log:printError("Bad delivery.completed payload", e);
        return;
    }
    error? r1 = updateOrderStatus(e.orderId, "DELIVERED", e.driverId);
    error? r2 = upsertDeliveryPerformance(e.driverId, true, e.durationMinutes);
    if r1 is error { log:printError("updateOrderStatus failed", r1); }
    if r2 is error { log:printError("upsertDeliveryPerformance failed", r2); }
}
