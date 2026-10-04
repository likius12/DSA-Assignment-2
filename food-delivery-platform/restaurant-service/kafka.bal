import ballerina/log;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:9092";
configurable int utcOffsetHours = 2; // Namibia (CAT)

final kafka:Producer producer = check new (kafkaBroker);

listener kafka:Listener orderListener = new (kafkaBroker, {
    groupId: "restaurant-service",
    topics: ["orders.created"]
});

// Consumes orders.created, decides, then publishes the result
service on orderListener {
    remote function onConsumerRecord(OrderCreated[] orders) returns error? {
        foreach OrderCreated o in orders {
            error? res = handleOrder(o);
            if res is error {
                log:printError("Failed handling order " + o.orderId, res);
            }
        }
    }
}

function handleOrder(OrderCreated o) returns error? {
    Restaurant? r = check restaurants->findOne({id: o.restaurantId}, {}, {_id: 0}, Restaurant);
    if r is () {
        return publishDecision("restaurant.order.rejected", o, "REJECTED", "Restaurant not found");
    }
    if !r.isOpen || !(check withinHours(r.openingHours)) {
        return publishDecision("restaurant.order.rejected", o, "REJECTED", "Restaurant is closed");
    }

    // Reserve stock atomically, item by item; roll back if one fails
    OrderItem[] reserved = [];
    foreach OrderItem item in o.items {
        mongodb:UpdateResult res = check menuItems->updateOne(
            {id: item.itemId, restaurantId: o.restaurantId, available: true, stockQty: {"$gte": item.quantity}},
            {inc: {stockQty: -item.quantity}}
        );
        if res.modifiedCount == 0 {
            check releaseStock(reserved);
            return publishDecision("restaurant.order.rejected", o, "REJECTED", "Item unavailable: " + item.itemId);
        }
        reserved.push(item);
    }
    return publishDecision("restaurant.order.confirmed", o, "CONFIRMED");
}

function releaseStock(OrderItem[] items) returns error? {
    foreach OrderItem item in items {
        _ = check menuItems->updateOne({id: item.itemId}, {inc: {stockQty: item.quantity}});
    }
}

function publishDecision(string topic, OrderCreated o, string status, string? reason = ()) returns error? {
    RestaurantDecision d = {orderId: o.orderId, restaurantId: o.restaurantId, status: status};
    if reason is string {
        d.reason = reason;
    }
    // keyed by orderId so one order's events stay in order within a partition
    check producer->send({topic: topic, key: o.orderId, value: d});
    log:printInfo("Published " + topic + " for order " + o.orderId);
}

function publishReady(string orderId) returns error? {
    check producer->send({topic: "orders.ready", key: orderId, value: {orderId: orderId, status: "READY"}});
}

function withinHours(OpeningHours h) returns boolean|error {
    time:Civil now = time:utcToCivil(time:utcNow());
    int current = ((now.hour + utcOffsetHours) % 24) * 60 + now.minute;
    return current >= check toMinutes(h.open) && current < check toMinutes(h.close);
}

function toMinutes(string t) returns int|error {
    int hh = check int:fromString(t.substring(0, 2));
    int mm = check int:fromString(t.substring(3, 5));
    return hh * 60 + mm;
}
