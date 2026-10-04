import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable string KAFKA_BROKER = "kafka:9092";
configurable int PORT = 8083;

mongodb:Client mongoClient = check new ({
    connection: {
        serverAddress: {
            host: "mongodb",
            port: 27017
        },
        auth: <mongodb:ScramSha256AuthCredential>{
            username: "admin",
            password: "admin123",
            database: "admin"
        }
    }
});

type OrderRequest record {|
    string customerId;
    string restaurantId;
    string[] items;
    decimal totalAmount;
|};

type OrderRecord record {|
    string _id;
    string customerId;
    string restaurantId;
    string[] items;
    decimal totalAmount;
    string status;
    string createdAt;
|};

type StatusUpdate record {|
    string status;
|};

final map<string[]> VALID_TRANSITIONS = {
    "CREATED": ["CONFIRMED", "CANCELLED"],
    "CONFIRMED": ["OUT_FOR_DELIVERY", "CANCELLED"],
    "OUT_FOR_DELIVERY": ["DELIVERED"],
    "DELIVERED": [],
    "CANCELLED": []
};

final kafka:Producer orderProducer = check new (KAFKA_BROKER, {
    clientId: "order-service"
});

listener kafka:Listener paymentListener = new (KAFKA_BROKER, {
    groupId: "order-service-payment-group",
    topics: ["payments.completed", "payments.failed"],
    pollingInterval: 1,
    autoCommit: true
});

listener kafka:Listener deliveryListener = new (KAFKA_BROKER, {
    groupId: "order-service-delivery-group",
    topics: ["delivery.assigned", "delivery.completed"],
    pollingInterval: 1,
    autoCommit: true
});

function getOrdersCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("order_db");
    return check db->getCollection("orders");
}

function parsePayload(string raw) returns json|error {
    json|error parsed = raw.fromJsonString();
    if parsed is error {
        return parsed;
    }
    return <json>parsed;
}

function updateOrderStatus(string orderId, string newStatus) returns error? {
    mongodb:Collection ordersColl = check getOrdersCollection();

    map<json> filter = {_id: orderId};
    OrderRecord? existing = check ordersColl->findOne(filter, targetType = OrderRecord);
    if existing is () {
        return error("Order not found: " + orderId);
    }

    string[] allowed = VALID_TRANSITIONS[existing.status] ?: [];
    boolean valid = false;
    foreach var s in allowed {
        if s == newStatus {
            valid = true;
        }
    }
    if !valid {
        log:printWarn("Invalid transition for " + orderId + ": " + existing.status + " -> " + newStatus);
        return;
    }

    map<json> deleteFilter = {_id: orderId};
    _ = check ordersColl->deleteOne(deleteFilter);

    map<json> newDoc = {
        "_id": existing._id,
        "customerId": existing.customerId,
        "restaurantId": existing.restaurantId,
        "items": existing.items,
        "totalAmount": existing.totalAmount,
        "status": newStatus,
        "createdAt": existing.createdAt
    };
    _ = check ordersColl->insertOne(newDoc);
    log:printInfo("Order " + orderId + " -> " + newStatus);
}

service /api/v1/orders on new http:Listener(PORT) {

    resource function get .() returns OrderRecord[]|error {
        mongodb:Collection ordersColl = check getOrdersCollection();
        map<json> emptyFilter = {};
        stream<OrderRecord, error?> orderStream = check ordersColl->find(emptyFilter, targetType = OrderRecord);
        OrderRecord[] result = [];
        check from OrderRecord rec in orderStream
            do {
                result.push(rec);
            };
        return result;
    }

    resource function get [string id]() returns OrderRecord|error? {
        mongodb:Collection ordersColl = check getOrdersCollection();
        map<json> filter = {_id: id};
        return check ordersColl->findOne(filter, targetType = OrderRecord);
    }

    resource function post .(OrderRequest payload) returns json|error {
        mongodb:Collection ordersColl = check getOrdersCollection();
        string newId = uuid:createType1AsString();

        map<json> doc = {
            "_id": newId,
            "customerId": payload.customerId,
            "restaurantId": payload.restaurantId,
            "items": payload.items,
            "totalAmount": payload.totalAmount,
            "status": "CREATED",
            "createdAt": "now"
        };

        _ = check ordersColl->insertOne(doc);

        map<json> eventPayload = {
            "orderId": newId,
            "customerId": payload.customerId,
            "restaurantId": payload.restaurantId,
            "totalAmount": payload.totalAmount
        };

        check orderProducer->send({
            topic: "orders.created",
            value: eventPayload.toString()
        });

        log:printInfo("Order created: " + newId);
        return doc;
    }

    resource function patch [string id]/status(StatusUpdate payload) returns json|error {
        mongodb:Collection ordersColl = check getOrdersCollection();

        map<json> filter = {_id: id};
        OrderRecord? existing = check ordersColl->findOne(filter, targetType = OrderRecord);
        if existing is () {
            return error("Order not found: " + id);
        }

        string[] allowed = VALID_TRANSITIONS[existing.status] ?: [];
        boolean valid = false;
        foreach var s in allowed {
            if s == payload.status {
                valid = true;
            }
        }
        if !valid {
            return error("Invalid transition: " + existing.status + " -> " + payload.status);
        }

        map<json> deleteFilter = {_id: id};
        _ = check ordersColl->deleteOne(deleteFilter);

        map<json> newDoc = {
            "_id": existing._id,
            "customerId": existing.customerId,
            "restaurantId": existing.restaurantId,
            "items": existing.items,
            "totalAmount": existing.totalAmount,
            "status": payload.status,
            "createdAt": existing.createdAt
        };
        _ = check ordersColl->insertOne(newDoc);
        log:printInfo("Order " + id + " manually moved to " + payload.status);
        return {status: payload.status};
    }
}

service on paymentListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);

            json|error parseResult = parsePayload(payloadStr);
            if parseResult is error {
                log:printError("Failed to parse payload");
                continue;
            }
            json payload = <json>parseResult;

            map<json> payloadMap = <map<json>>payload;
            string orderId = payloadMap["orderId"].toString();

            if rec.offset.partition.topic == "payments.completed" {
                check updateOrderStatus(orderId, "CONFIRMED");
            } else {
                check updateOrderStatus(orderId, "CANCELLED");
            }
        }
    }
}

service on deliveryListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);

            json|error parseResult = parsePayload(payloadStr);
            if parseResult is error {
                log:printError("Failed to parse payload");
                continue;
            }
            json payload = <json>parseResult;

            map<json> payloadMap = <map<json>>payload;
            string orderId = payloadMap["orderId"].toString();

            if rec.offset.partition.topic == "delivery.assigned" {
                check updateOrderStatus(orderId, "OUT_FOR_DELIVERY");
            } else if rec.offset.partition.topic == "delivery.completed" {
                check updateOrderStatus(orderId, "DELIVERED");
            }
        }
    }
}