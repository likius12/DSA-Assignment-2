import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string KAFKA_BROKER = "kafka:9092";
configurable string MONGO_URI = "mongodb://mongodb:27017";
configurable int PORT = 8083;

// mongodb:Client mongoClient = check new ({
//     connection: {
//         serverAddress: {
//             host: "mongodb",
//             port: 8083
//         },
//         auth: <mongodb:ScramSha256AuthCredential>{
//             username: "admin",
//             password: "admin123",
//             database: "order_db"
//         }
//     }
// });

public enum OrderStatus {
    CREATED, CONFIRMED, PREPARING, READY, OUT_FOR_DELIVERY, DELIVERED, CANCELLED
}

type Order record {|
    string orderId;
    string customerId;
    string restaurantId;
    string items;
    OrderStatus status;
    string createdAt;
|};

final map<OrderStatus[]> Valid_transactions = {
    "CREATED": [],
    "CONFIRMED": [],
    "PREPARING": [],
    "READY": [],
    "OUT_FOR_DELIVERY": [],
    "DELIVERED": [],
    "CANCELLED": []
};

// STILL DONT LIKE THIS 

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

function getOrdersCollection() returns mongodb:Collection|error {
    mongodb:Database orderDb = check mongoClient->getDatabase("order_db");
    return check orderDb->getCollection("orders");
}

// initializing kafka listeners and producers

listener kafka:Listener kafkaListenerPayments = new (kafka:DEFAULT_URL, {
    groupId: "payments-service",
    topics: ["payments.completed", "payments.failed"],
    pollingInterval: 1
});

listener kafka:Listener kafkaListenerDelivery = new (kafka:DEFAULT_URL, {
    groupId: "delivery-service",
    topics: ["delivery.assigned", "delivery.completed"],
    pollingInterval: 1
});

final kafka:Producer kafkaOrders = check new (kafka:DEFAULT_URL, {
    clientId: "orders",
    retryCount: 3
});

service /api/v1/orders on new http:Listener(PORT) {

    resource function get .() returns Order[]|error {
        mongodb:Collection orders = check getOrdersCollection();
        stream<Order, error?> orderStream = check orders->find({}, targetType = Order);
        Order[] result = [];
        check from Order orderItem in orderStream
            do {
                result.push(orderItem);
            };
        return result;
    }

    resource function post .(Order payload) returns json|error? {
        mongodb:Collection orders = check getOrdersCollection();
        string orderId = uuid:createType1AsString();

        json orderDoc = {
            _id: orderId,
            customerId: payload.customerId,
            restaurantId: payload.restaurantId,
            items: payload.items,
            status: "CREATED",
            createdAt: "now"
        };

        check orders->insertOne(<Order>orderDoc);

        check kafkaOrders->send({
            topic: "orders.created",
            value:  payload.toString()
        });

        log:printInfo("Order created: " + orderId);
        return payload;
    }

    resource function get [string id]() returns Order?|error {
        mongodb:Collection orders = check getOrdersCollection();
        Order|mongodb:DatabaseError|mongodb:ApplicationError|error? result = orders->findOne({_id: id}, targetType = Order);
        if result is Order|() {
            return result;
        }
        log:printError("Failed to retrieve order: " + result.message());
        return;
    }

    resource function patch [string id]/status(Order payload) returns json|error {
        mongodb:Collection orders = check getOrdersCollection();
        Order|mongodb:DatabaseError|mongodb:ApplicationError| error? existing = orders->findOne({_id: id}, targetType = Order);
        if existing is error {
            return existing;
        }

        if existing is Order {
            string current = existing.status.toString();
            string next = payload.status.toString();

            OrderStatus[] allowed = Valid_transactions[current] ?: [];
            boolean valid = false;
            foreach var item in allowed {
                if item.toString() == next {valid = true;}
            }
            if !valid {
                return error("Invalid transition: " + current + " -> " + next);
            }

            mongodb:UpdateResult _ = check orders->updateOne({_id: id}, {"$set":{status: next}});
            log:printInfo("Order " + id + " moved to " + next);
            return {status: next};
        }

        return error("Order not found");
    }

}

service kafka:Service on kafkaListenerDelivery {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadString = check string:fromBytes(rec.value);
            json|error parseResult = payloadString.fromJsonString();
            if parseResult is error {
                return parseResult;
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

service kafka:Service on kafkaListenerPayments {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadString = check string:fromBytes(rec.value);
            json|error parseResult = payloadString.fromJsonString();
            if parseResult is error {
                return parseResult;
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

function updateOrderStatus(string orderId, string newStatus) returns error? {
    mongodb:Collection orders = check getOrdersCollection();
    Order? existing = check orders->findOne({_id: orderId}, targetType = Order);
    if existing is () { return error("Order not found: " + orderId); }
    _ = check orders->updateOne({_id: orderId}, {"$set": {status: newStatus}});
    log:printInfo("Order " + orderId + " auto-updated to " + newStatus);
}
