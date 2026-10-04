import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string KAFKA_BROKER = "kafka:9092";
configurable int PORT = 8084;

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

type Payment record {|
    string _id;
    string orderId;
    string customerId;
    decimal amount;
    string status;
    string transactionId;
    string createdAt;
|};

final kafka:Producer paymentProducer = check new (KAFKA_BROKER, {
    clientId: "payment-service"
});

listener kafka:Listener orderListener = new (KAFKA_BROKER, {
    groupId: "payment-service-group",
    topics: ["orders.created"],
    pollingInterval: 1,
    autoCommit: true
});

function getPaymentsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("payment_db");
    return check db->getCollection("payments");
}

function parsePayload(string raw) returns json|error {
    json|error parsed = raw.fromJsonString();
    if parsed is error {
        return parsed;
    }
    return <json>parsed;
}

service /api/v1/payments on new http:Listener(PORT) {

    resource function get .() returns Payment[]|error {
        mongodb:Collection payments = check getPaymentsCollection();
        map<json> emptyFilter = {};
        stream<Payment, error?> paymentStream = check payments->find(emptyFilter, targetType = Payment);
        Payment[] result = [];
        check from Payment p in paymentStream
            do {
                result.push(p);
            };
        return result;
    }

    resource function get [string id]() returns Payment|error? {
        mongodb:Collection payments = check getPaymentsCollection();
        map<json> filter = {_id: id};
        return check payments->findOne(filter, targetType = Payment);
    }

    resource function get 'order/[string orderId]() returns Payment|error? {
        mongodb:Collection payments = check getPaymentsCollection();
        map<json> filter = {orderId: orderId};
        return check payments->findOne(filter, targetType = Payment);
    }

    resource function get health() returns json {
        return {
            "status": "UP",
            "service": "payment-service",
            "timestamp": "now"
        };
    }
    }

service on orderListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        mongodb:Collection payments = check getPaymentsCollection();

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
            string customerId = payloadMap["customerId"].toString();
            decimal amount = check decimal:fromString(payloadMap["totalAmount"].toString());

            string paymentId = "pay_" + uuid:createType1AsString();
            string transactionId = "txn_" + uuid:createType1AsString();

            Payment payment = {
                _id: paymentId,
                orderId: orderId,
                customerId: customerId,
                amount: amount,
                status: "COMPLETED",
                transactionId: transactionId,
                createdAt: "now"
            };

            _ = check payments->insertOne(payment);

            map<json> eventPayload = {
                "orderId": orderId,
                "paymentId": paymentId,
                "transactionId": transactionId,
                "amount": amount,
                "status": "COMPLETED"
            };

            check paymentProducer->send({
                topic: "payments.completed",
                value: eventPayload.toString()
            });

            log:printInfo("Payment " + paymentId + " completed for order " + orderId);
        }
    }
}