import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string KAFKA_BROKER = "kafka:9092";
configurable int PORT = 8085;

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

type DriverRequest record {|
    string name;
    string phone;
|};

type Driver record {|
    string _id;
    string name;
    string phone;
    boolean available;
    string createdAt;
|};

type Delivery record {|
    string _id;
    string orderId;
    string driverId;
    string status;
    string assignedAt;
    string completedAt?;
|};

final kafka:Producer deliveryProducer = check new (KAFKA_BROKER, {
    clientId: "delivery-service"
});

listener kafka:Listener paymentListener = new (KAFKA_BROKER, {
    groupId: "delivery-service-group",
    topics: ["payments.completed"],
    pollingInterval: 1,
    autoCommit: true
});

function parsePayload(string raw) returns json|error {
    json|error parsed = raw.fromJsonString();
    if parsed is error {
        return parsed;
    }
    return <json>parsed;
}

service /api/v1/deliveries on new http:Listener(PORT) {

    resource function get .() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("delivery_db");
        mongodb:Collection deliveries = check db->getCollection("deliveries");
        stream<Delivery, error?> deliveryStream = check deliveries->find({});
        Delivery[] result = [];
        check from Delivery d in deliveryStream
            do {
                result.push(d);
            };
        return result;
    }

    resource function post drivers(DriverRequest payload) returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("delivery_db");
        mongodb:Collection drivers = check db->getCollection("drivers");

        string driverId = uuid:createType1AsString();
        Driver driver = {
            _id: driverId,
            name: payload.name,
            phone: payload.phone,
            available: true,
            createdAt: "now"
        };
        _ = check drivers->insertOne(driver);
        log:printInfo("Driver registered: " + driverId);
        return driver;
    }

    resource function get drivers() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("delivery_db");
        mongodb:Collection drivers = check db->getCollection("drivers");
        stream<Driver, error?> driverStream = check drivers->find({});
        Driver[] result = [];
        check from Driver d in driverStream
            do {
                result.push(d);
            };
        return result;
    }

    resource function post [string id]/complete() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("delivery_db");
        mongodb:Collection deliveries = check db->getCollection("deliveries");
        mongodb:Collection drivers = check db->getCollection("drivers");

        Delivery? delivery = check deliveries->findOne({_id: id});
        if delivery is () {
            return error("Delivery not found: " + id);
        }

        map<json> update = {status: "COMPLETED", completedAt: "now"};
        _ = check deliveries->updateOne({_id: id}, {"$set": update});

        _ = check drivers->updateOne({_id: delivery.driverId}, {"$set": {available: true}});

        check deliveryProducer->send({
            topic: "delivery.completed",
            value: {orderId: delivery.orderId, deliveryId: id, driverId: delivery.driverId}.toString()
        });

        log:printInfo("Delivery completed: " + id);
        return {status: "COMPLETED", deliveryId: id};
    }
}

service on paymentListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        mongodb:Database db = check mongoClient->getDatabase("delivery_db");
        mongodb:Collection drivers = check db->getCollection("drivers");
        mongodb:Collection deliveries = check db->getCollection("deliveries");

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

            Driver? availableDriver = check drivers->findOne({available: true});
            if availableDriver is () {
                log:printWarn("No available driver for order " + orderId);
                continue;
            }

            string driverId = availableDriver._id;
            _ = check drivers->updateOne({_id: driverId}, {"$set": {available: false}});

            string deliveryId = uuid:createType1AsString();
            Delivery delivery = {
                _id: deliveryId,
                orderId: orderId,
                driverId: driverId,
                status: "ASSIGNED",
                assignedAt: "now"
            };
            _ = check deliveries->insertOne(delivery);

            check deliveryProducer->send({
                topic: "delivery.assigned",
                value: {orderId: orderId, deliveryId: deliveryId, driverId: driverId}.toString()
            });

            log:printInfo("Delivery " + deliveryId + " assigned to driver " + driverId);
        }
    }
}