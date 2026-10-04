import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable string KAFKA_BROKER = "kafka:9092";
configurable int PORT = 8086;

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

type Notification record {|
    string _id;
    string topic;
    string payload;
    string channel;
    string sentAt;
|};

listener kafka:Listener notificationListener = new (KAFKA_BROKER, {
    groupId: "notification-service-group",
    topics: [
        "orders.created",
        "orders.cancelled",
        "payments.completed",
        "payments.failed",
        "delivery.assigned",
        "delivery.completed",
        "restaurant.menu.updated"
    ],
    pollingInterval: 1,
    autoCommit: true
});

service /api/v1/notifications on new http:Listener(PORT) {

    resource function get .() returns Notification[]|error {
        mongodb:Database notificationDb = check mongoClient->getDatabase("notification_db");
        mongodb:Collection notifications = check notificationDb->getCollection("notifications");
        map<json> emptyFilter = {};
        stream<Notification, error?> notifStream = check notifications->find(emptyFilter, targetType = Notification);
        Notification[] result = [];
        check from Notification n in notifStream
            do {
                result.push(n);
            };
        return result;
    }
}

service on notificationListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        mongodb:Database notificationDb = check mongoClient->getDatabase("notification_db");
        mongodb:Collection notifications = check notificationDb->getCollection("notifications");

        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);
            string notifId = uuid:createType1AsString();
            string topicName = rec.offset.partition.topic;

            Notification notification = {
                _id: notifId,
                topic: topicName,
                payload: payloadStr,
                channel: "SMS",
                sentAt: "now"
            };

            _ = check notifications->insertOne(notification);
            log:printInfo("NOTIFY [" + topicName + "] " + payloadStr);
        }
    }
}