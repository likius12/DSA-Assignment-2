
ballerina
import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

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

listener kafka:Consumer notificationConsumer = check new ({
    bootstrapServers: KAFKA_BROKER,
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
    clientId: "notification-consumer"
});

service /api/v1/notifications on new http:Listener(PORT) {

    resource function get .() returns json|error {
        mongodb:Database notificationDb = check mongoClient->getDatabase("notification_db");
        mongodb:Collection notifications = check notificationDb->getCollection("notifications");
        stream<Notification, error?> notifStream = check notifications->find({});
        Notification[] result = [];
        check from Notification n in notifStream
            do {
                result.push(n);
            };
        return result;
    }
}

service on notificationConsumer {
    remote function onMessage(kafka:ConsumerRecord[] records) returns error? {
        mongodb:Database notificationDb = check mongoClient->getDatabase("notification_db");
        mongodb:Collection notifications = check notificationDb->getCollection("notifications");

        foreach var rec in records {
            string payloadStr = rec.value.toString();
            string notifId = uuid:createType1AsString();

            Notification notification = {
                _id: notifId,
                topic: rec.topic,
                payload: payloadStr,
                channel: "SMS",
                sentAt: "now"
            };

            _ = check notifications->insertOne(notification);
            log:printInfo("NOTIFY [" + rec.topic + "] " + payloadStr);
        }
    }
}