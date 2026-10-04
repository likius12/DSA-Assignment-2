import ballerina/log;
import ballerinax/kafka;

configurable string KAFKA_BROKER = "kafka:9092";

final kafka:Producer restaurantProducer = check new (KAFKA_BROKER, {
    clientId: "restaurant-service"
});

function publishMenuUpdated(string restaurantId) returns error? {
    map<json> eventPayload = {
        "restaurantId": restaurantId,
        "updatedAt": "now"
    };

    check restaurantProducer->send({
        topic: "restaurant.menu.updated",
        value: eventPayload.toString()
    });

    log:printInfo("Published restaurant.menu.updated for restaurant " + restaurantId);
}