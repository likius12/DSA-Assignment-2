import ballerina/log;

function init() returns error? {
    log:printInfo("Starting Admin Service...");
    check initDatabase();
    check startKafkaConsumer();
    log:printInfo("Admin Service initialized");
}



