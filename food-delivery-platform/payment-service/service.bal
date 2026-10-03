import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configureable string KAFKA_BROKER = "kafka:9092";
configureable int PORT = 8084;



