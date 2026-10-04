import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

const string PAYMENT_DB = "payment_db";
const string PAYMENTS_COLLECTION = "payments";

type PaymentRequest record {|
    string orderId;
    string customerId;
    decimal amount;
    string currency;
    string paymentMethod;
    string cardNumber?;
    string cardHolderName?;
    string expiryDate?;
    string cvv?;
|};

final mongodb:Client mongoClient = check new ({
    connection: {
        host: "localhost",
        port: 27017,
        auth: {
            username: "admin",
            password: "admin123"
        }
    }
});

service /payment on new http:Listener(9090) {
    //payment
    resource function post .(PaymentRequest request) returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection payments = check db->getCollection(PAYMENTS_COLLECTION);
