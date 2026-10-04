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
        
Payment? existing = check payments->findOne({orderId: request.orderId});
;

if existing is Payment {
            if existing.status == "COMPLETED" {
                log:printInfo("Payment already exists for order: " + request.orderId);
                return existing;
            }
        }

         PaymentResult result = processPayment(request);

        // Create payment record
        string paymentId = generatePaymentId();
        Payment payment = {
            _id: paymentId,
            orderId: request.orderId,
            customerId: request.customerId,
            amount: request.amount,
            currency: request.currency,
            paymentMethod: request.paymentMethod,
            status: result.success ? "COMPLETED" : "FAILED",
            transactionId: result.transactionId,
            createdAt: "now"
        };

        // Save to database
        _ = check payments->insertOne(payment);

        // Publish Kafka event
        if result.success {
            check publishPaymentCompleted(payment);
            log:printInfo("Payment completed: " + paymentId);
        } else {
            check publishPaymentFailed(payment, result.message);
            log:printWarn("Payment failed: " + paymentId);
        }

         2. GET PAYMENT BY ID
   
    resource function get [string paymentId]() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection payments = check db->getCollection(PAYMENTS_COLLECTION);

        Payment? payment = check payments->findOne({_id: paymentId});
        if payment is () {
            return error("Payment not found: " + paymentId);
        }
        return payment;

