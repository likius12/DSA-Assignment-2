import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

const string PAYMENT_DB = "payment_db";
const string PAYMENTS_COLLECTION = "payments";
const string REFUNDS_COLLECTION = "refunds";

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

type RefundRequest record {|
    string paymentId;
    string reason;
|};

type Payment record {|
    string _id;
    string orderId;
    string customerId;
    decimal amount;
    string currency;
    string paymentMethod;
    string status;
    string? transactionId;
    string createdAt;
    string updatedAt?;
|};

type Refund record {|
    string _id;
    string paymentId;
    string orderId;
    decimal amount;
    string reason;
    string status;
    string createdAt;
|};

type PaymentResult record {|
    boolean success;
    string transactionId;
    string message;
|};

mongodb:Client mongoClient = check new ({
    host: "mongodb",
    port: 27017,
    username: "admin",
    password: "admin123",
    database: "admin"
});

service /payment on new http:Listener(9090) {
    resource function post .(PaymentRequest request) returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection payments = check db->getCollection(PAYMENTS_COLLECTION);

        Payment? existing = check payments->findOne({orderId: request.orderId});

        if existing is Payment {
            if existing.status == "COMPLETED" {
                log:printInfo("Payment already exists for order: " + request.orderId);
                return existing;
            }
        }

        PaymentResult result = processPayment(request);
        string paymentId = generatePaymentId();
        Payment payment = {
            _id: paymentId,
            orderId: request.orderId,
            customerId: request.customerId,
            amount: request.amount,
            currency: request.currency,
            paymentMethod: request.paymentMethod,
            status: result.success ? "COMPLETED" : "FAILED",
            transactionId: result.success ? result.transactionId : (),
            createdAt: "now"
        };

        _ = check payments->insertOne(payment);

        if result.success {
            check publishPaymentCompleted(payment);
            log:printInfo("Payment completed: " + paymentId);
        } else {
            check publishPaymentFailed(payment, result.message);
            log:printWarn("Payment failed: " + paymentId);
        }

        return payment;
    }

    resource function get [string paymentId]() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection payments = check db->getCollection(PAYMENTS_COLLECTION);

        Payment? payment = check payments->findOne({_id: paymentId});
        if payment is () {
            return error("Payment not found: " + paymentId);
        }
        return payment;
    }

    resource function get 'order/[string orderId]() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection payments = check db->getCollection(PAYMENTS_COLLECTION);

        Payment? payment = check payments->findOne({orderId: orderId});
        if payment is () {
            return error("Payment not found for order: " + orderId);
        }
        return payment;
    }

    resource function get .() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection payments = check db->getCollection(PAYMENTS_COLLECTION);

        stream<Payment, error?> paymentStream = check payments->find({});
        Payment[] result = [];
        check from Payment p in paymentStream
            do {
                result.push(p);
            };
        return result;
    }

    resource function post refunds(RefundRequest request) returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection payments = check db->getCollection(PAYMENTS_COLLECTION);
        mongodb:Collection refunds = check db->getCollection(REFUNDS_COLLECTION);

        Payment? payment = check payments->findOne({_id: request.paymentId});
        if payment is () {
            return error("Payment not found: " + request.paymentId);
        }

        if payment.status == "REFUNDED" {
            return error("Payment already refunded");
        }

        string refundId = generateRefundId();
        Refund refund = {
            _id: refundId,
            paymentId: payment._id,
            orderId: payment.orderId,
            amount: payment.amount,
            reason: request.reason,
            status: "COMPLETED",
            createdAt: "now"
        };
        _ = check refunds->insertOne(refund);

        map<json> update = {status: "REFUNDED", updatedAt: "now"};
        _ = check payments->updateOne({_id: payment._id}, {"$set": update});

        check publishRefundCompleted(refund);

        log:printInfo("Refund processed: " + refundId);
        return refund;
    }

    resource function get refunds() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase(PAYMENT_DB);
        mongodb:Collection refunds = check db->getCollection(REFUNDS_COLLECTION);

        stream<Refund, error?> refundStream = check refunds->find({});
        Refund[] result = [];
        check from Refund r in refundStream
            do {
                result.push(r);
            };
        return result;
    }

    resource function get health() returns json {
        return {
            status: "UP",
            "service": "payment-service",
            timestamp: "now"
        };
    }
}

function processPayment(PaymentRequest request) returns PaymentResult {
    if request.amount <= 0d {
        return {success: false, transactionId: "", message: "Invalid payment amount"};
    }

    string transactionId = "txn_" + uuid:createType1AsString();
    return {success: true, transactionId: transactionId, message: "Payment processed successfully"};
}

function generatePaymentId() returns string {
    return "pay_" + uuid:createType1AsString();
}

function generateRefundId() returns string {
    return "ref_" + uuid:createType1AsString();
}

function publishPaymentCompleted(Payment payment) returns error? {
    log:printInfo("Publishing payment completed event for paymentId: " + payment._id);
    return ();
}

function publishPaymentFailed(Payment payment, string message) returns error? {
    log:printInfo("Publishing payment failed event for paymentId: " + payment._id + " reason: " + message);
    return ();
}

function publishRefundCompleted(Refund refund) returns error? {
    log:printInfo("Publishing refund completed event for refundId: " + refund._id);
    return ();
}


