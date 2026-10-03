import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/mongodb;

configurable int PORT = 8081;

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

type CustomerRequest record {|
    string name;
    string email;
    string address;
|};

type Customer record {|
    string _id;
    string name;
    string email;
    string address;
    string createdAt;
|};

service /api/v1/customers on new http:Listener(PORT) {
    mongodb:Database customerDb;
    mongodb:Collection customers;

    function init() returns error? {
        self.customerDb = check mongoClient->getDatabase("customer_db");
        self.customers = check self.customerDb->getCollection("customers");
    }

    resource function get .() returns json|error {
        stream<Customer, error?> customerStream = check self.customers->find({});
        Customer[] result = [];
        check from Customer customer in customerStream
            do {
                result.push(customer);
            };
        return result;
    }

    resource function get [string id]() returns json|error {
        Customer? customer = check self.customers->findOne({_id: id});
        return customer;
    }

    resource function post .(CustomerRequest payload) returns json|error {
        string customerId = uuid:createType1AsString();

        Customer customer = {
            _id: customerId,
            name: payload.name,
            email: payload.email,
            address: payload.address,
            createdAt: "now"
        };

        _ = check self.customers->insertOne(customer);
        log:printInfo("Customer created: " + customerId);
        return customer;
    }

    resource function delete [string id]() returns json|error {
        map<json> filter = {_id: id};
        _ = check self.customers->deleteOne(filter);
        return {status: "deleted", id: id};
    }
}