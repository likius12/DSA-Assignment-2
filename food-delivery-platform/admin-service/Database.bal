import ballerinax/mongodb;

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

function getOrdersCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("order_db");
    return check db->getCollection("orders");
}

function getDeliveriesCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("delivery_db");
    return check db->getCollection("deliveries");
}

function getDriversCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("delivery_db");
    return check db->getCollection("drivers");
}

function getRestaurantsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("restaurant_db");
    return check db->getCollection("restaurants");
}

function getMenuItemsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("restaurant_db");
    return check db->getCollection("menuItems");
}