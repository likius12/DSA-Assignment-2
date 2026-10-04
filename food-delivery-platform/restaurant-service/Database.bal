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

function getRestaurantsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("restaurant_db");
    return check db->getCollection("restaurants");
}

function getMenuItemsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("restaurant_db");
    return check db->getCollection("menuItems");
}