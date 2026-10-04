import ballerinax/mongodb;

configurable string mongoUri = "mongodb://localhost:27017";
configurable string dbName = "restaurant_db";


// ...existing code...
final mongodb:Client mongoClient = check new ({connection: mongoUri});

function getRestaurantsCollection() returns mongodb:Collection|error {
    final mongodb:Database db = check mongoClient->getDatabase(dbName);
    return check db->getCollection("restaurants");
}

function getMenuItemsCollection() returns mongodb:Collection|error {
    final mongodb:Database db = check mongoClient->getDatabase(dbName);
    return check db->getCollection("menuItems");
}

final mongodb:Collection restaurants = check getRestaurantsCollection();
final mongodb:Collection menuItems = check getMenuItemsCollection();
