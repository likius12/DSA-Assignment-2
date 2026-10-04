import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable int PORT = 8082;

service /api/v1/restaurants on new http:Listener(PORT) {

    resource function get .() returns Restaurant[]|error {
        mongodb:Collection restaurants = check getRestaurantsCollection();
        map<json> emptyFilter = {};
        stream<Restaurant, error?> restaurantStream = check restaurants->find(emptyFilter, targetType = Restaurant);
        Restaurant[] result = [];
        check from Restaurant r in restaurantStream
            do {
                result.push(r);
            };
        return result;
    }

    resource function get [string id]() returns Restaurant|error? {
        mongodb:Collection restaurants = check getRestaurantsCollection();
        map<json> filter = {_id: id};
        return check restaurants->findOne(filter, targetType = Restaurant);
    }

    resource function post .(RestaurantInput payload) returns json|error {
        mongodb:Collection restaurants = check getRestaurantsCollection();
        string newId = uuid:createType1AsString();

        Restaurant newRestaurant = {
            _id: newId,
            name: payload.name,
            openingHours: payload.openingHours,
            isOpen: true,
            createdAt: "now"
        };

        _ = check restaurants->insertOne(newRestaurant);
        log:printInfo("Restaurant created: " + newId);
        return newRestaurant;
    }

    resource function post [string id]/menu(MenuItemInput payload) returns json|error {
        mongodb:Collection restaurants = check getRestaurantsCollection();
        mongodb:Collection menuItems = check getMenuItemsCollection();

        map<json> restaurantFilter = {_id: id};
        Restaurant? restaurant = check restaurants->findOne(restaurantFilter, targetType = Restaurant);
        if restaurant is () {
            return error("Restaurant not found: " + id);
        }

        string itemId = uuid:createType1AsString();
        MenuItem newItem = {
            _id: itemId,
            restaurantId: id,
            name: payload.name,
            price: payload.price,
            stockQty: payload.stockQty,
            available: true,
            createdAt: "now"
        };

        _ = check menuItems->insertOne(newItem);

        check publishMenuUpdated(id);

        log:printInfo("Menu item added: " + itemId + " to restaurant " + id);
        return newItem;
    }

    resource function get [string id]/menu() returns MenuItem[]|error {
        mongodb:Collection menuItems = check getMenuItemsCollection();
        map<json> filter = {restaurantId: id};
        stream<MenuItem, error?> itemStream = check menuItems->find(filter, targetType = MenuItem);
        MenuItem[] result = [];
        check from MenuItem m in itemStream
            do {
                result.push(m);
            };
        return result;
    }

    resource function patch menu/[string itemId](MenuItemUpdate payload) returns json|error {
        mongodb:Collection menuItems = check getMenuItemsCollection();

        map<json> filter = {_id: itemId};
        MenuItem? existing = check menuItems->findOne(filter, targetType = MenuItem);
        if existing is () {
            return error("Menu item not found: " + itemId);
        }

        string updatedName = existing.name;
        decimal updatedPrice = existing.price;
        int updatedStock = existing.stockQty;
        boolean updatedAvailable = existing.available;

        string? newName = payload.name;
        if newName is string {
            updatedName = newName;
        }

        decimal? newPrice = payload.price;
        if newPrice is decimal {
            updatedPrice = newPrice;
        }

        int? newStock = payload.stockQty;
        if newStock is int {
            updatedStock = newStock;
        }

        boolean? newAvailable = payload.available;
        if newAvailable is boolean {
            updatedAvailable = newAvailable;
        }

        map<json> deleteFilter = {_id: itemId};
        _ = check menuItems->deleteOne(deleteFilter);

        MenuItem updatedItem = {
            _id: existing._id,
            restaurantId: existing.restaurantId,
            name: updatedName,
            price: updatedPrice,
            stockQty: updatedStock,
            available: updatedAvailable,
            createdAt: existing.createdAt
        };
        _ = check menuItems->insertOne(updatedItem);

        check publishMenuUpdated(existing.restaurantId);

        log:printInfo("Menu item updated: " + itemId);
        return updatedItem;
    }

    resource function get health() returns json {
        return {
            "status": "UP",
            "service": "restaurant-service",
            "timestamp": "now"
        };
    }
}