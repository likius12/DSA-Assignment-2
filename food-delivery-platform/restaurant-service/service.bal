import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;

configurable int servicePort = 9091;

// ---------- Service ----------
service /api on new http:Listener(servicePort) {

    // Create a restaurant
    resource function post restaurants(RestaurantInput input)
            returns http:Created|error {
        Restaurant r = {
            id: uuid:createType4AsString(),
            name: input.name,
            openingHours: input.openingHours,
            isOpen: true
        };
        check restaurants->insertOne(r);
        return {body: r};
    }

    // Get one restaurant
    resource function get restaurants/[string id]()
            returns Restaurant|http:NotFound|error {
        Restaurant? r = check restaurants->findOne({id: id}, {}, {_id: 0}, Restaurant);
        if r is () {
            return http:NOT_FOUND;
        }
        return r;
    }

    // Update opening hours
    resource function put restaurants/[string id]/hours(OpeningHours hours)
            returns http:Ok|http:NotFound|error {
        mongodb:UpdateResult res = check restaurants->updateOne({id: id}, {set: {openingHours: hours}});
        if res.matchedCount == 0 {
            return http:NOT_FOUND;
        }
        return http:OK;
    }

    // Add a menu item
    resource function post restaurants/[string id]/menu(MenuItemInput input)
            returns http:Created|http:NotFound|error {
        Restaurant? r = check restaurants->findOne({id: id}, {}, {_id: 0}, Restaurant);
        if r is () {
            return http:NOT_FOUND;
        }
        MenuItem item = {
            id: uuid:createType4AsString(),
            restaurantId: id,
            name: input.name,
            price: input.price,
            stockQty: input.stockQty,
            available: input.available
        };
        check menuItems->insertOne(item);
        http:Created response = {body: item};
        return response;
    }

    // List a restaurant's menu
    resource function get restaurants/[string id]/menu()
            returns MenuItem[]|error {
        stream<MenuItem, error?> items =
            check menuItems->find({restaurantId: id}, {}, {_id: 0}, MenuItem);
        return check from MenuItem m in items
            select m;
    }

    // Update name / price / availability
    resource function put menu/[string itemId](MenuItemUpdate update)
            returns http:Ok|http:NotFound|error {
        map<json> changes = {};
        if update.name is string {
            changes["name"] = update.name;
        }
        if update.price is decimal {
            changes["price"] = update.price;
        }
        if update.available is boolean {
            changes["available"] = update.available;
        }
        if changes.length() == 0 {
            return http:OK;
        }
        mongodb:UpdateResult res = check menuItems->updateOne({id: itemId}, {set: changes});
        if res.matchedCount == 0 {
            return http:NOT_FOUND;
        }
        return http:OK;
    }

    // Set stock quantity
    resource function patch menu/[string itemId]/stock(StockUpdate update)
            returns http:Ok|http:NotFound|error {
        mongodb:UpdateResult res = check menuItems->updateOne({id: itemId}, {set: {stockQty: update.stockQty}});
        if res.matchedCount == 0 {
            return http:NOT_FOUND;
        }
        return http:OK;
    }

    // Kitchen marks an order ready -> publishes orders.ready
    resource function put orders/[string orderId]/ready() returns http:Ok|error {
        check publishReady(orderId);
        return http:OK;
    }
}
