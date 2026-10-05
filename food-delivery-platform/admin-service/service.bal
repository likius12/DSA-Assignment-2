import ballerina/http;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable int PORT = 8087;

service /api/v1/admin on new http:Listener(PORT) {

    resource function get health() returns json {
        return {
            "status": "UP",
            "service": "admin-service",
            "timestamp": "now"
        };
    }

    resource function get reports/orders() returns json|error {
        mongodb:Collection orders = check getOrdersCollection();
        int total = check orders->countDocuments({});

        int created = check orders->countDocuments({status: "CREATED"});
        int confirmed = check orders->countDocuments({status: "CONFIRMED"});
        int preparing = check orders->countDocuments({status: "PREPARING"});
        int ready = check orders->countDocuments({status: "READY"});
        int outForDelivery = check orders->countDocuments({status: "OUT_FOR_DELIVERY"});
        int delivered = check orders->countDocuments({status: "DELIVERED"});
        int cancelled = check orders->countDocuments({status: "CANCELLED"});

        return {
            "totalOrders": total,
            "byStatus": {
                "CREATED": created,
                "CONFIRMED": confirmed,
                "PREPARING": preparing,
                "READY": ready,
                "OUT_FOR_DELIVERY": outForDelivery,
                "DELIVERED": delivered,
                "CANCELLED": cancelled
            }
        };
    }

    resource function get reports/deliveries() returns json|error {
        mongodb:Collection deliveries = check getDeliveriesCollection();
        int total = check deliveries->countDocuments({});
        int assigned = check deliveries->countDocuments({status: "ASSIGNED"});
        int completed = check deliveries->countDocuments({status: "COMPLETED"});

        return {
            "totalDeliveries": total,
            "assigned": assigned,
            "completed": completed
        };
    }

    resource function get reports/drivers() returns json|error {
        mongodb:Collection drivers = check getDriversCollection();
        int total = check drivers->countDocuments({});
        int available = check drivers->countDocuments({available: true});
        int busy = check drivers->countDocuments({available: false});

        return {
            "totalDrivers": total,
            "available": available,
            "busy": busy
        };
    }

    resource function get reports/restaurants() returns json|error {
        mongodb:Collection restaurants = check getRestaurantsCollection();
        mongodb:Collection menuItems = check getMenuItemsCollection();

        int totalRestaurants = check restaurants->countDocuments({});
        int totalMenuItems = check menuItems->countDocuments({});

        return {
            "totalRestaurants": totalRestaurants,
            "totalMenuItems": totalMenuItems
        };
    }
}