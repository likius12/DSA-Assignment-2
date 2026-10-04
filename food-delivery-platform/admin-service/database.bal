import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;

// MongoDB client — connection is a plain URI string in 5.x
final mongodb:Client adminDb = check new ({
    connection: mongoUri
});

// Collection names
final string RESTAURANT_STATS = "restaurant_stats";
final string DELIVERY_PERFORMANCE = "delivery_performance";
final string ORDER_LEDGER = "order_ledger";
final string DAILY_REVENUE = "daily_revenue";
final string REPORTS = "reports";

// Helper — get a collection inside each function
isolated function getCollection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check adminDb->getDatabase(mongoDatabase);
    return check db->getCollection(name);
}

// ============ Init indexes ============
public function initDatabase() returns error? {
    mongodb:Collection restCol = check getCollection(RESTAURANT_STATS);
    _ = check restCol->createIndex({"restaurantId": 1}, {unique: true});

    mongodb:Collection delCol = check getCollection(DELIVERY_PERFORMANCE);
    _ = check delCol->createIndex({"driverId": 1}, {unique: true});

    mongodb:Collection ledgerCol = check getCollection(ORDER_LEDGER);
    _ = check ledgerCol->createIndex({"orderId": 1}, {unique: true});
    _ = check ledgerCol->createIndex({"createdAt": 1});

    mongodb:Collection revCol = check getCollection(DAILY_REVENUE);
    _ = check revCol->createIndex({"date": 1}, {unique: true});

    mongodb:Collection repCol = check getCollection(REPORTS);
    _ = check repCol->createIndex({"reportId": 1}, {unique: true});
    _ = check repCol->createIndex({"generatedAt": -1});

    log:printInfo("Admin DB indexes initialized");
}

// ============ Order Ledger ============
public function recordOrderCreated(OrderCreatedEvent event) returns error? {
    mongodb:Collection col = check getCollection(ORDER_LEDGER);

    map<json> doc = {
        "orderId": event.orderId,
        "customerId": event.customerId,
        "restaurantId": event.restaurantId,
        "totalAmount": event.totalAmount,
        "status": "CREATED",
        "createdAt": event.createdAt,
        "updatedAt": event.createdAt
    };

    _ = check col->insertOne(doc);
    log:printInfo("Order recorded in ledger", orderId = event.orderId);
}

public function updateOrderStatus(string orderId, string newStatus,
                                   string? driverId = ()) returns error? {
    mongodb:Collection col = check getCollection(ORDER_LEDGER);

    if driverId is string {
        _ = check col->updateOne({"orderId": orderId}, {"$set": {
            "status": newStatus,
            "driverId": driverId,
            "updatedAt": time:utcToString(time:utcNow())
        }});
    } else {
        _ = check col->updateOne({"orderId": orderId}, {"$set": {
            "status": newStatus,
            "updatedAt": time:utcToString(time:utcNow())
        }});
    }
}

// ============ Restaurant Stats ============
public function upsertRestaurantStats(string restaurantId, int orderDelta,
                                       decimal revenueDelta, boolean completed,
                                       boolean cancelled) returns error? {
    mongodb:Collection col = check getCollection(RESTAURANT_STATS);

    map<json> inc = {
        "totalOrders": orderDelta,
        "totalRevenue": revenueDelta
    };
    if completed {
        inc["completedOrders"] = 1;
    }
    if cancelled {
        inc["cancelledOrders"] = 1;
    }

    _ = check col->updateOne({"restaurantId": restaurantId}, {
        "$inc": inc,
        "$set": {"lastUpdated": time:utcToString(time:utcNow())},
        "$setOnInsert": {"restaurantId": restaurantId}
    }, {upsert: true});
}

public function getRestaurantStats(string restaurantId) returns RestaurantStats|error? {
    mongodb:Collection col = check getCollection(RESTAURANT_STATS);
    RestaurantStats|mongodb:Error? result = col->findOne({"restaurantId": restaurantId});
    if result is mongodb:Error {
        return result;
    }
    return result;
}

public function getAllRestaurantStats(int count = 100) returns RestaurantStats[]|error {
    mongodb:Collection col = check getCollection(RESTAURANT_STATS);

    stream<RestaurantStats, mongodb:Error?> result = check col->find(
        {},
        {sort: {"totalRevenue": -1}, 'limit: count}
    );
    RestaurantStats[] stats = [];
    check from RestaurantStats doc in result
        do {
            stats.push(doc);
        };
    return stats;
}

// ============ Delivery Performance ============
public function upsertDeliveryPerformance(string driverId, boolean completed,
                                           int durationMinutes) returns error? {
    mongodb:Collection col = check getCollection(DELIVERY_PERFORMANCE);

    map<json> inc = {"totalDeliveries": 1};
    if completed {
        inc["completedDeliveries"] = 1;
    }

    _ = check col->updateOne({"driverId": driverId}, {
        "$inc": inc,
        "$set": {"lastUpdated": time:utcToString(time:utcNow())},
        "$setOnInsert": {"driverId": driverId}
    }, {upsert: true});

    if completed && durationMinutes > 0 {
        DeliveryPerformance|mongodb:Error? doc = col->findOne({"driverId": driverId});
        if doc is DeliveryPerformance {
            int completedCount = doc.completedDeliveries;
            decimal currentAvg = doc.averageDeliveryTimeMinutes;
            decimal newAvg = ((currentAvg * <decimal>(completedCount - 1)) +
                              <decimal>durationMinutes) / <decimal>completedCount;
            _ = check col->updateOne({"driverId": driverId}, {
                "$set": {"averageDeliveryTimeMinutes": newAvg}
            });
        }
    }
}

public function getDeliveryPerformance(string driverId) returns DeliveryPerformance|error? {
    mongodb:Collection col = check getCollection(DELIVERY_PERFORMANCE);
    DeliveryPerformance|mongodb:Error? result = col->findOne({"driverId": driverId});
    if result is mongodb:Error {
        return result;
    }
    return result;
}

public function getAllDeliveryPerformance(int count = 100) returns DeliveryPerformance[]|error {
    mongodb:Collection col = check getCollection(DELIVERY_PERFORMANCE);

    stream<DeliveryPerformance, mongodb:Error?> result = check col->find(
        {},
        {sort: {"completedDeliveries": -1}, 'limit: count}
    );
    DeliveryPerformance[] perf = [];
    check from DeliveryPerformance doc in result
        do {
            perf.push(doc);
        };
    return perf;
}

// ============ Daily Revenue ============
public function recordDailyRevenue(decimal amount, boolean isRefund) returns error? {
    mongodb:Collection col = check getCollection(DAILY_REVENUE);

    string today = time:utcToString(time:utcNow()).substring(0, 10);

    map<json> inc = isRefund
        ? {"refunds": amount, "orderCount": 0}
        : {"revenue": amount, "orderCount": 1};

    _ = check col->updateOne({"date": today}, {
        "$inc": inc,
        "$set": {"date": today}
    }, {upsert: true});
}

public function getDailyRevenue(string fromDate, string toDate) returns DailyRevenue[]|error {
    mongodb:Collection col = check getCollection(DAILY_REVENUE);

    stream<DailyRevenue, mongodb:Error?> result = check col->find(
        {"date": {"$gte": fromDate, "$lte": toDate}},
        {sort: {"date": 1}}
    );

    DailyRevenue[] revenues = [];
    check from DailyRevenue doc in result
        do {
            DailyRevenue r = {
                date: doc.date,
                orderCount: doc.orderCount,
                revenue: doc.revenue,
                refunds: doc.refunds,
                netRevenue: doc.revenue - doc.refunds
            };
            revenues.push(r);
        };
    return revenues;
}

// ============ Platform Summary ============
public function getPlatformSummary() returns PlatformSummary|error {
    mongodb:Collection ledgerCol = check getCollection(ORDER_LEDGER);

    int totalOrders = check ledgerCol->countDocuments({});
    int completedOrders = check ledgerCol->countDocuments({"status": "DELIVERED"});
    int cancelledOrders = check ledgerCol->countDocuments({"status": "CANCELLED"});
    int activeOrders = totalOrders - completedOrders - cancelledOrders;

    decimal totalRevenue = 0;
    decimal totalRefunds = 0;

    // Use typed record for stream — connector requires a record, not map<json>
    stream<OrderLedgerEntry, mongodb:Error?> allOrders = check ledgerCol->find({});
    check from OrderLedgerEntry doc in allOrders
        do {
            if doc.status == "DELIVERED" {
                totalRevenue += doc.totalAmount;
            } else if doc.status == "CANCELLED" {
                totalRefunds += doc.totalAmount;
            }
        };

    decimal avgOrderValue = completedOrders > 0
        ? totalRevenue / <decimal>completedOrders
        : 0;

    mongodb:Collection restCol = check getCollection(RESTAURANT_STATS);
    int totalRestaurants = check restCol->countDocuments({});

    mongodb:Collection driverCol = check getCollection(DELIVERY_PERFORMANCE);
    int totalDrivers = check driverCol->countDocuments({});

    return {
        totalOrders: totalOrders,
        activeOrders: activeOrders,
        completedOrders: completedOrders,
        cancelledOrders: cancelledOrders,
        totalRevenue: totalRevenue,
        totalRefunds: totalRefunds,
        totalRestaurants: totalRestaurants,
        totalCustomers: 0,
        totalDrivers: totalDrivers,
        averageOrderValue: avgOrderValue,
        reportGeneratedAt: time:utcToString(time:utcNow())
    };
}

//==== Report Persistence ============
public function saveReport(AdminReport report) returns error? {
    mongodb:Collection col = check getCollection(REPORTS);
    _ = check col->insertOne(report);
}

public function getReport(string reportId) returns AdminReport|error? {
    mongodb:Collection col = check getCollection(REPORTS);
    AdminReport|mongodb:Error? result = col->findOne({"reportId": reportId});
    if result is mongodb:Error {
        return result;
    }
    return result;
}
