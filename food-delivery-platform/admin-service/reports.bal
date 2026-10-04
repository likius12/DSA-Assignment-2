import ballerina/time;
import ballerina/uuid;

// ============ Report Generators ============

public function generatePlatformSummary() returns AdminReport|error {
    PlatformSummary summary = check getPlatformSummary();
    return {
        reportId: uuid:createType4AsString(),
        reportType: "PLATFORM_SUMMARY",
        generatedAt: time:utcToString(time:utcNow()),
        data: summary
    };
}

public function generateRestaurantReport(string? restaurantId) returns AdminReport|error {
    json data;
    if restaurantId is string {
        RestaurantStats? stats = check getRestaurantStats(restaurantId);
        data = stats ?: {"message": "No data for restaurant " + restaurantId};
    } else {
        RestaurantStats[] stats = check getAllRestaurantStats();
        data = stats;
    }
    return {
        reportId: uuid:createType4AsString(),
        reportType: "RESTAURANT_REPORT",
        generatedAt: time:utcToString(time:utcNow()),
        data: data
    };
}

public function generateDeliveryReport(string? driverId) returns AdminReport|error {
    json data;
    if driverId is string {
        DeliveryPerformance? perf = check getDeliveryPerformance(driverId);
        data = perf ?: {"message": "No data for driver " + driverId};
    } else {
        DeliveryPerformance[] perf = check getAllDeliveryPerformance();
        data = perf;
    }
    return {
        reportId: uuid:createType4AsString(),
        reportType: "DELIVERY_REPORT",
        generatedAt: time:utcToString(time:utcNow()),
        data: data
    };
}

public function generateRevenueReport(string fromDate, string toDate) returns AdminReport|error {
    DailyRevenue[] revenues = check getDailyRevenue(fromDate, toDate);
    return {
        reportId: uuid:createType4AsString(),
        reportType: "REVENUE_REPORT",
        generatedAt: time:utcToString(time:utcNow()),
        data: {
            "from": fromDate,
            "to": toDate,
            "dailyRevenues": revenues
        }
    };
}

public function generateTopRestaurantsReport(int topN) returns AdminReport|error {
    RestaurantStats[] all = check getAllRestaurantStats(1000);
    // already sorted by totalRevenue desc in DB query
    RestaurantStats[] top = all.length() > topN ? all.slice(0, topN) : all;
    return {
        reportId: uuid:createType4AsString(),
        reportType: "TOP_RESTAURANTS",
        generatedAt: time:utcToString(time:utcNow()),
        data: top
    };
}

public function generateTopDriversReport(int topN) returns AdminReport|error {
    DeliveryPerformance[] all = check getAllDeliveryPerformance(1000);
    DeliveryPerformance[] top = all.length() > topN ? all.slice(0, topN) : all;
    return {
        reportId: uuid:createType4AsString(),
        reportType: "TOP_DRIVERS",
        generatedAt: time:utcToString(time:utcNow()),
        data: top
    };
}
