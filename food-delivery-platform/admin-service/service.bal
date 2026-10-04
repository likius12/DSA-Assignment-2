import ballerina/http;
import ballerina/time;

listener http:Listener adminListener = new (port);

service /api/admin on adminListener {

    resource function get health() returns json {
        return {
            "status": "UP",
            "service": "admin-service",
            "timestamp": time:utcToString(time:utcNow())
        };
    }

    // ---------- Summary ----------
    resource function get reports/summary() returns http:Response {
        return generateReportResponse(generatePlatformSummary());
    }

    // ---------- Restaurants ----------
    resource function get reports/restaurants() returns http:Response {
        return generateReportResponse(generateRestaurantReport(()));
    }

    resource function get reports/restaurants/[string restaurantId]() returns http:Response {
        return generateReportResponse(generateRestaurantReport(restaurantId));
    }

    // ---------- Deliveries ----------
    resource function get reports/deliveries() returns http:Response {
        return generateReportResponse(generateDeliveryReport(()));
    }

    resource function get reports/deliveries/[string driverId]() returns http:Response {
        return generateReportResponse(generateDeliveryReport(driverId));
    }

    // ---------- Revenue ----------
    // GET /api/admin/reports/revenue?from=2026-09-01&to=2026-10-01
    resource function get reports/revenue(http:Request req) returns http:Response {
        string fromDate = req.getQueryParamValue("from") ?: time:utcToString(time:utcNow()).substring(0, 10);
        string toDate = req.getQueryParamValue("to") ?: time:utcToString(time:utcNow()).substring(0, 10);
        return generateReportResponse(generateRevenueReport(fromDate, toDate));
    }

    // ---------- Top Restaurants ----------
    // GET /api/admin/reports/toprestaurants        (default n=10)
    // GET /api/admin/reports/toprestaurants?n=5
    resource function get reports/toprestaurants(http:Request req) returns http:Response {
        string? nStr = req.getQueryParamValue("n");
        int topN = nStr is string ? checkpanic int:fromString(nStr) : 10;
        return generateReportResponse(generateTopRestaurantsReport(topN));
    }

    // ---------- Top Drivers ----------
    // GET /api/admin/reports/topdrivers        (default n=10)
    // GET /api/admin/reports/topdrivers?n=5
    resource function get reports/topdrivers(http:Request req) returns http:Response {
        string? nStr = req.getQueryParamValue("n");
        int topN = nStr is string ? checkpanic int:fromString(nStr) : 10;
        return generateReportResponse(generateTopDriversReport(topN));
    }

    // ---------- Specific report (MUST be last among reports/*) ----------
    resource function get reports/[string reportId]() returns http:Response {
        http:Response res = new;
        AdminReport|error? report = getReport(reportId);
        if report is error {
            res.statusCode = 500;
            res.setJsonPayload({"error": report.message()});
            return res;
        }
        if report == () {
            res.statusCode = 404;
            res.setJsonPayload({"error": "Report not found: " + reportId});
            return res;
        }
        res.setJsonPayload(report);
        return res;
    }

    // ---------- Raw stats ----------
    resource function get stats/restaurants() returns http:Response {
        http:Response res = new;
        RestaurantStats[]|error stats = getAllRestaurantStats();
        if stats is error {
            res.statusCode = 500;
            res.setJsonPayload({"error": stats.message()});
            return res;
        }
        res.setJsonPayload(stats);
        return res;
    }

    resource function get stats/drivers() returns http:Response {
        http:Response res = new;
        DeliveryPerformance[]|error perf = getAllDeliveryPerformance();
        if perf is error {
            res.statusCode = 500;
            res.setJsonPayload({"error": perf.message()});
            return res;
        }
        res.setJsonPayload(perf);
        return res;
    }

    resource function get stats/orders() returns http:Response {
        http:Response res = new;
        PlatformSummary|error summary = getPlatformSummary();
        if summary is error {
            res.statusCode = 500;
            res.setJsonPayload({"error": summary.message()});
            return res;
        }
        res.setJsonPayload(summary);
        return res;
    }
}

// ============ Helper ============
function generateReportResponse(AdminReport|error report) returns http:Response {
    http:Response res = new;
    if report is error {
        res.statusCode = 500;
        res.setJsonPayload({
            "error": "Failed to generate report",
            "message": report.message()
        });
        return res;
    }
    res.setJsonPayload(report);
    return res;
}
