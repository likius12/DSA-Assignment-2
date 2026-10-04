// ============ Kafka Event Payloads ============
type OrderCreatedEvent record {|
    string orderId;
    string customerId;
    string restaurantId;
    json[] items?;
    decimal totalAmount;
    string createdAt;
|};

type OrderConfirmedEvent record {|
    string orderId;
    string restaurantId;
    decimal totalAmount;
    string timestamp;
|};

type OrderCancelledEvent record {|
    string orderId;
    string restaurantId;
    string? reason;
    string timestamp;
|};

type PaymentCompletedEvent record {|
    string orderId;
    string restaurantId;
    decimal amount;
    string timestamp;
|};

type DeliveryAssignedEvent record {|
    string orderId;
    string driverId;
    string restaurantId;
    string timestamp;
|};

type DeliveryCompletedEvent record {|
    string orderId;
    string driverId;
    int durationMinutes;
    decimal earning;
    string timestamp;
|};

// ============ Stored Documents ============
type RestaurantStats record {|
    string restaurantId;
    string restaurantName?;
    int totalOrders;
    int completedOrders;
    int cancelledOrders;
    decimal totalRevenue;
    decimal averageOrderValue;
    decimal averagePreparationTimeMinutes;
    string lastUpdated;
|};

type DeliveryPerformance record {|
    string driverId;
    string driverName?;
    int totalDeliveries;
    int completedDeliveries;
    decimal averageDeliveryTimeMinutes;
    decimal onTimePercentage;
    decimal totalEarnings;
    string lastUpdated;
|};

type DailyRevenue record {|
    string date;
    int orderCount;
    decimal revenue;
    decimal refunds;
    decimal netRevenue;
|};

type PlatformSummary record {|
    int totalOrders;
    int activeOrders;
    int completedOrders;
    int cancelledOrders;
    decimal totalRevenue;
    decimal totalRefunds;
    int totalRestaurants;
    int totalCustomers;
    int totalDrivers;
    decimal averageOrderValue;
    string reportGeneratedAt;
|};

type AdminReport record {|
    string reportId;
    string reportType;
    string generatedAt;
    json data;
|};
// For internal projections during aggregation-free scans
type OrderLedgerEntry record {|
    string orderId;
    string customerId;
    string restaurantId;
    decimal totalAmount;
    string status;
    string createdAt;
    string updatedAt;
    string? driverId?;
|};
