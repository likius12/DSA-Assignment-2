// ---------- Types ----------
type OpeningHours record {|
    string open;   // e.g. "08:00"
    string close;  // e.g. "21:00"
|};

type RestaurantInput record {|
    string name;
    OpeningHours openingHours;
|};

type Restaurant record {|
    string id;
    string name;
    OpeningHours openingHours;
    boolean isOpen;
|};

type MenuItemInput record {|
    string name;
    decimal price;
    int stockQty;
    boolean available = true;
|};

type MenuItem record {|
    string id;
    string restaurantId;
    string name;
    decimal price;
    int stockQty;
    boolean available;
|};

type MenuItemUpdate record {|
    string name?;
    decimal price?;
    boolean available?;
|};

type StockUpdate record {|
    int stockQty;
|};

// ---------- Kafka event types ----------
type OrderItem record {|
    string itemId;
    int quantity;
|};

type OrderCreated record {|
    string orderId;
    string restaurantId;
    string customerId;
    OrderItem[] items;
|};

type RestaurantDecision record {|
    string orderId;
    string restaurantId;
    string status;   // CONFIRMED | REJECTED | READY
    string reason?;
|};
