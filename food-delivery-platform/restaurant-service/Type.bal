type OpeningHours record {|
    string open;
    string close;
|};

type RestaurantInput record {|
    string name;
    OpeningHours openingHours;
|};

type Restaurant record {|
    string _id;
    string name;
    OpeningHours openingHours;
    boolean isOpen;
    string createdAt;
|};

type MenuItemInput record {|
    string name;
    decimal price;
    int stockQty;
|};

type MenuItem record {|
    string _id;
    string restaurantId;
    string name;
    decimal price;
    int stockQty;
    boolean available;
    string createdAt;
|};

type MenuItemUpdate record {|
    string name?;
    decimal price?;
    int stockQty?;
    boolean available?;
|};