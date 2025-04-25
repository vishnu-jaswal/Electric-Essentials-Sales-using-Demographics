-- Load the dataset from HDFS
sales = LOAD 'salesdata.csv' 
    USING PigStorage(',') 
    AS (product:chararray, location:chararray, quantity:int, liked:int);

-- Most Sold Product Overall
grp_all = GROUP sales BY product;
total_qty = FOREACH grp_all GENERATE group AS product, SUM(sales.quantity) AS total_quantity;
sorted_qty = ORDER total_qty BY total_quantity DESC;
top_product = LIMIT sorted_qty 1;

-- Most Sold Product at Each Location
grp_loc = GROUP sales BY (location, product);
qty_by_loc = FOREACH grp_loc GENERATE group.location AS location, group.product AS product, SUM(sales.quantity) AS quantity;
sorted_qty_loc = ORDER qty_by_loc BY location, quantity DESC;

-- Most Liked Product Overall
grp_liked = GROUP sales BY product;
liked_total = FOREACH grp_liked GENERATE group AS product, SUM(sales.liked) AS total_likes;
sorted_liked = ORDER liked_total BY total_likes DESC;
top_liked = LIMIT sorted_liked 1;

-- Most Liked Product at Each Location
grp_liked_loc = GROUP sales BY (location, product);
liked_by_loc = FOREACH grp_liked_loc GENERATE group.location AS location, group.product AS product, SUM(sales.liked) AS likes;
sorted_liked_loc = ORDER liked_by_loc BY location, likes DESC;

-- Products with Decrementing Sales
-- Uncomment if a 'date' field exists
-- sales_by_date = GROUP sales BY (product, date);
-- daily_qty = FOREACH sales_by_date GENERATE group.product AS product, group.date AS date, SUM(sales.quantity) AS qty;
-- sorted_daily = ORDER daily_qty BY product, date;
-- Use UDF or additional logic to detect declining trend

-- Dump top results
DUMP top_product;
DUMP sorted_qty_loc;
DUMP top_liked;
DUMP sorted_liked_loc;
