sales_raw = LOAD 'hdfs://localhost:9000/assignment/pig/sales.csv'
    USING PigStorage(',')
    AS (
        order_id:int,
        customer_id:chararray,
        product:chararray,
        category:chararray,
        price_rs:int,
        quantity:int,
        order_date:chararray,
        order_hour:int
    );

sales = FILTER sales_raw BY order_id IS NOT NULL AND order_id != 0;

grouped_products = GROUP sales BY product;

product_sales = FOREACH grouped_products GENERATE
    group                   AS product,
    SUM(sales.quantity)     AS total_quantity,
    COUNT(sales)            AS total_orders;

best_selling_products = ORDER product_sales BY total_quantity DESC;

DUMP best_selling_products;

STORE best_selling_products
    INTO 'hdfs://localhost:9000/assignment/pig/A7/O1/best_selling_products'
    USING PigStorage(',');

sales_with_revenue = FOREACH sales GENERATE
    category,
    (price_rs * quantity) AS revenue_rs;

grouped_category = GROUP sales_with_revenue BY category;

category_revenue = FOREACH grouped_category GENERATE
    group                                       AS category,
    SUM(sales_with_revenue.revenue_rs)          AS total_revenue_rs,
    COUNT(sales_with_revenue)                   AS total_orders;

sorted_category_revenue = ORDER category_revenue BY total_revenue_rs DESC;

DUMP sorted_category_revenue;

STORE sorted_category_revenue
    INTO 'hdfs://localhost:9000/assignment/pig/A7/O2/category_revenue'
    USING PigStorage(',');

grouped_hours = GROUP sales BY order_hour;

hourly_orders = FOREACH grouped_hours GENERATE
    group           AS hour,
    COUNT(sales)    AS total_orders;

peak_hours = ORDER hourly_orders BY total_orders DESC;

DUMP peak_hours;

STORE peak_hours
    INTO 'hdfs://localhost:9000/assignment/pig/A7/O3/peak_buying_hours'
    USING PigStorage(',');
