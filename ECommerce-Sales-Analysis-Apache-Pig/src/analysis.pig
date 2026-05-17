-- ============================================================
-- analysis.pig — E-Commerce Sales Analysis using Apache Pig
-- ============================================================
-- Analyzes 500 e-commerce transaction records to produce:
--   1. Best-selling products by total quantity sold
--   2. Category-wise revenue ranking
--   3. Peak buying hours (hour-of-day order distribution)
--
-- Dataset: sales.csv (500 records, 8 fields)
-- HDFS Input:  hdfs://localhost:9000/assignment/pig/sales.csv
-- HDFS Output: hdfs://localhost:9000/assignment/pig/A7/O1-O3/
-- ============================================================

-- ── STEP 1: Load raw sales data ──────────────────────────────
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

-- ── STEP 2: Filter out header row and null records ────────────
sales = FILTER sales_raw BY order_id IS NOT NULL AND order_id != 0;

-- ─────────────────────────────────────────────────────────────
-- ANALYSIS 1: Best-Selling Products by Quantity
-- Groups all orders by product name, sums quantities and orders
-- Output: product, total_quantity, total_orders
-- ─────────────────────────────────────────────────────────────
grouped_products = GROUP sales BY product;

product_sales = FOREACH grouped_products GENERATE
    group                   AS product,
    SUM(sales.quantity)     AS total_quantity,
    COUNT(sales)            AS total_orders;

-- Sort descending by quantity (best sellers first)
best_selling_products = ORDER product_sales BY total_quantity DESC;

DUMP best_selling_products;

STORE best_selling_products
    INTO 'hdfs://localhost:9000/assignment/pig/A7/O1/best_selling_products'
    USING PigStorage(',');

-- ─────────────────────────────────────────────────────────────
-- ANALYSIS 2: Category-Wise Revenue
-- Computes revenue = price × quantity per order,
-- then aggregates by product category.
-- Output: category, total_revenue_rs, total_orders
-- ─────────────────────────────────────────────────────────────
sales_with_revenue = FOREACH sales GENERATE
    category,
    (price_rs * quantity) AS revenue_rs;

grouped_category = GROUP sales_with_revenue BY category;

category_revenue = FOREACH grouped_category GENERATE
    group                                       AS category,
    SUM(sales_with_revenue.revenue_rs)          AS total_revenue_rs,
    COUNT(sales_with_revenue)                   AS total_orders;

-- Sort descending by revenue (most profitable categories first)
sorted_category_revenue = ORDER category_revenue BY total_revenue_rs DESC;

DUMP sorted_category_revenue;

STORE sorted_category_revenue
    INTO 'hdfs://localhost:9000/assignment/pig/A7/O2/category_revenue'
    USING PigStorage(',');

-- ─────────────────────────────────────────────────────────────
-- ANALYSIS 3: Peak Buying Hours
-- Groups orders by hour of day (0–23) to identify when
-- customers are most active.
-- Output: hour, total_orders
-- ─────────────────────────────────────────────────────────────
grouped_hours = GROUP sales BY order_hour;

hourly_orders = FOREACH grouped_hours GENERATE
    group           AS hour,
    COUNT(sales)    AS total_orders;

-- Sort descending (peak hours first)
peak_hours = ORDER hourly_orders BY total_orders DESC;

DUMP peak_hours;

STORE peak_hours
    INTO 'hdfs://localhost:9000/assignment/pig/A7/O3/peak_buying_hours'
    USING PigStorage(',');
