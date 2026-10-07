CREATE OR REFRESH MATERIALIZED VIEW dim_product
AS
SELECT
    product_id,
    product_name,
    category,
    brand,
    sku,
    price,
    discount_percentage,
    rating,
    stock,
    availability_status,
    return_policy
FROM youtube_dev.silver.silver_products;

CREATE OR REFRESH MATERIALIZED VIEW dim_customer
AS
SELECT
    customer_id,
    first_name,
    last_name,
    customer_name,
    email,
    phone,
    username,
    age,
    gender
FROM youtube_dev.silver.silver_customers;


CREATE OR REFRESH MATERIALIZED VIEW fact_sales
AS
SELECT
    order_id,
    customer_id,
    product_id,

    quantity,

    unit_price,

    discount_percentage,

    -- Gross sales
    ROUND(
        quantity * unit_price,
        2
    ) AS gross_sales,

    -- Discount amount
    ROUND(
        quantity
        * unit_price
        * discount_percentage / 100,
        2
    ) AS discount_amount,

    -- Net sales
    ROUND(
        quantity
        * unit_price
        * (1 - discount_percentage / 100),
        2
    ) AS net_sales,

    source_file,

    _ingestion_timestamp,

    CURRENT_TIMESTAMP() AS gold_processed_timestamp

FROM youtube_dev.silver.silver_sales;