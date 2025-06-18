-- ==========================================
-- STEP 1: Merge Semua Tabel ke View full_data
-- ==========================================
CREATE MATERIALIZED VIEW full_data AS
SELECT
    o.*, 
    oi.product_id, oi.seller_id, oi.shipping_limit_date, oi.price, oi.freight_value, oi.order_item_id,
    p.payment_type, p.payment_installments, p.payment_value,
    r.review_id, r.review_score, r.review_creation_date, r.review_answer_timestamp,
    c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state,
    pr.product_category_name
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
LEFT JOIN reviews r ON o.order_id = r.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products pr ON oi.product_id = pr.product_id;

-- ==========================================
-- STEP 2: Handling missing value 
-- ==========================================
CREATE TEMP VIEW value_stats AS
WITH
    -- handling data numerikal dengan median
    medians AS (
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS med_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY freight_value) AS med_freight,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY payment_value) AS med_pay_val,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY payment_installments) AS med_install
        FROM full_data
    ),
    -- handling kategorikal dengan modus
    modes AS (
        SELECT
            (SELECT payment_type FROM full_data WHERE payment_type IS NOT NULL GROUP BY payment_type ORDER BY COUNT(*) DESC LIMIT 1) AS mode_payment,
            (SELECT product_category_name FROM full_data WHERE product_category_name IS NOT NULL GROUP BY product_category_name ORDER BY COUNT(*) DESC LIMIT 1) AS mode_category
    )
SELECT * FROM medians, modes;

-- ==========================================
-- STEP 3: Imputation beberapa data yang masih null
-- ==========================================
CREATE MATERIALIZED VIEW clean_data AS
SELECT 
    fd.order_id,
    COALESCE(fd.price, vs.med_price) AS price,
    COALESCE(fd.freight_value, vs.med_freight) AS freight_value,
    COALESCE(fd.payment_value, vs.med_pay_val) AS payment_value,
    COALESCE(fd.payment_installments, vs.med_install) AS payment_installments,
    COALESCE(fd.payment_type, vs.mode_payment) AS payment_type,
    COALESCE(fd.product_category_name, vs.mode_category) AS product_category_name,
    COALESCE(fd.review_score::TEXT, 'unknown') AS review_score,
    COALESCE(fd.review_id, 'unknown') AS review_id,
    -- melakukan feature enrichment 
    CASE 
        WHEN fd.payment_type = 'credit_card' THEN 'credit_card'
        ELSE 'non_credit'
    END AS payment_type_grouped,
    COALESCE(fd.product_id, 'unknown') AS product_id,
    COALESCE(fd.seller_id, 'unknown') AS seller_id,
    COALESCE(fd.order_item_id, 1)::INT AS order_item_id,
    COALESCE(fd.shipping_limit_date, LEAD(fd.shipping_limit_date) OVER (PARTITION BY fd.seller_id ORDER BY fd.shipping_limit_date)) AS shipping_limit_date,
    fd.order_purchase_timestamp,
    fd.order_delivered_customer_date,
    fd.order_estimated_delivery_date,
    fd.customer_id
FROM full_data fd, value_stats vs;

-- ==========================================
-- STEP 4: Data validation (memastikan logic untuk fitur delivery)
-- ==========================================
CREATE TEMP VIEW valid_delivery AS
SELECT 
    EXTRACT(DAY FROM order_delivered_customer_date - order_purchase_timestamp) AS delivery_days
FROM clean_data
WHERE order_delivered_customer_date IS NOT NULL 
  AND order_purchase_timestamp IS NOT NULL 
  AND order_delivered_customer_date >= order_purchase_timestamp;

CREATE TEMP VIEW delivery_median AS
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_days) AS med_delivery FROM valid_delivery;

-- STEP 5:Data final
CREATE MATERIALIZED VIEW final_dataset AS
SELECT 
    cd.*,
    CASE 
        WHEN cd.order_delivered_customer_date < cd.order_purchase_timestamp THEN 
            cd.order_purchase_timestamp + (dm.med_delivery || ' days')::INTERVAL
        ELSE cd.order_delivered_customer_date
    END AS fixed_delivery_date,
    (COALESCE(cd.order_delivered_customer_date, cd.order_purchase_timestamp + (dm.med_delivery || ' days')::INTERVAL) - cd.order_purchase_timestamp)::INT AS delivery_time_days
FROM clean_data cd, delivery_median dm;
