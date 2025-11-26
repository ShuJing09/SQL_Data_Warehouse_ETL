DROP VIEW IF EXISTS gold.dim_customers;
CREATE OR REPLACE VIEW gold.dim_customers AS

SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    el.cntry AS country,
    ci.cst_marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 el ON ci.cst_key = el.cid




DROP VIEW IF EXISTS gold.dim_products;
CREATE OR REPLACE VIEW gold.dim_products AS

SELECT
    ROW_NUMBER() OVER (ORDER BY cp.prd_start_dt, cp.prd_key) AS product_key,
    cp.prd_id AS product_id,
    cp.prd_key AS product_number,
    cp.prd_nm AS product_name,
    cp.cat_id AS category_id,
    ec.cat AS category,
    ec.subcat AS subcategory,
    ec.maintenance,
    cp.prd_cost AS cost,
    cp.prd_line AS product_line,
    cp.prd_start_dt AS start_date
FROM silver.crm_prd_id cp
LEFT JOIN silver.erp_px_cat_g1v2 ec ON cp.cat_id = ec.id
WHERE cp.prd_end_dt IS NULL -- Filter out all historical data, meaning retrieve current data only



DROP VIEW IF EXISTS gold.fact_sales;
CREATE OR REPLACE VIEW gold.fact_sales AS

SELECT
    csd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    csd.sls_order_dt AS order_date,
    csd.sls_ship_dt AS ship_date,
    csd.sls_due_dt AS due_date,
    csd.sls_sales AS sales_amount,
    csd.sls_quantity AS quantity,
    csd.sls_price AS price
FROM silver.crm_sales_details csd
LEFT JOIN gold.dim_products pr ON csd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu ON csd.sls_cust_id = cu.customer_id
ORDER BY quantity, order_number

SELECT * FROM gold.fact_sales