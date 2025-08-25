----------Creating Gold Layer------------
-- Creating SALES table for Gold Layer
--- All from silver.crm_sales_details and determined to be FACT table
--- Using dimension table's surrogate keys to link to fact table
--------- Renaming for improved readability
------------- Creating View
CREATE VIEW gold.fact_sales AS

SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details AS sd	
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = customer_id


-- Data Check
-- Foreign Key Integrity (Dimensions)
----- Expectation: No results
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
