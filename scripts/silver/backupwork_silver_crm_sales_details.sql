/*
==================================================================
This is the backup work for the data cleansing and standardization process required to load from bronze to silver layers.
This was done for the crm_sales_details table.
This includes all steps completed.
=====================================================================
*/

-- Checking for unwanted spaces
-- Expectation: No Results
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Checking for connectivity using prd_key
-- Expectation: No results, meaning can connect crm_prd_info and crm_sales_details with prd_key

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) 


-- Converting date columns from INT to DATE data type
------- Checking quality of dates columns
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
------- Converting bad data to NULLS and rest to DATE type
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details


-- Check for invalid date numbers
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt < sls_order_dt OR sls_order_dt > sls_due_dt


-- Checking data quality for last 3 columns
SELECT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Rules for enhancing data quality of sales, quantity, and price columns
--------- If Sales is negative, zero, or null, derive it using Quantity and Price
--------- If Price is zero or null, calculate it using Sales and Quantity
--------- If Price is negative, convert it to a positive value
SELECT
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
END sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
END sls_price
FROM bronze.crm_sales_details

-- Adding above data cleanse to main query and Inserting into silver table
--Inserting the cleaned & standardized data
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
END sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
END sls_price
FROM bronze.crm_sales_details


---- Silver Quality Check
------ Check for invalid date numbers
------- Expectation: No results
SELECT *
FROM silver.crm_sales_details
WHERE sls_ship_dt < sls_order_dt OR sls_order_dt > sls_due_dt

------- Checking if data following rules
------- Expectation: No results
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

------ Final full check
SELECT *
FROM silver.crm_sales_details
