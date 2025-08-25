/*
==================================================================
This is the backup work for the data cleansing and standardization process required to load from bronze to silver layers.
This was done for the three tables from the ERP source file.
This includes all steps completed.
=====================================================================
*/


---------- Silver Layer Work for ERP source file. --------------------
-- Starting with erp_CUST_AZ12 - checking table and checking for joinabililty
SELECT 
cid,
bdate,
gen
FROM bronze.erp_CUST_AZ12
WHERE cid LIKE '%AW00011000';

SELECT *
FROM silver.crm_cust_info;

-- Cleaning cid column for joinability per data flow chart
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_CUST_AZ12

-- Moving onto bdate column, Identify Out-of-Range dates
SELECT DISTINCT
bdate
FROM bronze.erp_CUST_AZ12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Transforming bdate to account for out-of-range dates in main query
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END bdate,
gen
FROM bronze.erp_CUST_AZ12

-- Moving onto gen column, checking for data standardization & consistency
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
END gen
FROM bronze.erp_CUST_AZ12

-- Adding to main query and inserting into Silver layer
INSERT INTO silver.erp_CUST_AZ12 (
cid,
bdate,
gen)

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ----- Removes 'NAS%' prefix where needed
	ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL -------- Sets future birthdates to NULL
	ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
END gen -------- Normalize gender values and handle unknown cases
FROM bronze.erp_CUST_AZ12

-- Data Quality Checks 
----Identify Out-of-Range dates
SELECT DISTINCT
bdate
FROM silver.erp_CUST_AZ12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
----Checking Data Standardization & Consistency
SELECT DISTINCT
gen
FROM silver.erp_CUST_AZ12
----Final Full check
SELECT * FROM silver.erp_CUST_AZ12

----------------------------------------------------------

-- Moving onto erp_LOC_A101
-- Checking data joinability with cid column per data flow chart
SELECT 
cid,
cntry
FROM bronze.erp_LOC_A101;

SELECT cst_key FROM silver.crm_cust_info;

--- Cleaning cid column
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_LOC_A101

-- Moving onto cntry column
SELECT DISTINCT
cntry
FROM bronze.erp_LOC_A101
ORDER BY cntry

-- Cleaning cntry column within main query
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_LOC_A101

-- Insert into Silver layer table
INSERT INTO silver.erp_LOC_A101(cid,cntry)

SELECT 
REPLACE(cid, '-', '') cid, -- Handles invalid values in cid column
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END cntry -- Normalize cntry values and handle missing or blank country codes
FROM bronze.erp_LOC_A101

-- Data Quality Checks 
SELECT DISTINCT
cntry
FROM silver.erp_LOC_A101
ORDER BY cntry

SELECT * FROM silver.erp_LOC_A101


----------------------------------------------------------

-- Moving onto erp_PX_CAT_G1V2
-- First ID column already matches column from crm_cust_info, no action needed
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_PX_CAT_G1V2

-- Checking for unwanted spaces in following substring columns
-- Expectation: No results
SELECT * FROM bronze.erp_PX_CAT_G1V2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
-- Data is all clean and ready to be inserted into silver layer
SELECT DISTINCT
cat
FROM bronze.erp_PX_CAT_G1V2;
SELECT DISTINCT
subcat
FROM bronze.erp_PX_CAT_G1V2;
SELECT DISTINCT
maintenance
FROM bronze.erp_PX_CAT_G1V2;

-- Inserting into Silver layer
INSERT INTO silver.erp_PX_CAT_G1V2 (id,cat,subcat,maintenance)

SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_PX_CAT_G1V2

-- Checking silver 

SELECT * FROM silver.erp_PX_CAT_G1V2
