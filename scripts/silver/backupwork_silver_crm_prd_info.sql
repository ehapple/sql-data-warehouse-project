/*
==================================================================
This is the backup work for the data cleansing and standardization process required to load from bronze to silver layers.
This was done for the crm_prd_info table.
This includes all steps completed.
=====================================================================
*/

-- Data Quality Check
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- Going row by row cleaning data as needed
-- Check for Nulls and/or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Seperating the prd_key into 2 separate columns to increase joinability with other tables
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- to join with erp_PX_CAT_G1V2 table
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- to join with crm_sales_details table
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


-- Moving onto prd_nm, check for unwanted spaces for string value columns (results means extra spaces)
-- Expectation: No Result, so no cleansing for column needed
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- prd_cost, Data Standardization & Consistency, confirming values and removing nulls
------ Confirming values,checking for nulls and negatives
SELECT DISTINCT prd_cost
FROM bronze.crm_prd_info

------- Adding clean up for prd_cost to main query
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- to join with erp_PX_CAT_G1V2 table
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- to join with crm_sales_details table
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, --removing nulls and replacing with 0
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- Moving onto prd_line, checking values and see abbreviations.
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

------ Adding lengthening abbreviations for data consistency in main query for prd_line
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- to join with erp_PX_CAT_G1V2 table
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- to join with crm_sales_details table
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, --removing nulls and replacing with 0
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


-- Moving onto prd_start_dt and prd_end_dt, checking for invalid date orders 
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------- Converting End date to equal start date of the next record - 1
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- to join with erp_PX_CAT_G1V2 table
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- to join with crm_sales_details table
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, --removing nulls and replacing with 0
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info


--Inserting the cleaned & standardized data
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- to join with erp_PX_CAT_G1V2 table
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- to join with crm_sales_details table
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, --removing nulls and replacing with 0
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info


-- Silver Quality Check
------ Check for Nulls and/or Duplicates
------ Expectation: No Result
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

------ Check for unwanted spaces (results means extra spaces)
------ Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

------ Confirming values,checking for nulls and negatives
------ Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

------ Data Standardization Check
------ Expectation: Friendly, stnadardized 
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

------ Check for invalid date orders
------ Expectation: No Result
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------ Final Full Check
SELECT * 
FROM silver.crm_prd_info
