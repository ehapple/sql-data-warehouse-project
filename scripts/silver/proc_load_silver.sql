/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

-- Creating stored procedure
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

-- Whole Process time tracking
    DECLARE @batch_start_time DATETIME;
    DECLARE @batch_end_time   DATETIME;

	-- Per table Time tracking for tracking efficiency
	DECLARE @start_time DATETIME, @end_time DATETIME;

	-- Error Handling
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		-- Adding prints for script process tracking
		PRINT '==============================';
		PRINT 'Loading Silver Layer';
		PRINT '==============================';

		PRINT '----------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------';

	-------------- FULL INSERT FROM BRONZE TO SILVER FOR ALL 6 TABLES ---------------

	-- Starting with crm_cust_info
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_cust_info'
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting data into: silver.crm_cust_info'
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A'
		END cst_gndr,
		cst_create_date
	FROM(	SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL) AS t
	WHERE flag_last = 1
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
	PRINT '--------------------------------';

	-- CRM_PRD_INFO
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info'
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting data into: silver.crm_prd_info'
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
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
	PRINT '--------------------------------';

	-- CRM_SALES_DETAILS
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details'
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting data into: silver.crm_sales_details'
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
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
	PRINT '--------------------------------';

		PRINT '----------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------';

	--- ERP Source Tables
	------ silver.erp_CUST_AZ12
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_CUST_AZ12'
	TRUNCATE TABLE silver.erp_CUST_AZ12;
	PRINT '>> Inserting data into: silver.erp_CUST_AZ12'
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
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
	PRINT '--------------------------------';

	------silver.erp_LOC_A101
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_LOC_A101'
	TRUNCATE TABLE silver.erp_LOC_A101;
	PRINT '>> Inserting data into: silver.erp_LOC_A101'
	INSERT INTO silver.erp_LOC_A101(cid,cntry)
	SELECT 
		REPLACE(cid, '-', '') cid, -- Handles invalid values in cid column
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			ELSE TRIM(cntry)
		END cntry -- Normalize cntry values and handle missing or blank country codes
	FROM bronze.erp_LOC_A101
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
	PRINT '--------------------------------';

	------- silver.erp_PX_CAT_G1V2
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2'
	TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
	PRINT '>> Inserting data into: silver.erp_PX_CAT_G1V2'
	INSERT INTO silver.erp_PX_CAT_G1V2 (id,cat,subcat,maintenance)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_PX_CAT_G1V2
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
	PRINT '--------------------------------';

	-- Total Silver duration
	SET @batch_end_time = GETDATE();
	PRINT '=== TOTAL SILVER LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(20)) + ' seconds';

	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'ERROR OCCURRED LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================';
	END CATCH
END
