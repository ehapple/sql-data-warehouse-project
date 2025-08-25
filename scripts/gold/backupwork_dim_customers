----------Creating Gold Layer------------
-- Creating CUSTOMERS table for Gold Layer
--- Starting from master table, joining customer tables
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_CUST_AZ12 AS ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 AS la
ON		ci.cst_key = la.cid


-- Checking JOINS quality by checking for duplicates
---- Expectation: No results
SELECT cst_id, COUNT(*) FROM(
	SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_CUST_AZ12 AS ca
	ON		ci.cst_key = ca.cid
	LEFT JOIN silver.erp_LOC_A101 AS la
	ON		ci.cst_key = la.cid
) t GROUP BY cst_id
HAVING COUNT(*) > 1


-- Data integration for duplicate gender columns from mult tables
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr ---- CRM is the master for gender info
		ELSE COALESCE(ca.gen,'N/A')
	END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_CUST_AZ12 AS ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 AS la
ON		ci.cst_key = la.cid
ORDER BY 1,2

-- Adding above data integration into main query and renaming and organizing columns
SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr ---- CRM is the master for gender info
		ELSE COALESCE(ca.gen,'N/A')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_CUST_AZ12 AS ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 AS la
ON		ci.cst_key = la.cid


-- Creating surrogate key using row window function, and creating view 
CREATE VIEW gold.dim_customers AS

SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr ---- CRM is the master for gender info
		ELSE COALESCE(ca.gen,'N/A')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_CUST_AZ12 AS ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 AS la
ON		ci.cst_key = la.cid
