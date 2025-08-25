----------Creating Gold Layer------------
-- Creating PRODUCTS table for Gold Layer
--- Starting from master table (crm_prd_info)
SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filters for current data


-- Ensuring product keys are unique
-- Expectation: No results
SELECT prd_key, COUNT(*) FROM (
SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filters for current data
) t GROUP BY prd_key
HAVING COUNT(*) > 1

-- Sorting columns to improve readability, renaming columns and creating surrogate key
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filters for current data


-- Creating view for dimension table for products
CREATE VIEW gold.dim_products AS

SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filters for current data
