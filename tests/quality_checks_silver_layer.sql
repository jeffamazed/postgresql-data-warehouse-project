-- Check for NULL or duplicate customer IDs (should not exist)
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Detect leading/trailing spaces in customer keys
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- List all unique marital status values to check for inconsistencies
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- ----------------------------------------------------------------------

-- Check for NULL or duplicate product IDs
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Detect unwanted spaces in product names
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Identify NULL or negative product costs
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Check distinct product lines for consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Detect products where end date is before start date
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ----------------------------------------------------------------------

-- Identify invalid due dates (format, range, or zero)
SELECT DISTINCT sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0
   OR LENGTH(sls_due_dt::text) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-- Check if order dates occur after ship or due dates
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Verify sales amount equals quantity × price, and all values are positive
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price;

-- ----------------------------------------------------------------------

-- Check for out-of-range birthdates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < DATE '1924-01-01'
   OR bdate > NOW();

-- List distinct gender values for standardization review
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- ----------------------------------------------------------------------

-- Check country field values for standardization
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ----------------------------------------------------------------------

-- Detect unwanted spaces in category-related fields
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Check unique maintenance types for consistency
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;
