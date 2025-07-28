-- =============================================================
-- Drop and Recreate Tables in the 'bronze' schema
-- =============================================================

-- Drop tables if they exist
DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.erp_loc_a101;
DROP TABLE IF EXISTS bronze.erp_cust_az12;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

-- =============================================================
-- Create Customer Info Table
-- =============================================================
CREATE TABLE bronze.crm_cust_info (
  cst_id INT,
  cst_key VARCHAR(50),
  cst_firstname VARCHAR(50),
  cst_lastname VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr VARCHAR(50),
  cst_create_data DATE
);

-- =============================================================
-- Create Product Info Table
-- =============================================================
CREATE TABLE bronze.crm_prd_info (
  prd_id INT,
  prd_key VARCHAR(50),
  prd_nm VARCHAR(50),
  prd_cost INT,
  prd_line VARCHAR(10),
  prd_start_dt TIMESTAMP,
  prd_end_dt TIMESTAMP
);

-- =============================================================
-- Create Sales Details Table
-- =============================================================
CREATE TABLE bronze.crm_sales_details (
  sls_ord_num VARCHAR(50),
  sls_prd_key VARCHAR(50),
  sls_cust_id INT,
  sls_order_dt INT,
  sls_ship_dt INT,
  sls_due_dt INT,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT
);

-- =============================================================
-- Create Location Info Table
-- =============================================================
CREATE TABLE bronze.erp_loc_a101 (
  cid VARCHAR(50),
  cntry VARCHAR(30)
);

-- =============================================================
-- Create ERP Customer Info Table
-- =============================================================
CREATE TABLE bronze.erp_cust_az12 (
  cid VARCHAR(50),
  bdate DATE,
  gen VARCHAR(10)
);

-- =============================================================
-- Create Product Category Mapping Table
-- =============================================================
CREATE TABLE bronze.erp_px_cat_g1v2 (
  id VARCHAR(20),
  cat VARCHAR(20),
  subcat VARCHAR(40),
  maintenance VARCHAR(10)
);

-- =============================================================
-- Bulk Insert from CSV Files
-- NOTE: These are **relative paths**, so make sure to run this script
--       from the correct working directory where the 'datasets/' folder exists.
-- =============================================================

-- Clear existing data before reloading
TRUNCATE TABLE bronze.crm_cust_info;
TRUNCATE TABLE bronze.crm_prd_info;
TRUNCATE TABLE bronze.sales_details;
TRUNCATE TABLE bronze.erp_cust_az12;
TRUNCATE TABLE bronze.erp_loc_a101;
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

-- Load fresh data from CSV files
\COPY bronze.crm_cust_info FROM 'datasets/source_crm/cust_info.csv' WITH (FORMAT csv, HEADER true);
\COPY bronze.crm_prd_info FROM 'datasets/source_crm/prd_info.csv' WITH (FORMAT csv, HEADER true);
\COPY bronze.crm_sales_details FROM 'datasets/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true);
\COPY bronze.erp_cust_az12 FROM 'datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT csv, HEADER true);
\COPY bronze.erp_loc_a101 FROM 'datasets/source_erp/LOC_A101.csv' WITH (FORMAT csv, HEADER true);
\COPY bronze.erp_px_cat_g1v2 FROM 'datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true);
