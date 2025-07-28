CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer;
  start_time timestamp;
  end_time timestamp;
BEGIN
  BEGIN -- Start try block

  -- Modify from bronze layer and insert into silver layer crm_cust_info

  -- for time
  RAISE NOTICE '=== Processing silver.crm_cust_info ===';
  start_time := clock_timestamp();
  RAISE NOTICE 'Start time: %', start_time;

  RAISE NOTICE 'Truncating table: silver.crm_cust_info';
  TRUNCATE TABLE silver.crm_cust_info;
  RAISE NOTICE 'Inserting data into: silver.crm_cust_info';

  INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
  )
  SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) cst_firstname,
    TRIM(cst_lastname) cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a'
    END cst_marital_status, -- Normalize marital status values to readable format
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a'
    END cst_gndr, -- Normalize gender values to readable format
    cst_create_date
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL  
  ) t WHERE flag_last = 1; -- Select the most recent record per customer

  -- for time
  end_time := clock_timestamp();
  RAISE NOTICE 'End time: %', end_time;
  RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

  -- check count
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RAISE NOTICE 'Inserted rows: %', v_rows;

  -- Modify from bronze layer and insert into silver layer crm_prd_info

  -- for time
  RAISE NOTICE '=== Processing silver.crm_prd_info ===';
  start_time := clock_timestamp();
  RAISE NOTICE 'Start time: %', start_time;

  RAISE NOTICE 'Truncating table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE 'Inserting data into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
    )
    SELECT 
      prd_id,
      REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id, -- Extract Category ID
      SUBSTRING(prd_key, 7, LENGTH(prd_key)) prd_key, -- Extract Product Key
      prd_nm,
      COALESCE(prd_cost, 0) prd_cost, -- Handle nulls converted to 0
      CASE UPPER(TRIM(prd_line))
           WHEN 'M' THEN 'Mountain'
           WHEN 'R' THEN 'Road'
           WHEN 'S' THEN 'Other Sales'
           WHEN 'T' THEN 'Touring'
           ELSE 'n/a'
      END prd_line, -- Map product line codes to descriptive values
      prd_start_dt::DATE,
      (LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC) - INTERVAL '1 day')::DATE prd_end_dt -- Calculate end date as one day before the next start date
    FROM bronze.crm_prd_info;

    -- for time
    end_time := clock_timestamp();
    RAISE NOTICE 'End time: %', end_time;
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- check count
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'Inserted rows: %', v_rows;

    -- Modify from bronze layer and insert into silver layer crm_sales_details

    -- for time
    RAISE NOTICE '=== Processing silver.crm_sales_details ===';
    start_time := clock_timestamp();
    RAISE NOTICE 'Start time: %', start_time;

    RAISE NOTICE 'Truncating table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE 'Inserting data into: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
    )
    SELECT
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
           ELSE (sls_order_dt::varchar)::date
      END sls_order_dt,
      CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
           ELSE (sls_ship_dt::varchar)::date
      END sls_ship_dt,
      CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
           ELSE (sls_due_dt::varchar)::date
      END sls_due_dt,
      CASE WHEN sls_sales IS NULL 
        OR sls_sales <= 0 
        OR (sls_sales IS DISTINCT FROM sls_quantity * ABS(sls_price) AND sls_price IS NOT NULL) 
             THEN sls_quantity * ABS(sls_price)
           ELSE sls_sales
      END sls_sales, -- Recalculate sales if original value is missing or incorrect

      sls_quantity,
      
      CASE WHEN sls_price IS NULL OR sls_price <= 0 
             THEN sls_sales / NULLIF(sls_quantity, 0)
           ELSE sls_price
      END sls_price -- Derive price if original value is invalid
    FROM bronze.crm_sales_details;

    -- for time
    end_time := clock_timestamp();
    RAISE NOTICE 'End time: %', end_time;
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- check count
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'Inserted rows: %', v_rows;

    -- Modify from bronze layer and insert into silver layer erp_cust_az12

    -- for time
    RAISE NOTICE '=== Processing silver.erp_cust_az12 ===';
    start_time := clock_timestamp();
    RAISE NOTICE 'Start time: %', start_time;

    RAISE NOTICE 'Truncating table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE 'Inserting data into: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (
      cid,
      bdate,
      gen
    )
    SELECT
      CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
           ELSE cid
      END cid,
      CASE WHEN bdate > NOW() THEN NULL
           ELSE bdate
      END bdate, -- Set future birthdates to NULL
      CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
           WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
           ELSE 'n/a'
      END gen -- Normalize gender values and handle unknown cases
    FROM bronze.erp_cust_az12;

    -- for time
    end_time := clock_timestamp();
    RAISE NOTICE 'End time: %', end_time;
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- check count
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'Inserted rows: %', v_rows;

    -- Modify from bronze layer and insert into silver layer erp_loc_a101

    -- for time
    RAISE NOTICE '=== Processing silver.erp_loc_a101 ===';
    start_time := clock_timestamp();
    RAISE NOTICE 'Start time: %', start_time;

    RAISE NOTICE 'Truncating table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE 'Inserting data into: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101 (
      cid,
      cntry
    )
    SELECT 
      REPLACE(cid, '-', '') cid,
      CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
           WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
           WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
           ELSE TRIM(cntrY)
      END cntry -- Normalize and handle missing or blank country codes
    FROM bronze.erp_loc_a101;

    -- for time
    end_time := clock_timestamp();
    RAISE NOTICE 'End time: %', end_time;
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- check count
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'Inserted rows: %', v_rows;

    -- Modify from bronze layer and insert into silver layer erp_px_cat_g1v2

    -- for time
    RAISE NOTICE '=== Processing silver.erp_px_cat_g1v2 ===';
    start_time := clock_timestamp();
    RAISE NOTICE 'Start time: %', start_time;

    RAISE NOTICE 'Truncating table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Inserting data into: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2(
      id,
      cat,
      subcat,
      maintenance
    )
    SELECT
      id,
      cat,
      subcat,
      maintenance 
    FROM bronze.erp_px_cat_g1v2;

    -- for time
    end_time := clock_timestamp();
    RAISE NOTICE 'End time: %', end_time;
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- check count
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'Inserted rows: %', v_rows;

  EXCEPTION
  WHEN unique_violation THEN
    RAISE WARNING 'Duplicate key violation.';
  WHEN division_by_zero THEN
    RAISE WARNING 'Math error.';
  WHEN OTHERS THEN
    RAISE WARNING 'Unhandled error (%): %', SQLSTATE, SQLERRM;
  END;
END;
$$; 