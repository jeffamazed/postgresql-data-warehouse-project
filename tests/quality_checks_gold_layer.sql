-- Quality Checks for Gold Layer

-- Check uniqueness of customer_key in dim_customers
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check uniqueness of product_key in dim_products
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Check referential integrity in fact_sales (missing dimension joins)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;
