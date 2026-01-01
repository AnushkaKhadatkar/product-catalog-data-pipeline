-- Test: Ensure each product has exactly one current record
-- This test validates SCD Type 2 implementation

SELECT 
    product_id,
    COUNT(*) as current_count
FROM {{ ref('dim_products_scd2') }}
WHERE is_current = TRUE
GROUP BY product_id
HAVING COUNT(*) > 1