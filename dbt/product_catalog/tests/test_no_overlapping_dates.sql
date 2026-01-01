-- Test: Ensure no overlapping date ranges for the same product
-- Each product should have non-overlapping valid_from/valid_to ranges

WITH date_ranges AS (
    SELECT 
        product_id,
        valid_from,
        COALESCE(valid_to, '9999-12-31'::TIMESTAMP_NTZ) AS valid_to
    FROM {{ ref('dim_products_scd2') }}
)

SELECT 
    a.product_id,
    a.valid_from AS range1_start,
    a.valid_to AS range1_end,
    b.valid_from AS range2_start,
    b.valid_to AS range2_end
FROM date_ranges a
JOIN date_ranges b
    ON a.product_id = b.product_id
    AND a.valid_from < b.valid_from
WHERE a.valid_to > b.valid_from