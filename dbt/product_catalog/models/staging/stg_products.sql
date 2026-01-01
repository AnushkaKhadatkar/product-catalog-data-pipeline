{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging model: Clean and prepare raw product data
-- This model standardizes data types, handles nulls, and applies business rules

WITH source_data AS (
    SELECT * FROM {{ source('staging', 'raw_products') }}
),

cleaned_data AS (
    SELECT
        -- Primary identifiers
        product_id,
        uniq_id,
        sku,
        gtin13,
        
        -- Product information
        TRIM(title) AS title,
        TRIM(COALESCE(NULLIF(brand, ''), 'Unbranded')) AS brand,
        CASE 
            WHEN brand IS NULL OR brand = '' THEN 'Unbranded'
            ELSE TRIM(brand)
        END AS category,
        TRIM(description) AS description,
        TRIM(url) AS url,
        
        -- Pricing and availability
        CAST(price AS DECIMAL(10,2)) AS price,
        UPPER(TRIM(currency)) AS currency,
        TRIM(availability) AS availability,
        
        -- Timestamps
        created_at,
        updated_at,
        loaded_at
        
    FROM source_data
    WHERE product_id IS NOT NULL
        AND price IS NOT NULL
        AND price > 0
        AND title IS NOT NULL
)

SELECT * FROM cleaned_data