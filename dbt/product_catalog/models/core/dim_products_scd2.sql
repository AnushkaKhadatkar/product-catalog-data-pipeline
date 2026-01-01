{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        schema='core',
        on_schema_change='append_new_columns'
    )
}}

/*
    Product Dimension with SCD Type 2
    
    This model implements Slowly Changing Dimension Type 2 to track
    historical changes in product attributes over time.
    
    Key fields:
    - surrogate_key: Unique identifier for each version
    - product_id: Natural business key
    - valid_from: Start date of this version
    - valid_to: End date of this version (NULL for current)
    - is_current: Boolean flag for active record
*/

WITH source_data AS (
    SELECT * FROM {{ ref('stg_products') }}
),

{% if is_incremental() %}

-- ============================================================================
-- INCREMENTAL LOGIC: Handle updates and new records
-- ============================================================================

existing_records AS (
    -- Get all currently active records from the dimension table
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

changed_records AS (
    -- Identify records that are new or have changed
    SELECT 
        s.*,
        e.surrogate_key AS existing_surrogate_key,
        e.valid_from AS existing_valid_from,
        CASE 
            WHEN e.product_id IS NULL THEN 'INSERT'  -- New product
            WHEN s.title != e.title OR 
                 s.price != e.price OR 
                 s.description != e.description OR 
                 s.category != e.category THEN 'UPDATE'  -- Changed attributes
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM source_data s
    LEFT JOIN existing_records e
        ON s.product_id = e.product_id
),

records_to_close AS (
    -- Close out the old versions of changed records
    SELECT 
        e.surrogate_key,
        e.product_id,
        e.title,
        e.category,
        e.brand,
        e.price,
        e.description,
        e.url,
        e.availability,
        e.currency,
        e.created_at,
        e.updated_at,
        e.valid_from,
        c.updated_at AS valid_to,  -- Set end date to new record's timestamp
        FALSE AS is_current,  -- Mark as historical
        e.uniq_id
    FROM existing_records e
    INNER JOIN changed_records c
        ON e.product_id = c.product_id
    WHERE c.change_type = 'UPDATE'
),

new_versions AS (
    -- Create new versions for changed and new records
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id', 'updated_at']) }} AS surrogate_key,
        product_id,
        title,
        category,
        brand,
        price,
        description,
        url,
        availability,
        currency,
        created_at,
        updated_at,
        updated_at AS valid_from,  -- New version starts now
        NULL AS valid_to,  -- Open-ended (current version)
        TRUE AS is_current,  -- Mark as current
        uniq_id
    FROM changed_records
    WHERE change_type IN ('INSERT', 'UPDATE')
),

unchanged_records AS (
    -- Keep records that haven't changed
    SELECT 
        e.surrogate_key,
        e.product_id,
        e.title,
        e.category,
        e.brand,
        e.price,
        e.description,
        e.url,
        e.availability,
        e.currency,
        e.created_at,
        e.updated_at,
        e.valid_from,
        e.valid_to,
        e.is_current,
        e.uniq_id
    FROM existing_records e
    LEFT JOIN changed_records c
        ON e.product_id = c.product_id
    WHERE c.change_type = 'NO_CHANGE' OR c.change_type IS NULL
),

final_incremental AS (
    -- Combine all record types
    SELECT * FROM records_to_close
    UNION ALL
    SELECT * FROM new_versions
    UNION ALL
    SELECT * FROM unchanged_records
)

SELECT * FROM final_incremental

{% else %}

-- ============================================================================
-- INITIAL LOAD: First time running the model
-- ============================================================================

initial_load AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id', 'created_at']) }} AS surrogate_key,
        product_id,
        title,
        category,
        brand,
        price,
        description,
        url,
        availability,
        currency,
        created_at,
        updated_at,
        created_at AS valid_from,  -- Record starts from creation
        NULL AS valid_to,  -- Open-ended
        TRUE AS is_current,  -- All records are current on initial load
        uniq_id
    FROM source_data
)

SELECT * FROM initial_load

{% endif %}