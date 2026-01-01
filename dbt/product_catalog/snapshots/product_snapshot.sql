{% snapshot product_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True,
    )
}}

-- Snapshot of product data to track historical changes
-- This captures point-in-time product information

SELECT 
    product_id,
    title,
    brand,
    category,
    price,
    currency,
    description,
    url,
    availability,
    updated_at,
    uniq_id
FROM {{ ref('stg_products') }}

{% endsnapshot %}