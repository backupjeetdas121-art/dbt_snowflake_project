-- ---------------------------------------------------------------------
-- int_products_current
-- Same pattern as int_customers_current: maintains a full current-state
-- table of products via incremental MERGE, used as the source for the
-- snap_products SCD2 snapshot.
-- ---------------------------------------------------------------------

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='merge'
    )
}}

select
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price,
    cost,
    created_at,
    updated_at
from {{ ref('stg_products') }}

{% if is_incremental() %}
where updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
   or product_id not in (select product_id from {{ this }})
{% endif %}
