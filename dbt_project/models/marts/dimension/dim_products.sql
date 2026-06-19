-- ---------------------------------------------------------------------
-- dim_products
-- Reads the SCD2 snapshot (full history) and exposes a clean dimension
-- with a surrogate key, validity window, and an `is_current` flag.
-- ---------------------------------------------------------------------

with snapshot as (

    select * from {{ ref('snap_products') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} as product_sk,
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price,
    cost,
    dbt_valid_from                                       as effective_from,
    coalesce(dbt_valid_to, '9999-12-31'::timestamp)      as effective_to,
    case when dbt_valid_to is null then true else false end as is_current
from snapshot
