-- ---------------------------------------------------------------------
-- fact_orders
-- Incremental fact table, merged on order_id (so status updates to an
-- existing order overwrite the row rather than duplicating it).
-- Joins to the CURRENT surrogate key of each dimension at load time.
-- ---------------------------------------------------------------------

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
    {% endif %}

),

dim_cust as (
    select customer_id, customer_sk
    from {{ ref('dim_customers') }}
    where is_current = true
),

dim_prod as (
    select product_id, product_sk
    from {{ ref('dim_products') }}
    where is_current = true
)

select
    o.order_id,
    dc.customer_sk,
    dp.product_sk,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.discount,
    o.total_amount,
    o.order_status,
    o.payment_method,
    o.created_at,
    o.updated_at
from orders o
left join dim_cust dc on o.customer_id = dc.customer_id
left join dim_prod dp on o.product_id = dp.product_id
