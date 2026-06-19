{% snapshot snap_products %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True,
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
    updated_at
from {{ ref('int_products_current') }}

{% endsnapshot %}
