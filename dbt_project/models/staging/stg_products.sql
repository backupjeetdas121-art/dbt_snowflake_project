with source as (

    select * from {{ source('stg', 'stg_products') }}

),

renamed as (

    select
        product_id,
        trim(product_name)                  as product_name,
        coalesce(category, 'unknown')       as category,
        coalesce(sub_category, 'unknown')   as sub_category,
        coalesce(brand, 'unknown')          as brand,
        price,
        cost,
        created_at,
        updated_at
    from source
    where product_id is not null

)

select * from renamed
