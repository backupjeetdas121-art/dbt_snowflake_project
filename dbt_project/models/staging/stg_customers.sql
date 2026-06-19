with source as (

    select * from {{ source('stg', 'stg_customers') }}

),

renamed as (

    select
        customer_id,
        trim(first_name)                          as first_name,
        trim(last_name)                           as last_name,
        lower(trim(email))                        as email,
        phone,
        address,
        city,
        state,
        country,
        coalesce(acquisition_channel, 'unknown')  as acquisition_channel,
        signup_date,
        created_at,
        updated_at
    from source
    where customer_id is not null

)

select * from renamed
