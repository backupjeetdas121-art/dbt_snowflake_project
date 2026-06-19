-- ---------------------------------------------------------------------
-- int_customers_current
--
-- stg_customers only ever contains the CURRENT incremental batch
-- (STG is truncate+load every run). dbt snapshot, however, needs to
-- compare a FULL current-state view against history to detect changes.
--
-- This model is materialized incrementally with a MERGE strategy:
--   - First run: loads the full current batch as the baseline.
--   - Every run after: upserts only the rows present in this run's
--     batch (new customers + customers whose attributes changed),
--     while customers untouched this run remain unchanged in `this`.
--
-- snap_customers.sql snapshots THIS model, giving an always-complete
-- "current state" source for SCD2 history capture.
-- ---------------------------------------------------------------------

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    country,
    acquisition_channel,
    signup_date,
    created_at,
    updated_at
from {{ ref('stg_customers') }}

{% if is_incremental() %}
-- stg_customers already represents "rows changed since last extract",
-- so on incremental runs we simply merge whatever is in this batch.
where updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
   or customer_id not in (select customer_id from {{ this }})
{% endif %}
