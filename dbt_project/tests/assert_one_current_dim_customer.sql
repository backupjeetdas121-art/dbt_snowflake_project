-- ---------------------------------------------------------------------
-- assert_one_current_dim_customer
-- A dbt test FAILS if this query returns any rows.
--
-- SCD2 integrity check: every customer_id must have at most one row
-- where is_current = true. More than one indicates a problem in the
-- snapshot/dimension build (e.g. duplicate "open" records).
-- ---------------------------------------------------------------------

select
    customer_id,
    count(*) as current_record_count
from {{ ref('dim_customers') }}
where is_current = true
group by customer_id
having count(*) > 1
