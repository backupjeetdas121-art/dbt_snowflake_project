-- ---------------------------------------------------------------------
-- assert_no_negative_order_amounts
-- A dbt test FAILS if this query returns any rows.
--
-- Data quality check: order quantity and total_amount must never be
-- negative. Catches upstream source data issues that the staging
-- WHERE clause might miss after future schema changes.
-- ---------------------------------------------------------------------

select *
from {{ ref('fact_orders') }}
where total_amount < 0
   or quantity < 0
