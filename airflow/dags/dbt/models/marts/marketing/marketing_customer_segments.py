import snowflake.snowpark.functions as F
from snowflake.snowpark import Window


def model(dbt, session):
    dbt.config(materialized="table")

    fact = (
        dbt.ref("fact_orders")
        .filter(~F.col("order_status").isin("cancelled", "returned"))
    )

    customers = (
        dbt.ref("dim_customers")
        .filter(F.col("is_current") == True)
        .select(F.col("customer_sk"), F.col("customer_id"), F.col("email"))
    )

    customer_orders = (
        fact.group_by("customer_sk")
        .agg(
            F.max("order_date").alias("last_order_date"),
            F.count_distinct("order_id").alias("frequency"),
            F.sum("total_amount").alias("monetary"),
        )
    )

    scored = (
        customer_orders.select(
            F.col("customer_sk"),
            F.col("last_order_date"),
            F.col("frequency"),
            F.col("monetary"),
            F.datediff("day", F.col("last_order_date"), F.current_timestamp()).alias(
                "recency_days"
            ),
        )
        .select(
            F.col("customer_sk"),
            F.col("last_order_date"),
            F.col("frequency"),
            F.col("monetary"),
            F.col("recency_days"),
            F.ntile(5).over(Window.order_by(F.col("recency_days").desc())).alias(
                "recency_score"
            ),
            F.ntile(5).over(Window.order_by(F.col("frequency").asc())).alias(
                "frequency_score"
            ),
            F.ntile(5).over(Window.order_by(F.col("monetary").asc())).alias(
                "monetary_score"
            ),
        )
    )

    return (
        scored.join(customers, on="customer_sk", how="inner")
        .select(
            F.col("customer_id"),
            F.col("email"),
            F.col("last_order_date"),
            F.col("recency_days"),
            F.col("frequency"),
            F.col("monetary"),
            F.col("recency_score"),
            F.col("frequency_score"),
            F.col("monetary_score"),
            (
                F.col("recency_score")
                + F.col("frequency_score")
                + F.col("monetary_score")
            ).alias("rfm_total_score"),
        )
        .select(
            F.col("customer_id"),
            F.col("email"),
            F.col("last_order_date"),
            F.col("recency_days"),
            F.col("frequency"),
            F.col("monetary"),
            F.col("recency_score"),
            F.col("frequency_score"),
            F.col("monetary_score"),
            F.col("rfm_total_score"),
            F.when(
                (F.col("recency_score") >= 4)
                & (F.col("frequency_score") >= 4)
                & (F.col("monetary_score") >= 4),
                F.lit("Champion"),
            )
            .when(
                (F.col("recency_score") >= 3) & (F.col("frequency_score") >= 3),
                F.lit("Loyal"),
            )
            .when(
                (F.col("recency_score") <= 2) & (F.col("frequency_score") <= 2),
                F.lit("At Risk"),
            )
            .otherwise(F.lit("Needs Attention"))
            .alias("customer_segment"),
        )
    )
