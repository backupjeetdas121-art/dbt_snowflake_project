import snowflake.snowpark.functions as F


def model(dbt, session):
    dbt.config(materialized="table")

    customers = (
        dbt.ref("dim_customers")
        .filter(F.col("is_current") == True)
        .select(F.col("customer_sk"), F.col("customer_id"), F.col("city"))
    )

    fact = (
        dbt.ref("fact_orders")
        .filter(~F.col("order_status").isin("cancelled", "returned"))
    )

    orders_by_customer = (
        fact.group_by("customer_sk")
        .agg(
            F.count_distinct("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
        )
    )

    customer_activity = (
        customers.join(orders_by_customer, on="customer_sk", how="left")
        .select(
            F.col("city"),
            F.col("customer_id"),
            F.coalesce(F.col("total_orders"), F.lit(0)).alias("total_orders"),
            F.coalesce(F.col("total_revenue"), F.lit(0)).alias("total_revenue"),
        )
    )

    aggregated = (
        customer_activity.group_by("city")
        .agg(
            F.count_distinct("customer_id").alias("total_customers"),
            F.sum("total_orders").alias("total_orders"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum(
                F.when(F.col("total_orders") > 0, F.lit(1)).otherwise(F.lit(0))
            ).alias("customers_with_orders"),
        )
    )

    return aggregated.select(
        F.col("city"),
        F.col("total_customers"),
        F.col("total_orders"),
        F.col("total_revenue"),
        (F.col("total_revenue") / F.col("total_customers")).alias(
            "revenue_per_customer"
        ),
        (
            F.col("customers_with_orders") / F.col("total_customers")
        ).alias("conversion_rate"),
    )
