import snowflake.snowpark.functions as F


def model(dbt, session):
    dbt.config(materialized="table")

    fact = (
        dbt.ref("fact_orders")
        .filter(~F.col("order_status").isin("cancelled", "returned"))
    )

    customers = (
        dbt.ref("dim_customers")
        .filter(F.col("is_current") == True)
        .select(
            F.col("customer_sk"),
            F.col("customer_id").alias("customer_id"),
            F.col("first_name").alias("first_name"),
            F.col("last_name").alias("last_name"),
            F.col("email").alias("email"),
            F.col("country").alias("country"),
        )
    )

    fact_for_ltv = fact.select(
        F.col("order_id"),
        F.col("order_date"),
        F.col("total_amount"),
        F.col("customer_sk"),
    )

    joined = (
        fact_for_ltv.join(
            customers,
            fact_for_ltv["customer_sk"] == customers["customer_sk"],
            how="inner",
        ).select(
            F.col("order_id"),
            F.col("order_date"),
            F.col("total_amount"),
            F.col("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("email"),
            F.col("country"),
        )
    )

    aggregated = (
        joined.group_by("customer_id", "first_name", "last_name", "email", "country")
        .agg(
            F.count_distinct("order_id").alias("total_orders"),
            F.sum("total_amount").alias("lifetime_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    return aggregated.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email"),
        F.col("country"),
        F.col("total_orders"),
        F.col("lifetime_value"),
        F.col("first_order_date"),
        F.col("last_order_date"),
        (F.col("lifetime_value") / (F.col("total_orders"))).alias(
            "avg_order_value"
        ),
    )
