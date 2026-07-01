import snowflake.snowpark.functions as F


def model(dbt, session):
    dbt.config(materialized="table")

    fact = dbt.ref("fact_orders").filter(~F.col("order_status").isin("cancelled"))

    products = (
        dbt.ref("dim_products")
        .filter(F.col("is_current") == True)
        .select(F.col("product_sk"), F.col("category"), F.col("sub_category"), F.col("brand"))
    )

    customers = (
        dbt.ref("dim_customers")
        .filter(F.col("is_current") == True)
        .select(F.col("customer_sk"), F.col("country"))
    )

    joined = (
        fact.join(products, on="product_sk", how="left")
        .join(customers, on="customer_sk", how="left")
        .select(
            F.to_date(F.col("order_date")).alias("order_date"),
            F.col("category"),
            F.col("sub_category"),
            F.col("brand"),
            F.col("country"),
            F.col("quantity"),
            F.col("total_amount"),
            F.col("discount"),
            F.col("order_id"),
            F.col("customer_id"),
        )
    )

    return (
        joined.group_by("order_date", "category", "sub_category", "brand", "country")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("total_amount").alias("total_revenue"),
            (F.sum(F.col("discount") * F.col("quantity"))).alias("total_discount"),
            F.count_distinct("order_id").alias("total_orders"),
            F.count_distinct("customer_id").alias("unique_customers"),
        )
    )
