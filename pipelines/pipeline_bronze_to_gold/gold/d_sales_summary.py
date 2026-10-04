from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name="gold.d_sales_summary",
    partition_cols=["order_date"],
    table_properties={"quality": "gold"},
    comment="Gold layer aggregates with date-based overwrites",
)
def d_sales_summary():
    df_daily_agg = (
        dp.read("silver.fact_orders")
        .groupBy(col("order_date"))
        .agg(
            countDistinct(col("order_id")).alias("total_orders"),
            sum(col("total_amount")).cast("decimal(10,2)").alias("total_revenue"),
            avg(col("total_amount")).cast("decimal(10,2)").alias("avg_order_value"),
            countDistinct(col("customer_id")).alias("unique_customers"),
            countDistinct(col("restaurant_id")).alias("unique_restaurants"),
            sum(when(col("order_type") == "dine_in", 1).otherwise(0)).alias(
                "dine_in_orders"
            ),
            sum(when(col("order_type") == "takeaway", 1).otherwise(0)).alias(
                "takeaway_orders"
            ),
            sum(when(col("order_type") == "delivery", 1).otherwise(0)).alias(
                "delivery_orders"
            ),
        )
        .select(
            "order_date",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "unique_customers",
            "unique_restaurants",
            "dine_in_orders",
            "takeaway_orders",
            "delivery_orders",
        )
    )
    return df_daily_agg