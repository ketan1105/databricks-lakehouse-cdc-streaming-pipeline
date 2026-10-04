from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.materialized_view(
    name="gold.d_customer_360",
    table_properties={"quality": "gold"}
)
def d_customer_360():
    df_customers = dp.read("silver.dim_customers")
    df_restaurants = dp.read("silver.dim_restaurants")
    df_orders = dp.read("silver.fact_orders")
    df_order_items = dp.read("silver.fact_order_items")
    df_reviews = dp.read("silver.fact_reviews")

    df_order_stats = (
        df_orders
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_spend"),
            round(avg("total_amount"), 2).alias("avg_order_value"),
            max("order_date").alias("last_order_date"),
        )
    )

    df_review_stats = (
        df_reviews
        .groupBy("customer_id")
        .agg(
            countDistinct("review_id").alias("total_reviews"),
            round(avg("rating"), 2).alias("avg_rating_given")
        )
    )

    win_fav_rest = Window.partitionBy("customer_id").orderBy(col("order_ct").desc())

    df_fav_restaurant = (
        df_orders
        .groupBy("customer_id", "restaurant_id")
        .agg(countDistinct("order_id").alias("order_ct"))
        .withColumn("rn", row_number().over(win_fav_rest))
        .filter(col("rn") == 1)
        .join(df_restaurants, "restaurant_id", "left")
        .select(col("customer_id"), col("name").alias("favorite_restaurant"))
    )

    win_fav_item = Window.partitionBy("customer_id").orderBy(col("item_qty").desc())

    df_fav_item = (
        df_orders.join(df_order_items, "order_id", "inner")
        .groupBy(df_orders.customer_id, df_order_items.item_name)
        .agg(sum("quantity").alias("item_qty"))
        .withColumn("rn", row_number().over(win_fav_item))
        .filter(col("rn") == 1)
        .select("customer_id", col("item_name").alias("favorite_item"))
    )

    df_c360 = (
        df_customers
        .join(df_order_stats, "customer_id", "left")
        .join(df_review_stats, "customer_id", "left")
        .join(df_fav_restaurant, "customer_id", "left")
        .join(df_fav_item, "customer_id", "left")    
        .select(
            col("customer_id"),
            col("name").alias("customer_name"),
            col("email"),
            df_customers.city,
            to_date(col("join_date")).alias("join_date"),

            # Loyalty Tier
            when(col("lifetime_spend") >= 5000, "Platinum")
            .when(col("lifetime_spend") >= 2000, "Gold")
            .when(col("lifetime_spend") >= 500, "Silver")
            .otherwise("Bronze").alias("loyalty_tier"),

            # Order Stats
            coalesce(col("total_orders"), lit(0)).cast("bigint").alias("total_orders"),
            coalesce(col("lifetime_spend"), lit(0)).cast("decimal(10,2)").alias("lifetime_spend"),
            coalesce(col("avg_order_value"), lit(0)).cast("decimal(10,2)").alias("avg_order_value"),
            col("last_order_date"),
            
            # Preferences
            col("favorite_restaurant"),
            col("favorite_item"),
            
            # Review Stats
            coalesce(col("avg_rating_given"), lit(0)).cast("decimal(3,2)").alias("avg_rating_given"),
            coalesce(col("total_reviews"), lit(0)).cast("bigint").alias("total_reviews"),
            
            when(
                col("lifetime_spend") >= 5000, 
                True
            ).otherwise(False).alias("is_vip")
        )
    )
    return df_c360