from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(
    name="gold.d_restaurant_reviews",
    table_properties={"quality": "gold"}
)
def d_restaurant_reviews():
    df_restaurants = dp.read("silver.dim_restaurants")
    df_reviews = dp.read("silver.fact_reviews")

    df_review_stats = (
        df_reviews
        .groupBy("restaurant_id")
        .agg(
            # Total reviews
            count("review_id").alias("total_reviews"),
            
            # Average rating
            round(avg("rating"), 2).alias("avg_rating"),
            
            # Rating distribution
            sum(when(col("rating") == 5, 1).otherwise(0)).alias("rating_5_count"),
            sum(when(col("rating") == 4, 1).otherwise(0)).alias("rating_4_count"),
            sum(when(col("rating") == 3, 1).otherwise(0)).alias("rating_3_count"),
            sum(when(col("rating") == 2, 1).otherwise(0)).alias("rating_2_count"),
            sum(when(col("rating") == 1, 1).otherwise(0)).alias("rating_1_count"),
            
            # Sentiment counts
            sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("sentiment_positive_count"),
            sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("sentiment_neutral_count"),
            sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("sentiment_negative_count"),
        )
    )

    df_restaurant_reviews = (
        df_restaurants
        .join(df_review_stats, "restaurant_id", "left")
        .select(
            col("restaurant_id"),
            col("name").alias("restaurant_name"),
            df_restaurants.city,
            
            # Review Stats
            coalesce(col("total_reviews"), lit(0)).cast("bigint").alias("total_reviews"),
            coalesce(col("avg_rating"), lit(0)).cast("decimal(3,2)").alias("avg_rating"),
            coalesce(col("rating_5_count"), lit(0)).cast("bigint").alias("rating_5_count"),
            coalesce(col("rating_4_count"), lit(0)).cast("bigint").alias("rating_4_count"),
            coalesce(col("rating_3_count"), lit(0)).cast("bigint").alias("rating_3_count"),
            coalesce(col("rating_2_count"), lit(0)).cast("bigint").alias("rating_2_count"),
            coalesce(col("rating_1_count"), lit(0)).cast("bigint").alias("rating_1_count"),
            
            # Sentiment Stats
            coalesce(col("sentiment_positive_count"), lit(0)).cast("bigint").alias("sentiment_positive_count"),
            coalesce(col("sentiment_neutral_count"), lit(0)).cast("bigint").alias("sentiment_neutral_count"),
            coalesce(col("sentiment_negative_count"), lit(0)).cast("bigint").alias("sentiment_negative_count")
        )
    )
    return df_restaurant_reviews