from pyspark.sql.functions import *
from config import (storage_account_name, storage_account_key, source_path, bronze_path, silver_path, gold_daily_path,
                    gold_location_path, gold_payment_path)

def read_source(spark):
    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",storage_account_key)
    raw_df = spark.read.csv(source_path,header=True,inferSchema=True)
    return raw_df

def build_bronze(spark, raw_df):
    bronze_df = raw_df
    for col_name in bronze_df.columns:
        bronze_df = bronze_df.withColumnRenamed(col_name, col_name.replace(" ", "_"))
    bronze_df = bronze_df.withColumn("ingestion_timestamp",current_timestamp())
    bronze_df = bronze_df.coalesce(2)
    bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path)
    bronze_df = spark.read.format("delta").load(bronze_path)
    return bronze_df


def build_silver(spark, bronze_df):
    silver_df = bronze_df.withColumn("booking_value", col("Booking_Value").cast("double")) \
                         .withColumn("ride_distance", col("Ride_Distance").cast("double")) \
                         .withColumn("driver_rating", col("Driver_Ratings").cast("double")) \
                         .withColumn("customer_rating", col("Customer_Rating").cast("double"))

    # Remove duplicate records
    silver_df = silver_df.dropDuplicates()

    # Check null counts before applying data quality filters
    silver_df.select([count(when(col(c).isNull(), c)).alias(c) 
                      for c in silver_df.columns]).show()

    # Keep valid ride records (completed bookings with valid values)
    silver_df = silver_df.filter((col("Booking_Status") == "Completed")
        & (col("booking_value").isNotNull())
        & (col("booking_value") > 0)
        & (col("ride_distance").isNotNull())
        & (col("ride_distance") > 0)
    )

    silver_df = silver_df.withColumn("pickup_date", col("Date")).withColumn("pickup_hour", hour("Time"))
    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    silver_df = spark.read.format("delta").load(silver_path)
    return silver_df


def build_gold(spark, silver_df):
    daily_metrics = [count("*").alias("total_trips"),
                     sum("booking_value").alias("total_revenue"),
                     avg("booking_value").alias("average_booking_value"),
                     avg("ride_distance").alias("average_ride_distance"),
                     avg("driver_rating").alias("average_driver_rating")]

    # Daily summary
    daily_summary_df = silver_df.groupBy("pickup_date").agg(*daily_metrics)
    daily_summary_df.show()

    daily_summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_daily_path)
    # Pickup location summary
    location_summary_df = silver_df.groupBy("Pickup_Location").agg(*daily_metrics)
    location_summary_df.show()
    location_summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_location_path)

    # Payment method summary
    payment_summary_df = silver_df.groupBy("Payment_Method").agg(*daily_metrics)
    payment_summary_df = payment_summary_df.orderBy("Payment_Method")
    payment_summary_df.show()
    payment_summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_payment_path)

    return {
        "daily_summary": daily_summary_df,
        "location_summary": location_summary_df,
        "payment_summary": payment_summary_df
    }


def run_etl(spark):
    raw_df = read_source(spark)
    bronze_df = build_bronze(spark, raw_df)
    silver_df = build_silver(spark, bronze_df)
    gold_dfs = build_gold(spark, silver_df)
    return {
        "status": "success",
        "bronze_rows": bronze_df.count(),
        "silver_rows": silver_df.count(),
        "daily_summary_rows": gold_dfs["daily_summary"].count(),
        "location_summary_rows": gold_dfs["location_summary"].count(),
        "payment_summary_rows": gold_dfs["payment_summary"].count()
    }