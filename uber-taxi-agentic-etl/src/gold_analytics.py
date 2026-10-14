from pyspark.sql.functions import *
from config import (gold_daily_path,gold_location_path,gold_payment_path)

def get_gold_metrics(spark):
    daily_df = spark.read.format("delta").load(gold_daily_path)
    location_df = spark.read.format("delta").load(gold_location_path)
    payment_df = spark.read.format("delta").load(gold_payment_path)

    overall = daily_df.agg(sum("total_trips").alias("total_trips"),
                           sum("total_revenue").alias("total_revenue")).first().asDict()

    if overall["total_trips"]:
        overall["overall_average_trip_amount"] = (overall["total_revenue"] / overall["total_trips"])
    else:
        overall["overall_average_trip_amount"] = None

    top_locations = [row.asDict()
                     for row in location_df.orderBy(desc("total_trips")).limit(10).collect()]

    payment_summary = [row.asDict() for row in payment_df.orderBy("payment_type").collect()]

    return {
        "overall": overall,
        "top_locations": top_locations,
        "payment_summary": payment_summary
    }