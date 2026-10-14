from pyspark.sql.functions import *
from config import silver_path

def run_quality_checks(spark):
    df = spark.read.format("delta").load(silver_path)
    row_count = df.count()
    required_columns = ["pickup_datetime","dropoff_datetime","trip_distance","total_amount","passenger_count"]
    null_counts = df.select([count(when(col(c).isNull(), 1)).alias(c) for c in required_columns]).first().asDict()

    invalid_duration = df.filter(col("trip_duration_minutes").isNull() | (col("trip_duration_minutes") < 0)).count()
    invalid_distance = df.filter(col("trip_distance").isNull() | (col("trip_distance") <= 0)).count()
    invalid_amount = df.filter(col("total_amount").isNull() | (col("total_amount") <= 0)).count()
    passed = (
        row_count > 0
        and all(value == 0 for value in null_counts.values())
        and invalid_duration == 0
        and invalid_distance == 0
        and invalid_amount == 0
    )

    return {
        "status": "passed" if passed else "failed",
        "row_count": row_count,
        "null_counts": null_counts,
        "invalid_duration": invalid_duration,
        "invalid_distance": invalid_distance,
        "invalid_amount": invalid_amount
    }