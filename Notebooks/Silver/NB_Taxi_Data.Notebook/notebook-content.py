# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6d86f134-80d9-4e93-a56b-3b28a553dee0",
# META       "default_lakehouse_name": "LH_Taxi_Analytics",
# META       "default_lakehouse_workspace_id": "c5ea75aa-21b9-4dcd-b756-ecc884f2ebcb",
# META       "known_lakehouses": [
# META         {
# META           "id": "6d86f134-80d9-4e93-a56b-3b28a553dee0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, count, when, isnan, min, max, avg, expr, broadcast, to_date, lit
from functools import reduce

files = []

for file in mssparkutils.fs.ls("Files/bronze/taxi_data"):
    if file.name.endswith(".parquet"):
        files.append(file.path)

dfs = []

for file in files:
    file_name = file.split("/")[-1]
    parts = file_name.replace(".parquet", "").split("_")[-1].split("-")
    source_year = int(parts[0])
    source_month = int(parts[1])

    df_temp = spark.read.parquet(file)

    df_temp = (df_temp
        .withColumn("passenger_count", col("passenger_count").cast("integer"))
        .withColumn("PULocationID", col("PULocationID").cast("integer"))
        .withColumn("DOLocationID", col("DOLocationID").cast("integer"))
        .withColumn("source_year", lit(source_year))
        .withColumn("source_month", lit(source_month)))

    dfs.append(df_temp)

df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
df = df.withColumnsRenamed({
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "total_amount": "amount"
})

df = df.dropDuplicates()

display(df.limit(5))
df.printSchema()

display(f"Total rows: {df.count()}")

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.taxi_trips_deduplicated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, when, isnan, min, max, avg, expr, broadcast, to_date, year, month

df = spark.read.table("silver.taxi_trips_deduplicated")

df = df.filter((col("payment_type") != 6) & (col("trip_distance") >= 0))

df = df.filter(
    (year("pickup_datetime") == col("source_year")) &
    (month("pickup_datetime") == col("source_month")))

remaining_columns = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "amount"
]

df_cleaned = df.select(*remaining_columns)
display(df_cleaned.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_counts = df_cleaned.select([
    count( when(col(c).isNull() | isnan(col(c)), 1) ).alias(c)
    if c in ["passenger_count", "trip_distance", "pickup_location_id", "dropoff_location_id", "amount"]
    else count(when(col(c).isNull(), 1)).alias(c)
    for c in df_cleaned.columns
])

display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def column_statistics(df, column):
    return df.agg(
        count(column).alias("count"),
        min(column).alias("min"),
        expr(f"percentile_approx(`{column}`, 0.001)").alias("p0.1"),
        expr(f"percentile_approx(`{column}`, 0.01)").alias("p1"),
        expr(f"percentile_approx(`{column}`, 0.25)").alias("p25"),
        expr(f"percentile_approx(`{column}`, 0.50)").alias("median"),
        expr(f"percentile_approx(`{column}`, 0.75)").alias("p75"),
        expr(f"percentile_approx(`{column}`, 0.95)").alias("p95"),
        expr(f"percentile_approx(`{column}`, 0.99)").alias("p99"),
        expr(f"percentile_approx(`{column}`, 0.999)").alias("p99.9"),
        max(column).alias("max")
    )

column_statistics(df_cleaned, "amount").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned = df_cleaned.filter( (col("amount") > 0) & (col("amount") <= 200))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zones = broadcast( spark.read.table("silver.taxi_zones").select("location_id"))

df_cleaned.join(zones, df_cleaned.pickup_location_id == zones.location_id,
    "left_anti").agg( count("*").alias("invalid_pickup_ids")).show()

df_cleaned.join(zones, df_cleaned.dropoff_location_id == zones.location_id,
    "left_anti").agg( count("*").alias("invalid_dropoff_ids")).show()

df_cleaned = (df_cleaned.join(zones, df_cleaned.pickup_location_id == zones.location_id, "left_semi")
            .join(zones, df_cleaned.dropoff_location_id == zones.location_id, "left_semi"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned = df_cleaned.withColumn(
    "passenger_count",
    when( (col("passenger_count") < 1) | (col("passenger_count") > 6), None)
    .otherwise(col("passenger_count").cast("integer")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import round as spark_round

df_cleaned = df_cleaned.filter( unix_timestamp("dropoff_datetime") > unix_timestamp("pickup_datetime"))

df_cleaned = df_cleaned.withColumn(
    "trip_duration_minutes",
    spark_round((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60, 2).cast("float")
)

df_cleaned = df_cleaned.withColumn(
    "average_speed_mph",
    spark_round(col("trip_distance") * 60.0 / col("trip_duration_minutes"), 2).cast("float")
)

display(df_cleaned.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned = df_cleaned.filter(
    (col("trip_duration_minutes") >= 2) &
    (col("trip_duration_minutes") <= 60) &
    (col("trip_distance") >= 0) &
    (col("trip_distance") <= 30) &
    ((col("trip_distance") == 0) |
        (
            (col("average_speed_mph") > 0) &
            (col("average_speed_mph") <= 40)
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("silver.taxi_trips")

column_statistics(df, "trip_distance").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_statistics(df, "trip_duration_minutes").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.agg(
    min("pickup_datetime").alias("min_date"),
    max("pickup_datetime").alias("max_date")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
