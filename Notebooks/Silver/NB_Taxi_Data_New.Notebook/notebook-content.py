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

existing_months = set()

if spark.catalog.tableExists("silver.taxi_trips_deduplicated"):
    existing_df = spark.table("silver.taxi_trips_deduplicated")

    existing_months = { (row.source_year, row.source_month)
        for row in existing_df.select("source_year", "source_month").distinct().collect()}

new_files = []

for file in files:
    file_name = file.split("/")[-1]
    parts = file_name.replace(".parquet", "").split("_")[-1].split("-")
    source_year = int(parts[0])
    source_month = int(parts[1])

    if source_year >= 2011 and (source_year, source_month) not in existing_months:
        new_files.append(file)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

if not new_files:
    print("No new taxi data files. Skipping...")
    mssparkutils.notebook.exit("NO_NEW_DATA")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfs = []

for file in new_files:
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

df = (df.withColumn("RatecodeID", col("RatecodeID").cast("double"))
    .withColumn("cbd_congestion_fee", lit(None).cast("double"))
    .withColumn("request_source", lit(None).cast("string")))

df = df.withColumnsRenamed({
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "total_amount": "amount"
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.dropDuplicates()

df.write.format("delta").mode("append").saveAsTable("silver.taxi_trips_deduplicated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, year, month

df_deduplicated = spark.read.table("silver.taxi_trips_deduplicated")
df_final = spark.read.table("silver.taxi_trips")

deduplicated_months = (
    df_deduplicated
    .select("source_year", "source_month")
    .distinct()
)

processed_months = (
    df_final
    .select(
        year("pickup_datetime").alias("source_year"),
        month("pickup_datetime").alias("source_month")
    )
    .distinct()
)

new_months = [
    (row.source_year, row.source_month)
    for row in deduplicated_months.join(
        processed_months,
        ["source_year", "source_month"],
        "left_anti"
    ).collect()
]

print(f"New months: {len(new_months)}")
print(sorted(new_months))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, when, isnan, min, max, avg, expr, broadcast, to_date, year, month, lit
from functools import reduce

new_months_df = spark.createDataFrame( new_months, ["source_year", "source_month"])

df = spark.read.table("silver.taxi_trips_deduplicated").join(broadcast(new_months_df), ["source_year", "source_month"], "inner")

df = df.filter(
    (col("payment_type") != 6) &
    (col("trip_distance") >= 0) &
    (year("pickup_datetime") == col("source_year")) &
    (month("pickup_datetime") == col("source_month"))
)

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

df_cleaned = df_cleaned.filter( (col("amount") > 0) & (col("amount") <= 200))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zones = broadcast( spark.read.table("silver.taxi_zones").select("location_id").distinct())

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

df_cleaned.write.format("delta").mode("append").saveAsTable("silver.taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
