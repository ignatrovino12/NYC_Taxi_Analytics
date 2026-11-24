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

from pyspark.sql.functions import col

df = spark.read.table("bronze.air_quality_measurements")

df_cleaned = df.filter(
    col("datetime_from_utc").isNotNull() &
    col("datetime_to_utc").isNotNull() &
    col("value").isNotNull()
)

df_cleaned = df_cleaned.dropDuplicates()
df_cleaned = df_cleaned.dropDuplicates(["sensor_id", "datetime_from_utc", "datetime_to_utc", "parameter_name"])
df_cleaned = df_cleaned.drop("interval")
df_cleaned = df_cleaned.filter(col("datetime_to_utc") > col("datetime_from_utc"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pm25 = df_cleaned.filter((col("parameter_name") == "pm25") & (col("value") >= 0))

df_sensor_hourly = df_pm25.groupBy(
    date_trunc("hour", col("datetime_from_utc")).alias("hour"),
    "sensor_id",
    "location_id"
).agg(
    avg("value").alias("sensor_pm25"),
    count("*").alias("measurements"))

df_hour_consensus = df_sensor_hourly.groupBy("hour").agg(
    countDistinct("sensor_id").alias("total_sensor_count"),
    spark_sum( when(col("sensor_pm25") >= 20, 1).otherwise(0)).alias("high_sensor_count")
).withColumn("high_sensor_ratio", 
            col("high_sensor_count") / col("total_sensor_count"))

a = df_sensor_hourly.alias("a")
b = df_sensor_hourly.alias("b")

df_peer_stats = (
    a.join(b,
        (col("a.hour") == col("b.hour")) &
        ~(
            (col("a.sensor_id") == col("b.sensor_id")) &
            (col("a.location_id") == col("b.location_id"))
        ),"inner")
    .groupBy(
        col("a.hour").alias("hour"),
        col("a.sensor_id").alias("sensor_id"),
        col("a.location_id").alias("location_id"),
        col("a.sensor_pm25").alias("sensor_pm25")
    )
    .agg(
        percentile_approx( col("b.sensor_pm25"), 0.5, 10000).alias("peer_median"),
        countDistinct("b.sensor_id").alias("peer_count")
    ))

df_comparison = ( df_peer_stats.join(df_hour_consensus, "hour")
    .withColumn( 
        "relative_ratio",
        col("sensor_pm25") / when( col("peer_median") > 0, col("peer_median")))
    .withColumn(
        "is_suspect",
        (col("peer_count") >= 9) &
        (col("sensor_pm25") >= 20) &
        (col("sensor_pm25") > col("peer_median") * 4) &
        ((col("high_sensor_ratio") < 0.20) | (col("sensor_pm25") > col("peer_median") * 10))
        ))


df_suspect_hours = df_comparison.filter(col("is_suspect")).select(
    "hour",
    "sensor_id",
    "location_id",
    "sensor_pm25",
    "peer_median",
    "relative_ratio",
    "high_sensor_count")

df_pm25_clean = (
    df_pm25.withColumn(
        "hour",
        date_trunc("hour", col("datetime_from_utc")))
    .join(
        df_suspect_hours.select( "hour", "sensor_id", "location_id").distinct(),
        on=["hour", "sensor_id", "location_id"],
        how="left_anti")
    .drop("hour"))

df_no2 = df_cleaned.filter(col("parameter_name") != "pm25")
df_cleaned = df_no2.unionByName(df_pm25_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned = (df_cleaned.withColumnRenamed("name", "location_name")
                        .withColumnRenamed("units", "unit"))

df_cleaned.write.format("delta").mode("append").saveAsTable("silver.air_quality_measurements")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
