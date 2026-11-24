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

from pyspark.sql.functions import col, count, sum, avg, date_format, to_date, round
import com.microsoft.spark.fabric

df_taxi = spark.table("silver.taxi_trips")

df_taxi.printSchema()
df_taxi.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_taxi = df_taxi.groupBy(
    to_date("pickup_datetime").alias("date"),
    col("pickup_location_id").alias("zone_key")
).agg(
    count("*").alias("total_trips"),
    round(sum("amount"), 2).alias("total_fares"),
    round(avg("amount"), 2).alias("average_fare"),
    round(avg("trip_distance"), 2).alias("average_trip_distance"),
    round(avg("trip_duration_minutes"), 2).alias("average_trip_duration"),
    round(avg("average_speed_mph"), 2).alias("average_speed_mph"),
    round(avg("passenger_count"), 2).alias("average_passenger_count")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_taxi = df_fact_taxi.withColumn("date_key", date_format("date", "yyyyMMdd").cast("integer")).drop("date")

# df_fact_taxi.printSchema()
# df_fact_taxi.show(5)

# print("Total rows:", df_fact_taxi.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_taxi.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.FactTaxiDaily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
