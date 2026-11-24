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

%pip install shapely

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, sum, avg, date_format, to_date, round, countDistinct, min, max
import com.microsoft.spark.fabric

df_aq = spark.table("silver.air_quality_measurements")
df_zones = spark.table("silver.taxi_zone_locations")

df_aq.printSchema()
df_aq.show(5)

df_zones.printSchema()
df_zones.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from shapely.geometry import shape, Point
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

zone_geometries = []

for row in df_zones.select("location_id", "geometry_json").collect():

    geometry = shape(json.loads(row["geometry_json"]))
    zone_geometries.append((row["location_id"], geometry))

df_sensors = df_aq.select("sensor_id", "latitude", "longitude").dropDuplicates(["sensor_id"])
sensors = df_sensors.collect()

rows = []
unmatched_sensors = []
uncertain_sensors = []

for sensor in sensors:
    point = Point(sensor["longitude"], sensor["latitude"])

    matches = [location_id for location_id, geometry in zone_geometries
                           if geometry.covers(point)]

    if len(matches) == 1:
        rows.append((sensor["sensor_id"], matches[0]))

    elif len(matches) == 0:
        unmatched_sensors.append(sensor["sensor_id"])

    else:
        uncertain_sensors.append((sensor["sensor_id"], matches))

schema = StructType([StructField("sensor_id", LongType(), False), StructField("zone_key", IntegerType(), False)])
df_sensor_zone = spark.createDataFrame(rows, schema)

print("Matched sensors:", df_sensor_zone.count())
print("Unmatched sensors:", len(unmatched_sensors))
print("Uncertain sensors:", len(uncertain_sensors))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_aq_mapped = df_aq.join(df_sensor_zone, on="sensor_id", how="inner")

df_sensor_daily = df_aq_mapped.groupBy(
    to_date("datetime_from_utc").alias("date"),
    "zone_key",
    "sensor_id",
    "parameter_name",
    "unit"
).agg(
    avg("value").alias("sensor_daily_avg"),
    count("*").alias("measurement_count")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_aq_daily = (
df_sensor_daily.groupBy(
    "date",
    "zone_key",
    "parameter_name",
    "unit"
).agg(
    round(avg("sensor_daily_avg"), 4).alias("average_value"),
    round(sum("measurement_count"), 0).alias("total_measurements"),
    count("*").alias("sensor_count")
).withColumn("date_key",
    date_format("date", "yyyyMMdd").cast("integer")
).drop("date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_aq_daily.printSchema()
df_fact_aq_daily.show(5)

print("Total rows:", df_fact_aq_daily.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_aq_daily.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.FactAirQualityDaily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
