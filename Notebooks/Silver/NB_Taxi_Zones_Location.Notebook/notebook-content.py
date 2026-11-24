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

import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

file_path= "Files/bronze/taxi_zones/NYC_Taxi_Zones_Location.geojson"
file_lines = spark.read.text(file_path).collect()
file_content = "\n".join(row["value"] for row in geojson_lines)

data = json.loads(file_content)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = []

for feature in data["features"]:
    properties = feature["properties"]

    rows.append((
        int(properties["locationid"]),
        properties["zone"],
        properties["borough"],
        json.dumps(feature["geometry"])
    ))

schema = StructType([
    StructField("location_id", IntegerType(), False),
    StructField("zone", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("geometry_json", StringType(), False)
])

df_zones = spark.createDataFrame(rows, schema)
df_zones.show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count

display(df_zones.filter(col("location_id").isNull() |col("geometry_json").isNull()).count())

display(df_zones.select("location_id").distinct().count())

df_zones.groupBy("location_id").agg(count("*").alias("count")).filter(col("count") > 1).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for location_id in [56, 103]:
    print(f"ID: {location_id}")

    for feature in data["features"]:
        properties = feature["properties"]

        if int(properties["locationid"]) == location_id:
            print(properties["zone"], properties["borough"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

id_counters = {}
rows = []

for feature in data["features"]:
    properties = feature["properties"]
    original_id = int(properties["locationid"])

    if original_id in [56, 103]:
        id_counters[original_id] = id_counters.get(original_id, 0) + 1

        if original_id == 56:
            location_id = 56 + (id_counters[original_id] - 1)
        else:
            location_id = 103 + (id_counters[original_id] - 1)
    else:
        location_id = original_id

    rows.append((location_id, properties["zone"], properties["borough"], json.dumps(feature["geometry"])))

df_zones = spark.createDataFrame(rows, schema)
df_zones.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_zones.write.format("delta").mode("overwrite").saveAsTable("silver.taxi_zone_locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
