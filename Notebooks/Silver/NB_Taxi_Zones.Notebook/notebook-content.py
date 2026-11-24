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

df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/taxi_zones/taxi_zone_lookup.csv")

display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.select("Borough").distinct())
display(df.select("service_zone").distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when

def fix_na_values(column_name):
    return (when(col("LocationID") == 264, "Unknown")
            .when(col("LocationID") == 265, "Outside of NYC")
            .otherwise(col(column_name)))

df_cleaned = df.withColumns({
    "Borough": fix_na_values("Borough"),
    "Zone": fix_na_values("Zone"),
    "service_zone": fix_na_values("service_zone")
})

df_cleaned = df_cleaned.toDF("location_id", "borough", "zone", "service_zone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned.write.format("delta").mode("overwrite").saveAsTable("silver.taxi_zones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
