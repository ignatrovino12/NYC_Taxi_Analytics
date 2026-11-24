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

import com.microsoft.spark.fabric
from pyspark.sql.functions import col, date_format

df_dim_date = spark.table("silver.date_lookup")

df_dim_date.printSchema()
df_dim_date.show(5)

print("Total dates:", df_dim_date.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_date.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.DimDate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_zone = spark.table("silver.taxi_zones").select(
    col("location_id").alias("zone_key"),col("borough"),
    col("zone"), col("service_zone"))

df_dim_zone.printSchema()
df_dim_zone.show(5)

print("Total zones:", df_dim_zone.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_zone.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.DimZone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_fx = spark.table("silver.ecb_exchange_rates").select(
    date_format(col("date"), "yyyyMMdd").cast("integer").alias("date_key"),
    col("usd_per_eur"))

df_dim_fx.printSchema()
df_dim_fx.show(5)

print("Total rows:", df_dim_fx.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_fx.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.DimFX")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_gdp = spark.table("silver.world_bank_gdp")

df_dim_gdp.printSchema()
df_dim_gdp.orderBy(col("year").desc()).show(5)

print("Total years:", df_dim_gdp.count())
print("Distinct years:", df_dim_gdp.select("year").distinct().count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_gdp.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.DimGDP")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
