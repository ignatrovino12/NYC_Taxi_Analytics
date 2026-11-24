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

df_dim_fx = spark.table("silver.ecb_exchange_rates").select(
    date_format(col("date"), "yyyyMMdd").cast("integer").alias("date_key"),
    col("usd_per_eur"))

df_dim_zone.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.DimZone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_gdp = spark.table("silver.world_bank_gdp")

df_dim_gdp.write.mode("overwrite").synapsesql("WH_Taxi_Analytics.dbo.DimGDP")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
