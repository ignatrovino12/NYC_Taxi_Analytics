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

from pyspark.sql.functions import col, lit, expr, sequence, explode, to_date, date_format
from pyspark.sql.functions import year, quarter, month, dayofmonth, dayofweek, when
from datetime import date
import calendar

BRONZE_PATH = "Files/bronze/taxi_data"
files = mssparkutils.fs.ls(BRONZE_PATH)

months = []

for file in files:
    filename = file.name

    if filename.startswith("yellow_tripdata_") and filename.endswith(".parquet"):
        year_month = filename.removeprefix("yellow_tripdata_").removesuffix(".parquet")
        file_year, file_month = map(int, year_month.split("-"))
        months.append((file_year, file_month))

if not months:
    raise ValueError("No valid Yellow Taxi Parquet files found.")

first_year, first_month = sorted(months)[0]
last_year, last_month = sorted(months)[-1]

start_date = date(first_year, first_month, 1).isoformat()

last_day = calendar.monthrange(last_year, last_month)[1]
end_date = date(last_year, last_month, last_day).isoformat()

print(f"Period: {start_date} to {end_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_date = spark.range(1).select(
    explode(sequence(
            to_date(lit(start_date)),
            to_date(lit(end_date)),
            expr("interval 1 day"))
    ).alias("date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_date = df_date.select(
    date_format("date", "yyyyMMdd").cast("integer").alias("date_key"),
    col("date"),
    year("date").alias("year"),
    quarter("date").alias("quarter"),
    month("date").alias("month"),
    date_format("date", "MMMM").alias("month_name"),
    dayofmonth("date").alias("day"),
    dayofweek("date").alias("day_of_week"),
    date_format("date", "EEEE").alias("day_name"),
    when(dayofweek("date").isin(1, 7), True)
        .otherwise(False).alias("is_weekend"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.date_lookup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
