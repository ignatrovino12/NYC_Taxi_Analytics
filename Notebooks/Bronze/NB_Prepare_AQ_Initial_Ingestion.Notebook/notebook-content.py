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

import re
import json
import calendar
from datetime import date, timedelta
from pyspark.sql import functions as F

BRONZE_PATH = "Files/bronze/taxi_data"
AQ_TABLE = "bronze.air_quality_measurements"
CHECKPOINT_TABLE = "bronze.sensor_checkpoints"

def get_taxi_period():
    files = mssparkutils.fs.ls(BRONZE_PATH)
    months = []

    for file in files:
        filename = file.name

        if filename.startswith("yellow_tripdata_") and filename.endswith(".parquet"):
            year_month = filename.removeprefix("yellow_tripdata_").removesuffix(".parquet")
            year, month = map(int, year_month.split("-"))
            months.append((year, month))

    first_year, first_month = min(months)
    last_year, last_month = max(months)

    date_from = date(first_year, first_month, 1)
    last_day = calendar.monthrange(last_year, last_month)[1]
    date_to = date(last_year, last_month, last_day)

    return {"DateFrom": date_from.isoformat(), "DateTo": date_to.isoformat()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_sensors_period():

    df = spark.read.table(AQ_TABLE)

    last_dates = (df.groupBy("sensor_id")
        .agg(F.max("datetime_from_utc").alias("last_date"))
        .filter(F.col("last_date").isNotNull())
        .withColumn("last_date", F.to_date("last_date"))
        .withColumn("sensor_id", F.col("sensor_id").cast("long")))

    last_dates.write.format("delta").mode("overwrite").saveAsTable(CHECKPOINT_TABLE)
    
    return last_dates.count()

def generate_chunks(date_from, date_to):

    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)

    chunks = []
    current = start

    while current <= end:
        if current.month <= 4:
            quarter_end = date(current.year, 4, 30)
        elif current.month <= 8:
            quarter_end = date(current.year, 8, 31)
        else:
            quarter_end = date(current.year, 12, 31)

        chunk_end = min(quarter_end, end)

        chunks.append({
            "ChunkStartDate": current.isoformat(),
            "ChunkEndDate": chunk_end.isoformat()
        })

        current = chunk_end + timedelta(days=1)

    return chunks



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_period = get_taxi_period()
date_from = taxi_period["DateFrom"]
date_to = taxi_period["DateTo"]
# get_sensors_period()

# Manual dates
# date_from = "2014-01-01"
# date_to = "2016-09-30"

result = {
    "DateFrom": date_from,
    "DateTo": date_to,
    "Chunks": generate_chunks(date_from, date_to)
}

notebookutils.notebook.exit(json.dumps(result, separators=(",", ":")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
