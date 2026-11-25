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

from pyspark.sql.functions import col, count, to_date, expr, lag
from pyspark.sql.window import Window

df = spark.table("silver.taxi_trips")

df_daily = (df.groupBy(to_date("pickup_datetime").alias("date"))
            .agg(count("*").alias("total_trips")))

window = Window.orderBy("date").rowsBetween(-3, 3)

df_daily = df_daily.withColumn("median_7d", expr("percentile_approx(total_trips, 0.5)").over(window))

df_suspicious = df_daily .filter(col("total_trips") < col("median_7d") * 0.5).orderBy("date")

display(df_suspicious)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit
from functools import reduce

suspicious_dates = [  row["date"] for row in df_suspicious.select("date").collect()]

suspicious_months = {date.strftime("%Y-%m") for date in suspicious_dates}

raw_files = []

for file in mssparkutils.fs.ls("Files/bronze/taxi_data"):
    if file.name.endswith(".parquet"):
        month = file.name.replace(".parquet", "").split("_")[-1]

        if month in suspicious_months:
            raw_files.append(file.path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_daily_dfs = []

for file in raw_files:

    df_raw = spark.read.parquet(file)

    pickup_col = (
        "tpep_pickup_datetime"
        if "tpep_pickup_datetime" in df_raw.columns
        else "pickup_datetime")

    df_raw_daily = (
        df_raw
        .select(to_date(col(pickup_col)).alias("date"))
        .filter(col("date").isin(suspicious_dates))
        .groupBy("date")
        .agg(count("*").alias("raw_trips")))

    raw_daily_dfs.append(df_raw_daily)

df_raw_counts = reduce( lambda a, b: a.unionByName(b), raw_daily_dfs)

df_raw_counts = (
    df_raw_counts
    .groupBy("date")
    .agg({"raw_trips": "sum"})
    .withColumnRenamed("sum(raw_trips)", "raw_trips"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_comparison = (df_suspicious
    .select("date", "total_trips", "median_7d")
    .join(df_raw_counts, "date", "left")
    .fillna({"raw_trips": 0})
    .withColumn( "retention_percentage", (col("total_trips") / col("raw_trips")) * 100)
    .orderBy("date"))

display(df_comparison)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
