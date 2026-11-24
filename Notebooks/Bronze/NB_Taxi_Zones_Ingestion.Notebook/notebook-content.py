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

import requests
import os
import tempfile

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
BRONZE_PATH = "Files/bronze/taxi_zones"
FILENAME = "taxi_zone_lookup.csv"
DESTINATION = f"{BRONZE_PATH}/{FILENAME}"
mssparkutils.fs.mkdirs(BRONZE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest_taxi_zones():
    local_path = os.path.join(tempfile.gettempdir(), FILENAME)
    temp_destination = f"{BRONZE_PATH}/taxi_zone_lookup_updated.csv"

    try:
        response = requests.get(URL, timeout=(30, 120))
        response.raise_for_status()

        with open(local_path, "wb") as f:
            f.write(response.content)

        if os.path.getsize(local_path) == 0:
            raise ValueError("File is empty.")

        mssparkutils.fs.cp(f"file:{local_path}", temp_destination)
        if mssparkutils.fs.exists(DESTINATION):
            mssparkutils.fs.rm(DESTINATION)
        mssparkutils.fs.mv(temp_destination, DESTINATION)

        return "SUCCESS"

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

ingest_taxi_zones()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
