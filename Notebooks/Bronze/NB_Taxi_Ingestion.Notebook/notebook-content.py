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
import time
from datetime import datetime

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BRONZE_PATH = "Files/bronze/taxi_data"

FORCE_RELOAD = False
CHUNK_SIZE = 8 * 1024 * 1024

TARGET_DATES = 10
MAX_DATES_TO_CHECK = 20

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def download_file(url, local_path):
    for attempt in range(3):
        try:
            response = requests.get(url, stream=True, timeout=(30, 300))
            response.raise_for_status()
            size = 0

            with response, open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        size += len(chunk)

            if size == 0:
                raise ValueError("File is empty.")

            return size

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if (status!=429) and not (500<=status<600) or attempt == 2:
                raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)

def download_taxi_month(year_month, files, force_reload=False):
    filename = f"yellow_tripdata_{year_month}.parquet"
    url = f"{BASE_URL}/{filename}"
    destination = f"{BRONZE_PATH}/{filename}"
    local_path = os.path.join(tempfile.gettempdir(), filename)

    exists = filename in files

    if exists and not force_reload:
        return "SKIPPED"

    try:
        download_file(url, local_path)
        if exists:
            mssparkutils.fs.rm(destination)
        mssparkutils.fs.cp(f"file:{local_path}", destination)
        files.add(filename)

        return "SUCCESS"

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def previous_month(year, month):
    month -= 1

    if month == 0:
        month = 12
        year -= 1

    return year, month


def ingest_taxi_data():
    files = {f.name for f in mssparkutils.fs.ls(BRONZE_PATH)}

    current = datetime.now()
    year = current.year
    month = current.month

    results = {
        "SUCCESS": [],
        "SKIPPED": [],
        "UNAVAILABLE": [],
        "FAILED": []}

    valid_dates = 0
    checked_dates = 0

    while valid_dates < TARGET_DATES and checked_dates < MAX_DATES_TO_CHECK:

        year_month = f"{year}-{month:02d}"
        checked_dates += 1

        try:
            status = download_taxi_month(year_month, files, FORCE_RELOAD)
            results[status].append(year_month)
            valid_dates += 1

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None

            if status_code in (403, 404):
                results["UNAVAILABLE"].append(year_month)
            else:
                results["FAILED"].append({"month": year_month, "error": str(e)})

        except Exception as e:
            results["FAILED"].append({"month": year_month, "error": str(e)})

        year, month = previous_month(year, month)

    return {
        "files_found": valid_dates,
        "files_checked": checked_dates,
        "results": results}

output = ingest_taxi_data()
output

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
