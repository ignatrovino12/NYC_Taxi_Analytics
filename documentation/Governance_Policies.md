# Governance Policies

## 1. Overview
This section describes the data governance practices applied throughout the Microsoft Fabric project, covering data quality, access control, security, refresh procedures, and known limitations.

## 2. Data Quality

Data quality is maintained primarily during the Silver layer processing, where validation, cleaning, and transformation steps are applied before the data is loaded into the Gold Warehouse.

- Validation: Invalid or incomplete records are handled during data processing.
- Cleaning: Invalid measurements, such as negative values, are excluded from the datasets.
- Anomaly Detection: Unusual readings (ex. PM2.5 values) are evaluated and compared with measurements from other rows and eliminated.
- Standardization: Data types, timestamps, and relevant fields are standardized before loading into the Gold layer.
- Aggregation: Data is aggregated at daily level before being loaded into the Gold Warehouse, supporting consistent reporting and analysis.

## 3. Data Refresh

Data refresh is managed through Microsoft Fabric pipelines and scheduled execution of data processing activities.

- Pipeline Scheduling: Two main data pipelines, one for bronze data and one for silver/gold data are configured to run on a daily schedule.
- Incremental Loading: Data ingestion checks for newly available records to avoid unnecessary data retrieval and duplicate processing.
- Silver and Gold Updates: Transformation notebooks and Warehouse loading activities are executed only when new data is available.
- Power BI Refresh: The semantic model is refreshed after the Gold Warehouse has been updated, ensuring that reports reflect the latest processed data.

## 4. Limitations

The project has several data availability and integration limitations that should be considered when interpreting the analysis results.

- **Air Quality Data Coverage:** The collected OpenAQ dataset contains no air quality measurements for NYC sensors before 2016. Therefore, comparisons between taxi activity and air quality are limited to the period for which air quality data is available.

- **NO2 Sensor Availability:** No NO2 sensors were available within the NYC taxi-zone areas considered in the spatial analysis. Consequently, NO2 measurements cannot be analyzed at the taxi-zone level.

- **Historical Taxi Data:** The NYC TLC dataset used in the project covers the period from 2011 to 2026. Data from before 2011 follows a different schema and would require a separate ingestion and transformation process. While integrating this data is technically possible, the project scope and current implementation were limited to records from 2011 onward.

- **Taxi Zone Assignment:** Taxi trips are assigned to zones based exclusively on the Pickup Location ID. Drop-off locations are not considered in the zone-level analysis. Therefore, the results reflect taxi activity originating from each zone rather than the total traffic associated with that zone.

- **Different Data Coverage:** The external sources have different historical coverage and update frequencies, which limits the periods over which all datasets can be consistently compared.
