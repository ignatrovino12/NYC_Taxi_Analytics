# Data Lineage

## 1. Overview

This section describes the data lineage of the four external sources used in the project. It illustrates how raw data flows through the Bronze, Silver, and Gold layers in Microsoft Fabric before being consumed by Power BI. The project follows a Medallion Architecture, separating raw ingestion, data transformation, and analytical modeling.

## 2. End-to-End Data Flow

![Diagram](./diagrams/Architecture%20Diagram.png)

## 3. Source-to-Gold Mapping

| Source | Silver Transformations | Gold Tables |
| --- | --- | --- |
| NYC TLC| Cleaning, validation, trip metrics, daily aggregation | FactTaxiDaily, DimZone, DimDate |
| Open AQ | Filtering, validation, anomaly detection, daily aggregation | FactAirQualityDaily |
| World Bank | Extraction and standardization of annual GDP values| DimGDP |
| ECB | Parsing and standardization of exchange rates | DimFX |

## 4. All Sources Data Lineage

### NYC Taxi & Limousine Commision

![Diagram](./diagrams/Taxi%20Data%20Lineage.png)

### OpenAQ

![Diagram](./diagrams/Air%20Quality%20Lineage.png)

### World Bank GDP

![Diagram](./diagrams/World%20Bank%20Lineage.png)

### Foreign Exchange Rates

![Diagram](./diagrams/ECB%20Lineage.png)

## 5. Data Lineage in Microsoft Fabric

The lineage diagrams provide an end-to-end view of how data from each source is processed and integrated into the Microsoft Fabric architecture. They highlight the transformations and dependencies between the Bronze, Silver, and Gold layers, as well as the connection to the Power BI semantic model. Together, they offer a clear overview of the data journey, from its original source to its final use in analytics and reporting.
