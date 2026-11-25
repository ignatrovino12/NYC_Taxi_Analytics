# NYC_Taxi_Analytics

An end-to-end data analytics project built with Microsoft Fabric and Power BI, exploring the relationships between NYC taxi mobility, air pollution, and economic trends over the years.

## Project Overview

The project integrates multiple public datasets to analyze:

- NYC taxi trip patterns and mobility trends.
- Daily pollution levels across NYC taxi zones.
- Relationships between transportation activity and air quality over time.
- Economic growth and USD/EUR exchange rate fluctuations.

## Architecture

The project follows a Medallion Architecture:

- Bronze: Raw data ingestion in a Lakehouse from external sources.
- Silver: Data cleaning, validation, deduplication, and transformations using PySpark Notebooks.
- Gold: Analytical fact and dimension tables organized into a Star Schema in a Fabric Warehouse.
- Power BI: Interactive dashboards for exploring mobility, air quality, and economic indicators.

## Documentation

More details about the project can be found in the documentation folder, including diagrams, ingestion workflows, transformation logic, data modeling, and dashboard explanations.

