**Real-Time Traffic & Weather Analytics Platform**

This project integrates real-time traffic, historical accident data, multi-year weather datasets, and machine learning forecasting into a unified data platform capable of producing insights and risk predictions for cities.

**It is built using:**

Apache Airflow – Workflow orchestration
Snowflake – Cloud data warehousing
dbt – SQL transformations and modeling
Open-Meteo API – Real-time + historical weather
TomTom Traffic API – Real-time congestion and incidents
Python – Data ingestion and automation
Snowflake ML – Forecasting for accident risk
Preset / Superset – Dashboarding

**High-Level Architecture**
The platform follows a multi-layer data architecture, described below:
1. Data Ingestion Layer
Airflow DAGs fetch real-time data from TomTom Traffic API and Open-Meteo API.
Batch extraction pipelines load multi-year accident and weather data.
Python scripts format, validate, and stage raw files into Snowflake.
2. Raw / Staging Layer (Snowflake + dbt)
Raw data lands in Snowflake in unmodified form.
dbt builds standardized staging models:
Cleaned traffic tables
Harmonized accident datasets
Weather snapshots
City-level metadata
3. Transformation Layer (dbt Marts)
dbt creates marts such as:
Accident Fact Tables
Real-Time Traffic Metrics
Daily Weather Metrics
Joined Accident × Weather Features
4. Machine Learning Layer (Snowflake ML)
Feature engineering combines the following:
Historical accident frequency
Weather trends
Traffic congestion indicators
A Snowflake ML.FORECAST model generates 7-day accident predictions.
Predictions are enhanced using real-time weather + traffic signals.
5. Analytics Layer
Preset dashboards visualize:
Traffic congestion patterns
Weather–accident correlations
Historical accident analysis
Accident risk forecasting
City-level risk scoring (Low → Critical)

**Detailed Components**
1. Historical Accident & Weather Pipeline
This pipeline extracts, processes, and enriches multi-year accident data:
Ingested 3.5M+ accident records from the US Accidents dataset.
Filtered for California-only data.
Cleaned missing values, standardized coordinates, normalized timestamps.
Loaded into Snowflake using Airflow.
Enriched each accident record with historical weather conditions:
Temperature
Precipitation
Visibility
Wind characteristics
Built dbt models to produce daily city-level accident summaries.
2. Real-Time Traffic Pipeline
Runs every 30 minutes using Airflow:
Fetches live traffic flow and incident data from TomTom API.
Computes:
Congestion score
Delays and incident severity
Average speed per road segment
Stores traffic snapshots in Snowflake.
dbt builds a real-time traffic mart used in dashboards and ML.
3. Real-Time Weather Pipeline
Runs every 15 minutes:
Retrieves live weather conditions for multiple California cities.
Captures:
Temperature
Humidity
Wind speed
Rain / precipitation intensity
Stores live snapshots and merges them into dbt models for monitoring.
4. Historical Weather Pipeline
Extracts multiple years of weather history.
Handles API rate limits with retry logic.
Aggregates into daily weather trend tables.
Joins with accident data for correlations and ML features.
5. Accident Forecasting with Snowflake ML
Steps:
Feature engineering:
7-day rolling accident averages
Weather conditions
Traffic congestion levels
Seasonal patterns
Snowflake's ML.FORECAST trains a time-series model.
A real-time adjustment layer:
Applies corrections using the latest weather + traffic signals.
Output:
7-day accident prediction
City-level risk score (Low, Moderate, High, Severe, Critical)

**Dashboards (Preset)**
The dashboards include:
1. Accident Analytics
Daily trends
Long-term patterns
Seasonal peaks
2. Weather × Accident Correlation
Heatmaps
Multivariate comparisons
3. Live Traffic Monitoring
Active incidents
Congestion scores
City-by-city comparisons
4. Combined Risk Dashboard
7-day forecast
Real-time adjustments
City risk scoring

<img width="1621" height="1098" alt="Screenshot 2025-12-09 at 5 26 26 PM" src="https://github.com/user-attachments/assets/e8046f1a-a982-48c2-8718-4475507140d0" />
<img width="1629" height="1111" alt="Screenshot 2025-12-09 at 5 26 05 PM" src="https://github.com/user-attachments/assets/6ed2013b-aa1f-4181-bf50-2481ea15265b" />
<img width="870" height="553" alt="Screenshot 2025-12-09 at 3 28 01 PM" src="https://github.com/user-attachments/assets/b9510eec-1160-4d4d-9c86-e07e2fab1929" />
