**Real-Time Traffic & Weather Analytics Platform**
<p align="center"> <img src="/images/traffic_architecture.png" width="80%"> </p>
Tech Stack
Component	Technology
Orchestration	Apache Airflow
Data Warehouse	Snowflake
Transformation	dbt
Real-Time APIs	TomTom Traffic API, Open-Meteo API
ML Forecasting	Snowflake ML
Dashboards	Preset / Apache Superset
Language	Python

What I Built
1️⃣ Historical Accident & Weather Pipeline
Processed 3.5M+ accident records (CA only)
Cleaned & loaded data using Airflow DAGs
Built staging + mart layers in dbt
Added historical weather:
Temperature
Rain
Wind
Visibility
Created a daily city-level accident fact table
2️⃣ Real-Time Traffic Pipeline
Runs every 30 minutes:
Traffic flow ingestion
Incident extraction
Congestion scoring
Speed profiling
Snowflake real-time traffic mart
3️⃣ Real-Time Weather Pipeline
Runs every 15 minutes:
Live temperature, humidity, wind, precipitation
Stored snapshots
dbt transformations for real-time metrics
4️⃣ Historical Weather Pipeline
Multi-year weather extraction across CA
Rate-limited retries
Built aggregated daily weather trends
5️⃣ Accident Forecasting (Snowflake ML)
Feature engineering (accident × weather × traffic)
Trained 7-day accident forecast model
Real-time correction using live congestion & weather
Output: City-Level Accident Risk Score (Low → Critical)

📊 Dashboards (Preset / Superset)
<p align="center"> <img src="/images/dashboard_example.png" width="80%"> </p>
Dashboards include:
Accident trend analysis
Weather–accident correlations
Real-time congestion monitoring
Weather overlays
ML-based risk forecast visualization
