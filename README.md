**Real-Time Traffic & Weather Analytics Platform**
**Airflow | Snowflake | dbt | Open-Meteo API | TomTom Traffic API | Preset | Snowflake ML**
An end-to-end, production-style platform combining real-time traffic, historical accidents, multi-year weather data, and machine learning forecasting. Designed to help cities understand traffic patterns, weather impact, and predict accident risk.

What we Built
1) Historical Accident & Weather Pipeline
Processed 3.5M+ accident records from the US Accidents dataset
Cleaned, filtered, and loaded California-focused data using Airflow
Built staging + mart models in dbt
Enriched accidents with historical weather metrics:
Temperature
Rain
Visibility
Wind
Created a daily city-level accident fact table
2) Real-Time Traffic Pipeline
Ingested live traffic flow & incident APIs every 30 minutes
Derived key metrics:
Congestion score
Incident severity
Traffic delays
Speed profiles
Built a traffic real-time mart in Snowflake
3) Real-Time Weather Pipeline
Pulled Open-Meteo live weather snapshots every 15 minutes
Captured temperature, humidity, wind, precipitation
Modeled real-time weather metrics using dbt
4) Historical Weather Pipeline
Queried multi-year weather history across California
Implemented retry-safe, rate-limited API extraction
Aggregated weather into daily city-level time series
5) Accident Forecasting Using Snowflake ML
Engineered features combining accidents, weather, and congestion
Trained a 7-day accident forecasting model with Snowflake ML
Adjusted predictions using live traffic & weather
Produced a City-Level Accident Risk Score (Low → Critical)

Interactive Dashboards (Preset / Apache Superset)
Dashboards include:
Daily accident trends
Weather ↔ Accident correlation heatmaps
Real-time congestion/incident monitoring
Live weather overlays
7-day accident risk forecasting
