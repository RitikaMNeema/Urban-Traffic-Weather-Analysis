# 🚦 Urban Traffic & Weather Impact Analysis for California

**A Data-Driven Approach to Smart Urban Mobility**

**Author:** Ritika Mukesh Neema

**Course:** DATA 226  
**Tools:** Airflow • Snowflake • dbt • Preset • TomTom API • Open-Meteo API

---

## 🎯 Project Overview

This project presents a comprehensive data-driven solution that integrates historical and real-time traffic and weather data to optimize urban mobility and enhance city resilience across California. The system processes millions of data points to identify patterns, predict congestion hotspots, and provide actionable insights for urban planning and traffic management.

### Key Objectives

1. **Proactive Management:** Predicting congestion before it materializes
2. **Integrated Analysis:** Revealing correlations between weather patterns and traffic incidents
3. **Real-Time Visualization:** Empowering commuters and emergency responders with live situational awareness

---

## 🏗️ System Architecture

The project employs a modern cloud-native architecture designed for scalability and separation of concerns.

### Architecture Components

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│   Data Sources  │ ───> │  Apache Airflow  │ ───> │   Snowflake     │
│  • Kaggle       │      │  (Orchestration) │      │ (Data Warehouse)│
│  • TomTom API   │      │  • ETL Pipelines │      │  • RAW Schema   │
│  • Open-Meteo   │      │  • ELT Pipelines │      │  • ANALYTICS    │
└─────────────────┘      └──────────────────┘      └─────────────────┘
                                                             │
                                                             ▼
                         ┌──────────────────┐      ┌─────────────────┐
                         │   Preset/BI      │ <─── │      dbt        │
                         │  (Dashboards)    │      │ (Transformations)│
                         │  • Live Traffic  │      │  • Staging      │
                         │  • Weather       │      │  • Analytics    │
                         │  • Correlations  │      │  • Marts        │
                         └──────────────────┘      └─────────────────┘
```

### Technology Stack

- **Orchestration:** Apache Airflow
- **Data Warehouse:** Snowflake
- **Transformation:** dbt (data build tool)
- **Visualization:** Preset.io
- **APIs:** TomTom Traffic API, Open-Meteo Weather API
- **ML:** Snowflake Cortex ML (FORECAST function)

---

## 📊 Data Sources

### Historical Data Sources

**US Accidents Dataset**
- **Source:** Kaggle dataset repository
- **Coverage:** 2016-2023
- **Records:** 7+ million (filtered to California)
- **Format:** CSV
- **Key Attributes:** Location coordinates, severity, timestamp, weather conditions, road features
- **Update Frequency:** Quarterly batch updates

**Open-Meteo Historical Weather API**
- **Source:** Open-Meteo historical weather archive
- **Coverage:** 2016-2023 (aligned with traffic data)
- **Format:** JSON API responses
- **Parameters:** Temperature, precipitation, wind speed, visibility, humidity, pressure
- **Resolution:** Hourly weather data

### Real-Time Data Sources

**TomTom Traffic API**
- **Update Frequency:** 30-minute intervals
- **Format:** JSON API responses with GeoJSON geometry
- **Metrics:** Current speed, free-flow speed, confidence level, road closure status
- **Coverage:** 50+ major California cities

**Open-Meteo Forecast API**
- **Update Frequency:** 15-minute intervals
- **Format:** JSON API responses
- **Parameters:** Current conditions plus 7-day forecasts
- **Resolution:** High-resolution global coverage with California focus

---

## 📂 Project Structure

```
project-root/
├── airflow/
│   └── dags/
│       ├── traffic_historical_etl.py
│       ├── traffic_historical_elt.py
│       ├── realtime_traffic_etl.py
│       ├── realtime_traffic_elt.py
│       ├── weather_historical_etl.py
│       ├── weather_historical_elt.py
│       ├── weather_realtime_etl.py
│       ├── weather_realtime_elt.py
│       └── ml_traffic_accident_forecast_realtime.py
├── dbt/
│   └── traffic_weather_analytics/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_traffic_flow_realtime.sql
│       │   │   ├── stg_traffic_accidents.sql
│       │   │   ├── stg_weather_realtime.sql
│       │   │   └── stg_weather_historical.sql
│       │   ├── analytics/
│       │   │   └── accident_weather_correlation.sql
│       │   └── marts/
│       │       ├── mart_realtime_traffic_dashboard.sql
│       │       ├── mart_traffic_safety_dashboard.sql
│       │       └── fct_weather_current.sql
│       ├── tests/
│       ├── schema.yml
│       ├── dbt_project.yml
│       └── profiles.yml
├── data/
│   └── US_Accidents_March23.csv  (Download from Google Drive link)
├── docker-compose.yaml
├── Dockerfile
└── README.md
```

**Note:** The `US_Accidents_March23.csv` file is not included in the repository due to its large size (7.7M+ records). Download it from the Google Drive link provided in the Prerequisites section.

---

## 🗄️ Database Schema Design

### Snowflake Schema Structure

#### RAW_ARCHIVE Schema (Historical Data)
- `TRAFFIC_ACCIDENTS_RAW` - 7.7M cleansed Kaggle records
- `TRAFFIC_ACCIDENTS_HISTORICAL` - Transformed historical accidents
- `WEATHER_CALIFORNIA_HISTORICAL_RAW` - Hourly weather history (2016-2023)
- `WEATHER_CALIFORNIA_HISTORICAL_ETL` - Processed historical weather

#### RAW Schema (Real-Time Data)
- `TRAFFIC_FLOW_RAW` - Live traffic flow metrics
- `TRAFFIC_FLOW_REALTIME` - Real-time traffic flow (30-min intervals)
- `TRAFFIC_INCIDENTS_RAW` - Active incidents
- `TRAFFIC_INCIDENTS_REALTIME` - Real-time incidents
- `WEATHER_CALIFORNIA_REALTIME_RAW` - Current weather conditions
- `WEATHER_CALIFORNIA_REALTIME_ETL` - Processed real-time weather

#### ANALYTICS Schema (dbt Models)
- **Staging Models:**
  - `stg_traffic_flow_realtime` - Cleaned traffic flow data
  - `stg_traffic_accidents` - Standardized accident records
  - `stg_weather_realtime` - Cleaned current weather
  - `stg_weather_historical` - Standardized historical weather

- **Analytics Models:**
  - `accident_weather_correlation` - Joined traffic and weather data

- **Mart Models:**
  - `mart_realtime_traffic_dashboard` - Real-time traffic metrics
  - `mart_traffic_safety_dashboard` - Safety analytics
  - `fct_weather_current` - Current weather fact table with regional comparisons

#### ML_PREDICTIONS Schema
- `ACCIDENT_FORECAST_FINAL_REALTIME` - ML-generated risk forecasts
- `ACCIDENT_TRAINING_VIEW_ENRICHED` - Training data view

---

## 🔧 dbt Transformation Architecture

### Layer 1: RAW Sources (Snowflake Tables)

**Real-time Schema (RAW)**
- TRAFFIC_FLOW_RAW
- TRAFFIC_FLOW_REALTIME
- TRAFFIC_INCIDENTS_RAW
- TRAFFIC_INCIDENTS_REALTIME
- WEATHER_CALIFORNIA_REALTIME_RAW
- WEATHER_CALIFORNIA_REALTIME_ETL

**Archive Schema (RAW_ARCHIVE)**
- TRAFFIC_ACCIDENTS_RAW
- TRAFFIC_ACCIDENTS_HISTORICAL
- WEATHER_CALIFORNIA_HISTORICAL_RAW
- WEATHER_CALIFORNIA_HISTORICAL_ETL

### Layer 2: Staging (Clean & Standardize)

**Models** (materialized: view)
- stg_traffic_flow_realtime
- stg_traffic_accidents
- stg_weather_realtime
- stg_weather_historical

**Transformations:**
- Parse timestamps
- Add categories
- Calculate metrics
- Filter invalid data

### Layer 3: Analytics (Join & Calculate)

**Model** (materialized: table)
- accident_weather_correlation

**Complex Logic:**
- JOIN accidents ← weather
- Match: city, date, hour
- Calculate risk scores
- Flag hazard conditions

**Risk Score Formula:**
```
Risk_Score = severity(3) + poor_visibility(2) + 
             heavy_rain(2) + snow(2) + 
             freezing(1) + high_wind(1)
```

### Layer 4: Marts (Dashboard Ready)

**Models** (materialized: table)
- mart_realtime_traffic_dashboard
- mart_traffic_safety_dashboard
- fct_weather_current

**Aggregations:**
- City-level summaries
- Time pattern analysis
- Weather correlations
- High-risk locations

**Output Metrics:**
- accident_count
- avg_risk_score
- congestion_level
- weather_related_count
- comfort_index
- regional_comparisons
- state_rankings

---

## ⚙️ Implementation Details

### ETL Pipelines (Python-Based Transformation)

#### 1. Traffic Historical ETL
- **File:** `traffic_historical_etl.py`
- **Purpose:** Process 7.7M Kaggle dataset
- **Strategy:** Full refresh, chunk processing (10,000 records/chunk)
- **Transformations:**
  - Column standardization (snake_case)
  - Null handling (temperature defaults to 70°F)
  - Coordinate validation (Lat -90 to 90)
  - Severity validation (1-4)

#### 2. Realtime Traffic ETL
- **File:** `realtime_traffic_etl.py`
- **Schedule:** Every 30 minutes
- **Transformations:**
  - Calculate speed_reduction_pct in Python
  - Parse GeoJSON geometry
  - Map severity levels (MINOR, MODERATE, MAJOR, CRITICAL)

#### 3. Weather Historical ETL
- **File:** `weather_historical_etl.py`
- **Transformations:**
  - Calculate heat_index_f (Temp > 80°F)
  - Add time dimensions (day_of_week, month, year)
  - Map WMO codes to readable conditions

#### 4. Weather Realtime ETL
- **File:** `weather_realtime_etl.py`
- **Schedule:** Every 30 minutes
- **Transformations:**
  - Calculate wind chill (Temp < 50°F, Wind > 3 mph)
  - Classify comfort_level
  - Set hazard flags (is_extreme_heat, is_high_wind, is_stormy)

### ELT Pipelines (dbt-Based Transformation)

#### 1. Traffic Historical ELT
- **File:** `traffic_historical_elt.py`
- **Flow:** Load raw → dbt staging → dbt analytics → dbt marts
- **Dependency:** Runs after weather historical ELT

#### 2. Realtime Traffic ELT
- **File:** `realtime_traffic_elt.py`
- **Schedule:** Every 30 minutes
- **Flow:** API → Raw tables → dbt transformations → Dashboard marts

#### 3. Weather Historical ELT
- **File:** `weather_historical_elt.py`
- **Flow:** API → Raw → dbt full pipeline (staging → integrated → marts)

#### 4. Weather Realtime ELT
- **File:** `weather_realtime_elt.py`
- **Schedule:** Every 30 minutes
- **Flow:** API → Raw → dbt staging → dbt test → Marts

### Machine Learning Pipeline

**File:** `ml_traffic_accident_forecast_realtime.py`

**Methodology:**
- Uses Snowflake Cortex ML (SNOWFLAKE.ML.FORECAST)
- Trains on 2-year history view: `ACCIDENT_TRAINING_VIEW_ENRICHED`

**Features:**
- **Target:** Daily accident count per city
- **Temporal:** day_of_week, month, is_weekend
- **Meteorological:** precipitation, wind_speed, freezing_temp flags

**Risk Multiplier Logic:**
- +15% Risk: Active precipitation (>0.1 in)
- +10% Risk: High winds (>25 mph)
- +20% Risk: Freezing temperatures (<32°F)
- +12% Risk: Real-time congestion >30%

**Risk Categories:**
- CRITICAL: ≥ 50 forecasted accidents
- HIGH: ≥ 30 forecasted accidents
- MEDIUM: ≥ 15 forecasted accidents
- LOW: < 15 forecasted accidents

---

## ☁️ Snowflake Configuration

### Account Details
```
Account: [Your Snowflake Account]
Database: [Your Database Name]
Warehouse: [Your Warehouse Name]
```

### Required Schemas
```sql
-- Create schemas
CREATE SCHEMA IF NOT EXISTS [YOUR_DATABASE].RAW;
CREATE SCHEMA IF NOT EXISTS [YOUR_DATABASE].RAW_ARCHIVE;
CREATE SCHEMA IF NOT EXISTS [YOUR_DATABASE].ANALYTICS;
CREATE SCHEMA IF NOT EXISTS [YOUR_DATABASE].ML_PREDICTIONS;
```

### Grant Permissions
```sql
GRANT USAGE ON DATABASE [YOUR_DATABASE] TO ROLE your_role;
GRANT USAGE ON SCHEMA [YOUR_DATABASE].RAW TO ROLE your_role;
GRANT USAGE ON SCHEMA [YOUR_DATABASE].RAW_ARCHIVE TO ROLE your_role;
GRANT USAGE ON SCHEMA [YOUR_DATABASE].ANALYTICS TO ROLE your_role;
GRANT CREATE TABLE ON SCHEMA [YOUR_DATABASE].ANALYTICS TO ROLE your_role;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA [YOUR_DATABASE].RAW TO ROLE your_role;
```

---

## ▶️ How to Run the Project

### Prerequisites

- Docker & Docker Compose
- Snowflake account with appropriate permissions
- TomTom API key
- Open-Meteo API access (free)
- US Accidents dataset (download from link below)

**Download US Accidents Dataset:**
Due to the large file size (7.7M+ records), the dataset is hosted on Google Drive:
- **Dataset Link:** https://drive.google.com/file/d/1U3u8QYzLjnEaSurtZfSAS_oh9AT2Mn8X/edit
- Download and place the CSV file in the `data/` directory as `US_Accidents_March23.csv`

### 1. Clone Repository

```bash
git clone [repository-url]
cd urban-traffic-weather-analysis
```

### 2. Environment Setup

Create `.env` file:
```bash
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=[YOUR_DATABASE]
SNOWFLAKE_WAREHOUSE=[YOUR_WAREHOUSE]
SNOWFLAKE_ROLE=your_role

# APIs
TOMTOM_API_KEY=your_tomtom_key
OPEN_METEO_API_URL=https://api.open-meteo.com/v1
```

### 3. Start Airflow

```bash
docker-compose up -d
```

### 4. Access Airflow UI

Navigate to: `http://localhost:8080`

**Login credentials:**
- Username: `airflow`
- Password: `airflow`

### 5. Add Airflow Variables

In Airflow UI → Admin → Variables:
```
california_cities = ["Los Angeles", "San Francisco", "San Diego", ...]
lookback_days = 180
chunk_size = 10000
```

### 6. Add Snowflake Connection

In Airflow UI → Admin → Connections:
- **Conn ID:** `snowflake_conn`
- **Conn Type:** Snowflake
- **Account:** your_account
- **User:** your_username
- **Password:** your_password
- **Warehouse:** [YOUR_WAREHOUSE]
- **Database:** [YOUR_DATABASE]
- **Schema:** RAW
- **Role:** your_role

### 7. Configure dbt Profile

Update `dbt/traffic_weather_analytics/profiles.yml`:
```yaml
traffic_weather_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_username
      password: your_password
      role: your_role
      database: [YOUR_DATABASE]
      warehouse: [YOUR_WAREHOUSE]
      schema: ANALYTICS
      threads: 4
```

### 8. Run Historical Data Pipelines

**Execute in this order:**

1. **Weather Historical ELT:**
   - Trigger: `Weather_California_Historical_ELT_DBT_Meteo`
   - This backfills 2016-2023 weather data

2. **Traffic Historical ELT:**
   - Trigger: `Traffic_Historical_ELT`
   - This processes the 7.7M accident records

### 9. Enable Real-Time Pipelines

Enable and unpause these DAGs (they run every 30 minutes):
- `Realtime_Traffic_ETL`
- `Realtime_Traffic_ELT`
- `Weather_Realtime_ETL`
- `Weather_Realtime_ELT`

### 10. Enable ML Pipeline

- Trigger: `ml_traffic_accident_forecast_realtime`
- This generates 7-day forecasts with risk adjustments

### 11. Validate in Snowflake

```sql
-- Check raw historical data
SELECT * FROM [YOUR_DATABASE].RAW_ARCHIVE.TRAFFIC_ACCIDENTS_HISTORICAL
LIMIT 100;

-- Check real-time traffic
SELECT * FROM [YOUR_DATABASE].RAW.TRAFFIC_FLOW_REALTIME
ORDER BY timestamp DESC
LIMIT 100;

-- Check analytics correlation
SELECT * FROM [YOUR_DATABASE].ANALYTICS.ACCIDENT_WEATHER_CORRELATION
LIMIT 100;

-- Check dashboard marts
SELECT * FROM [YOUR_DATABASE].ANALYTICS.MART_REALTIME_TRAFFIC_DASHBOARD
LIMIT 100;

-- Check ML predictions
SELECT * FROM [YOUR_DATABASE].ML_PREDICTIONS.ACCIDENT_FORECAST_FINAL_REALTIME
ORDER BY forecast_date DESC
LIMIT 100;
```

### 12. Set Up Preset Dashboards

**Connect to Snowflake:**
- Account: your_account
- Database: [YOUR_DATABASE]
- Schema: ANALYTICS
- Warehouse: [YOUR_WAREHOUSE]

**Create Datasets:**
- `MART_REALTIME_TRAFFIC_DASHBOARD`
- `MART_TRAFFIC_SAFETY_DASHBOARD`
- `FCT_WEATHER_CURRENT`
- `ACCIDENT_WEATHER_CORRELATION`

**Build Dashboards:**

1. **California Traffic Monitor - Live**
   - Live traffic flow map
   - Congestion metrics gauge
   - Top cities by congestion
   - Speed vs. congestion scatter plot

2. **California Weather Analytics - Live**
   - Current conditions metrics
   - Weather distribution donut chart
   - Precipitation heatmap
   - Regional comparison bar charts

3. **Accident-Weather Correlation**
   - Accident hotspots heatmap
   - Safety scores by time of day
   - Accident trend over time
   - Risk categorization pie chart

4. **Proactive Traffic Risk Management**
   - Accidents by risk level
   - Risk level distribution
   - Traffic accident risk assessment by city
   - 7-day forecast timeline

---

## 📊 Key SQL Queries

### Staging Model Example

```sql
-- models/staging/stg_traffic_flow_realtime.sql
SELECT
    flow_id,
    city,
    current_speed_mph,
    free_flow_speed_mph,
    timestamp_utc,
    ROUND((free_flow_speed_mph - current_speed_mph) / 
          free_flow_speed_mph * 100, 2) AS speed_reduction_pct,
    CASE 
        WHEN speed_reduction_pct > 50 THEN 'Heavy'
        WHEN speed_reduction_pct > 25 THEN 'Moderate'
        ELSE 'Light'
    END AS congestion_level
FROM {{ source('raw', 'traffic_flow_realtime') }}
WHERE timestamp_utc IS NOT NULL
```

### Analytics Model Example

```sql
-- models/analytics/accident_weather_correlation.sql
SELECT
    a.accident_id,
    a.city,
    a.severity,
    a.timestamp AS accident_timestamp,
    w.temperature_f,
    w.precipitation_in,
    w.wind_speed_mph,
    w.visibility_mi,
    -- Risk Score Calculation
    (a.severity * 3) + 
    (CASE WHEN w.visibility_mi < 1 THEN 2 ELSE 0 END) +
    (CASE WHEN w.precipitation_in > 0.5 THEN 2 ELSE 0 END) +
    (CASE WHEN w.snowfall_in > 0 THEN 2 ELSE 0 END) +
    (CASE WHEN w.temperature_f < 32 THEN 1 ELSE 0 END) +
    (CASE WHEN w.wind_speed_mph > 25 THEN 1 ELSE 0 END) AS risk_score
FROM {{ ref('stg_traffic_accidents') }} a
LEFT JOIN {{ ref('stg_weather_historical') }} w
    ON a.city = w.city
    AND DATE(a.timestamp) = DATE(w.timestamp)
    AND HOUR(a.timestamp) = HOUR(w.timestamp)
```

### Mart Model Example

```sql
-- models/marts/mart_realtime_traffic_dashboard.sql
SELECT
    city,
    region,
    AVG(current_speed_mph) AS avg_speed,
    AVG(speed_reduction_pct) AS avg_congestion_score,
    COUNT(DISTINCT flow_id) AS monitored_segments,
    MAX(timestamp_utc) AS last_updated,
    CASE
        WHEN AVG(speed_reduction_pct) > 50 THEN 'Heavy'
        WHEN AVG(speed_reduction_pct) > 25 THEN 'Moderate'
        ELSE 'Light'
    END AS overall_congestion_level
FROM {{ ref('stg_traffic_flow_realtime') }}
WHERE timestamp_utc >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY city, region
```

---

## 🐳 Docker Configuration

### Dockerfile

```dockerfile
FROM apache/airflow:2.10.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc g++ python3-dev libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake==5.7.0 \
    snowflake-connector-python==3.12.1 \
    dbt-core==1.8.8 \
    dbt-snowflake==1.8.4 \
    pandas==2.2.0 \
    requests==2.31.0
```

### docker-compose.yaml Key Configuration

```yaml
services:
  airflow:
    build: .
    platform: linux/amd64  # For M1/M2/M3/M4 Mac compatibility
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./dbt:/opt/airflow/dbt
      - ./data:/opt/airflow/data
```

---

## 🚀 Troubleshooting

### Issue: dbt command not found
**Solution:** Rebuild Docker image
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Issue: Snowflake connection fails
**Solution:** Test connection in Airflow UI and verify credentials

### Issue: API rate limits exceeded
**Solution:** Adjust schedule intervals in DAG files:
```python
schedule_interval='*/30 * * * *'  # Every 30 minutes
```

### Issue: Out of memory during historical load
**Solution:** Reduce chunk size in ETL script:
```python
chunk_size = 5000  # Reduce from 10000
```

### Issue: dbt models fail
**Solution:** Debug dbt in Docker container:
```bash
docker-compose exec airflow bash
cd /opt/airflow/dbt/traffic_weather_analytics
dbt debug
dbt run --select model_name
```

---

## 📈 Project Statistics

- **Total Data Points:** 7.7+ million historical records
- **Real-Time Updates:** Every 30 minutes
- **Cities Monitored:** 50+ major California cities
- **Time Range:** 2016-2023 (historical) + real-time
- **dbt Models:** 8 total (4 staging, 1 analytics, 3 marts)
- **Airflow DAGs:** 9 pipelines
- **Dashboards:** 4 interactive visualizations

---

## 📊 Key Findings

1. **Hyper-Localized Risk:** Los Angeles shows 89k impact score vs Sacramento's 2k
2. **The Fog Factor:** 52.73% of monitored areas experience fog, correlating with medium congestion
3. **Temporal Hotspots:** Tuesdays and Wednesdays show peak accident times (not Fridays as commonly assumed)
4. **Weather Impact:** Fog is a more persistent daily threat than rain in coastal cities

---

## 🎓 Lessons Learned

### Technical
- **Hybrid Architecture:** ETL for complex calculations (Heat Index) in Python; ELT for massive joins in Snowflake
- **Data Quality:** Rigorous null handling and validation essential for ML accuracy
- **Scalability:** Time-based partitioning necessary for 7.7M+ record queries

### Recommendations
- Dynamic resource deployment based on risk multipliers
- Infrastructure investments in weather-responsive signage for fog-prone areas
- Integration of critical risk alerts into public-facing apps

---

## 📚 Future Enhancements

1. **Geographic Expansion:** Scale to regional corridors and smaller municipalities
2. **Public Transport Integration:** Analyze modal shifts during adverse weather
3. **Predictive Routing:** Real-time route optimization based on forecasts
4. **Social Media Integration:** Incorporate real-time incident reports from Twitter/Waze

---

## 📖 References

[1] "US Accidents (2016 - 2023)," Kaggle. https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

[2] "Open-Meteo Weather API," Open-Meteo. https://open-meteo.com/

[3] "TomTom Traffic API," TomTom Developers. https://developer.tomtom.com/traffic-api

[4] Snowflake Documentation, "Machine Learning & Forecasting." https://docs.snowflake.com/en/user-guide/ml-functions

---

## 👥 Author

**Ritika Mukesh Neema**  
San Jose State University  
DATA 226 - Fall 2025

---

## 📝 License

This project is for educational purposes as part of DATA 226 coursework.

---

**Last Updated:** December 2025
