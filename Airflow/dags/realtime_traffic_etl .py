from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def return_snowflake_conn():
    """Connect to Snowflake using Airflow connection ID."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

TOMTOM_API_KEY = Variable.get("TOMTOM_KEY")
USER_DB = Variable.get("USER_DB")
USER_WH = Variable.get("USER_WH")

CALIFORNIA_CITIES = [
    # MAJOR METROPOLITAN AREAS
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437, "region": "Southern California", "county": "Los Angeles"},
    {"name": "San Diego", "lat": 32.7157, "lon": -117.1611, "region": "Southern California", "county": "San Diego"},
    {"name": "San Jose", "lat": 37.3382, "lon": -121.8863, "region": "Bay Area", "county": "Santa Clara"},
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194, "region": "Bay Area", "county": "San Francisco"},
    {"name": "Fresno", "lat": 36.7378, "lon": -119.7871, "region": "Central Valley", "county": "Fresno"},
    {"name": "Sacramento", "lat": 38.5816, "lon": -121.4944, "region": "Central Valley", "county": "Sacramento"},
    {"name": "Long Beach", "lat": 33.7701, "lon": -118.1937, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Oakland", "lat": 37.8044, "lon": -122.2712, "region": "Bay Area", "county": "Alameda"},
    {"name": "Bakersfield", "lat": 35.3733, "lon": -119.0187, "region": "Central Valley", "county": "Kern"},
    {"name": "Anaheim", "lat": 33.8366, "lon": -117.9143, "region": "Southern California", "county": "Orange"},
    # SOUTHERN CALIFORNIA
    {"name": "Santa Ana", "lat": 33.7455, "lon": -117.8677, "region": "Southern California", "county": "Orange"},
    {"name": "Riverside", "lat": 33.9806, "lon": -117.3755, "region": "Southern California", "county": "Riverside"},
    {"name": "Irvine", "lat": 33.6846, "lon": -117.8265, "region": "Southern California", "county": "Orange"},
    {"name": "Chula Vista", "lat": 32.6401, "lon": -117.0842, "region": "Southern California", "county": "San Diego"},
    {"name": "San Bernardino", "lat": 34.1083, "lon": -117.2898, "region": "Southern California", "county": "San Bernardino"},
    {"name": "Fontana", "lat": 34.0922, "lon": -117.4350, "region": "Southern California", "county": "San Bernardino"},
    {"name": "Oxnard", "lat": 34.1975, "lon": -119.1771, "region": "Southern California", "county": "Ventura"},
    {"name": "Moreno Valley", "lat": 33.9425, "lon": -117.2297, "region": "Southern California", "county": "Riverside"},
    {"name": "Huntington Beach", "lat": 33.6603, "lon": -117.9992, "region": "Southern California", "county": "Orange"},
    {"name": "Glendale", "lat": 34.1425, "lon": -118.2551, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Santa Clarita", "lat": 34.3917, "lon": -118.5426, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Oceanside", "lat": 33.1959, "lon": -117.3795, "region": "Southern California", "county": "San Diego"},
    {"name": "Corona", "lat": 33.8753, "lon": -117.5664, "region": "Southern California", "county": "Riverside"},
    {"name": "Palmdale", "lat": 34.5794, "lon": -118.1165, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Pasadena", "lat": 34.1478, "lon": -118.1445, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Torrance", "lat": 33.8358, "lon": -118.3406, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Escondido", "lat": 33.1192, "lon": -117.0864, "region": "Southern California", "county": "San Diego"},
    # BAY AREA
    {"name": "Fremont", "lat": 37.5485, "lon": -121.9886, "region": "Bay Area", "county": "Alameda"},
    {"name": "Hayward", "lat": 37.6688, "lon": -122.0808, "region": "Bay Area", "county": "Alameda"},
    {"name": "Sunnyvale", "lat": 37.3688, "lon": -122.0363, "region": "Bay Area", "county": "Santa Clara"},
    {"name": "Santa Clara", "lat": 37.3541, "lon": -121.9552, "region": "Bay Area", "county": "Santa Clara"},
    {"name": "Berkeley", "lat": 37.8715, "lon": -122.2730, "region": "Bay Area", "county": "Alameda"},
    {"name": "Concord", "lat": 37.9780, "lon": -122.0311, "region": "Bay Area", "county": "Contra Costa"},
    {"name": "Vallejo", "lat": 38.1041, "lon": -122.2566, "region": "Bay Area", "county": "Solano"},
    {"name": "Santa Rosa", "lat": 38.4404, "lon": -122.7141, "region": "Bay Area", "county": "Sonoma"},
    {"name": "San Mateo", "lat": 37.5630, "lon": -122.3255, "region": "Bay Area", "county": "San Mateo"},
    {"name": "Daly City", "lat": 37.6879, "lon": -122.4702, "region": "Bay Area", "county": "San Mateo"},
    # CENTRAL VALLEY
    {"name": "Modesto", "lat": 37.6391, "lon": -120.9969, "region": "Central Valley", "county": "Stanislaus"},
    {"name": "Stockton", "lat": 37.9577, "lon": -121.2908, "region": "Central Valley", "county": "San Joaquin"},
    {"name": "Visalia", "lat": 36.3302, "lon": -119.2921, "region": "Central Valley", "county": "Tulare"},
    {"name": "Clovis", "lat": 36.8252, "lon": -119.7029, "region": "Central Valley", "county": "Fresno"},
    {"name": "Elk Grove", "lat": 38.4088, "lon": -121.3716, "region": "Central Valley", "county": "Sacramento"},
    {"name": "Roseville", "lat": 38.7521, "lon": -121.2880, "region": "Central Valley", "county": "Placer"},
    {"name": "Merced", "lat": 37.3022, "lon": -120.4830, "region": "Central Valley", "county": "Merced"},
    {"name": "Turlock", "lat": 37.4947, "lon": -120.8466, "region": "Central Valley", "county": "Stanislaus"},
    # CENTRAL COAST
    {"name": "Salinas", "lat": 36.6777, "lon": -121.6555, "region": "Central Coast", "county": "Monterey"},
    {"name": "Santa Barbara", "lat": 34.4208, "lon": -119.6982, "region": "Central Coast", "county": "Santa Barbara"},
    {"name": "Santa Cruz", "lat": 36.9741, "lon": -122.0308, "region": "Central Coast", "county": "Santa Cruz"},
    {"name": "San Luis Obispo", "lat": 35.2828, "lon": -120.6596, "region": "Central Coast", "county": "San Luis Obispo"},
    # NORTH REGIONS
    {"name": "Redding", "lat": 40.5865, "lon": -122.3917, "region": "North Valley", "county": "Shasta"},
    {"name": "Chico", "lat": 39.7285, "lon": -121.8375, "region": "North Valley", "county": "Butte"},
    {"name": "Eureka", "lat": 40.8021, "lon": -124.1637, "region": "North Coast", "county": "Humboldt"},
    # DESERT REGIONS
    {"name": "Palm Springs", "lat": 33.8303, "lon": -116.5453, "region": "Desert", "county": "Riverside"},
    {"name": "Indio", "lat": 33.7206, "lon": -116.2156, "region": "Desert", "county": "Riverside"},
    {"name": "Victorville", "lat": 34.5362, "lon": -117.2928, "region": "Desert", "county": "San Bernardino"},
]


@task
def test_tomtom_api():
    
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    params = {
        "key": TOMTOM_API_KEY,
        "point": "34.0522,-118.2437",  # Los Angeles
        "unit": "MPH"
    }
    
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Sample data: {data}")
            return {"status": "success", "code": 200}
        elif response.status_code == 403:
            return {"status": "forbidden", "code": 403}
        elif response.status_code == 429:
            return {"status": "rate_limited", "code": 429}
        elif response.status_code == 401:
            return {"status": "unauthorized", "code": 401}
        else:
            return {"status": "error", "code": response.status_code}
            
    except Exception as e:
        return {"status": "exception", "error": str(e)}

@task
def extract_transform_load_traffic_flow():

    traffic_flows = []
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for city in CALIFORNIA_CITIES:
        try:
            url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
            params = {
                "key": TOMTOM_API_KEY,
                "point": f"{city['lat']},{city['lon']}",
                "unit": "MPH",
            }

            response = requests.get(url, params=params, timeout=10)
            
            # Enhanced debugging for first city
            if city['name'] == 'Los Angeles':
                logger.info(f"irst request details:")

            
            if response.status_code == 200:
                data = response.json()
                flow_data = data.get("flowSegmentData", {})

                current_speed = flow_data.get("currentSpeed", 0)
                free_flow_speed = flow_data.get("freeFlowSpeed", 0)
                current_travel_time = flow_data.get("currentTravelTime", 0)
                free_flow_travel_time = flow_data.get("freeFlowTravelTime", 0)

                flow_record = {
                    "flow_id": f"{city['name']}_{timestamp.replace(' ', '_').replace(':', '')}",
                    "city": city["name"],
                    "state": "CA",
                    "road_name": flow_data.get("frc", "Unknown"),
                    "road_segment_id": f"{city['name']}_segment",
                    "timestamp": timestamp,
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "current_speed_mph": current_speed,
                    "free_flow_speed_mph": free_flow_speed,
                    "current_travel_time_sec": current_travel_time,
                    "free_flow_travel_time_sec": free_flow_travel_time,
                    "confidence_level": flow_data.get("confidence", 0.5),
                    "road_closure": flow_data.get("roadClosure", False),
                }

                if free_flow_speed > 0:
                    flow_record["speed_reduction_pct"] = round(
                        (free_flow_speed - current_speed) / free_flow_speed * 100, 2
                    )
                else:
                    flow_record["speed_reduction_pct"] = 0

                flow_record["delay_sec"] = current_travel_time - free_flow_travel_time

                traffic_flows.append(flow_record)
                
            else:
                
                logger.error(f"Error response: {response.text}")
        except Exception as e:
            logger.error(f"Error extracting traffic for {city['name']}: {str(e)}")
            continue

    if not traffic_flows:
        return {"count": 0}

    cur = return_snowflake_conn()
    try:
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute(f"USE WAREHOUSE {USER_WH}")

        cur.execute("DROP TABLE IF EXISTS TRAFFIC_FLOW_REALTIME")

        cur.execute(
            """
        CREATE TABLE TRAFFIC_FLOW_REALTIME (
            flow_id VARCHAR(100) NOT NULL,
            city VARCHAR(100) NOT NULL,
            state VARCHAR(50),
            road_name VARCHAR(200),
            road_segment_id VARCHAR(100),
            timestamp TIMESTAMP_NTZ NOT NULL,
            latitude FLOAT,
            longitude FLOAT,
            current_speed_mph FLOAT,
            free_flow_speed_mph FLOAT,
            speed_reduction_pct FLOAT,
            current_travel_time_sec NUMBER(10),
            free_flow_travel_time_sec NUMBER(10),
            delay_sec NUMBER(10),
            confidence_level FLOAT,
            road_closure BOOLEAN,
            PRIMARY KEY (flow_id, timestamp)
        )
        """
        )

        insert_sql = """
        INSERT INTO TRAFFIC_FLOW_REALTIME (
            flow_id, city, state, road_name, road_segment_id, timestamp, latitude, longitude,
            current_speed_mph, free_flow_speed_mph, speed_reduction_pct,
            current_travel_time_sec, free_flow_travel_time_sec, delay_sec,
            confidence_level, road_closure
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        rows = [
            (
                r["flow_id"],
                r["city"],
                r["state"],
                r["road_name"],
                r["road_segment_id"],
                r["timestamp"],
                r["latitude"],
                r["longitude"],
                r["current_speed_mph"],
                r["free_flow_speed_mph"],
                r["speed_reduction_pct"],
                r["current_travel_time_sec"],
                r["free_flow_travel_time_sec"],
                r["delay_sec"],
                r["confidence_level"],
                r["road_closure"],
            )
            for r in traffic_flows
        ]

        logger.info(f"Inserting {len(rows)} rows into TRAFFIC_FLOW_REALTIME...")
        cur.executemany(insert_sql, rows)

        return {"count": len(rows)}

    except Exception as e:
        raise
    finally:
        cur.close()

@task
def extract_transform_load_traffic_incidents():


    incidents = []
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for city in CALIFORNIA_CITIES:
        try:
            url = "https://api.tomtom.com/traffic/services/5/incidentDetails"
            params = {
                "key": TOMTOM_API_KEY,
                "bbox": f"{city['lon']-0.1},{city['lat']-0.1},{city['lon']+0.1},{city['lat']+0.1}",
                "fields": "{incidents{type,geometry{type,coordinates},properties{"
                "id,iconCategory,magnitudeOfDelay,events{description,code},"
                "startTime,endTime}}}",
                "language": "en-US",
            }

            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                incidents_data = data.get("incidents", [])

                for inc in incidents_data:
                    props = inc.get("properties", {})
                    coords = inc.get("geometry", {}).get(
                        "coordinates", [city["lon"], city["lat"]]
                    )

                    # Flatten coordinates to single lon/lat point
                    if isinstance(coords, list) and len(coords) > 0:
                        first = coords[0]
                        if isinstance(first, list) and len(first) >= 2:
                            lon = first[0]
                            lat = first[1]
                        else:
                            lon = first
                            lat = coords[1] if len(coords) > 1 else city["lat"]
                    else:
                        lon = city["lon"]
                        lat = city["lat"]

                    # Derive a 'street-like' label from first event description
                    events = props.get("events", [])
                    street = None
                    if isinstance(events, list) and events:
                        street = events[0].get("description")

                    magnitude = props.get("magnitudeOfDelay", 0)
                    if magnitude == 0:
                        severity = "MINOR"
                    elif magnitude == 1:
                        severity = "MODERATE"
                    elif magnitude == 2:
                        severity = "MAJOR"
                    else:
                        severity = "CRITICAL"

                    incident_record = {
                        "incident_id": props.get(
                            "id", f"INC_{city['name']}_{len(incidents)}"
                        ),
                        "city": city["name"],
                        "state": "CA",
                        "timestamp": timestamp,
                        "latitude": lat,
                        "longitude": lon,
                        "street": street,
                        "incident_type": props.get("iconCategory", "UNKNOWN"),
                        "severity": severity,
                        "description": events[0].get("description", "No description")
                        if events
                        else "No description",
                        "delay_minutes": props.get("delay", 0) / 60
                        if props.get("delay")
                        else 0,
                        "length_meters": 0,
                        "affected_road_count": 0,
                        "start_time": props.get("startTime", timestamp),
                        "end_time": props.get("endTime"),
                        "is_active": True,
                    }

                    incidents.append(incident_record)
            else:
                
                logger.error(f"Error response: {response.text}")
        except Exception as e:
            logger.error(f"Error extracting incidents for {city['name']}: {str(e)}")
            continue

    if not incidents:
        return {"count": 0}

    cur = return_snowflake_conn()
    try:
        cur.execute("USE DATABASE USER_DB_JACKAL")
        cur.execute("USE SCHEMA RAW")

        logger.info("Dropping TRAFFIC_INCIDENTS_REALTIME table if exists...")
        cur.execute("DROP TABLE IF EXISTS TRAFFIC_INCIDENTS_REALTIME")
        logger.info("Creating TRAFFIC_INCIDENTS_REALTIME table...")

        cur.execute(
            """
        CREATE TABLE TRAFFIC_INCIDENTS_REALTIME (
            incident_id VARCHAR(100) NOT NULL,
            city VARCHAR(100) NOT NULL,
            state VARCHAR(50),
            timestamp TIMESTAMP_NTZ NOT NULL,
            latitude FLOAT,
            longitude FLOAT,
            street VARCHAR(500),
            incident_type VARCHAR(50),
            severity VARCHAR(20),
            description VARCHAR(2000),
            delay_minutes NUMBER(10),
            length_meters NUMBER(10),
            affected_road_count NUMBER(5),
            start_time TIMESTAMP_NTZ,
            end_time TIMESTAMP_NTZ,
            is_active BOOLEAN,
            api_source VARCHAR(50),
            PRIMARY KEY (incident_id, timestamp)
        )
        """
        )

        cur.execute("DESCRIBE TABLE TRAFFIC_INCIDENTS_REALTIME")
        columns = cur.fetchall()

        insert_sql = """
        INSERT INTO TRAFFIC_INCIDENTS_REALTIME (
            incident_id, city, state, timestamp, latitude, longitude, street,
            incident_type, severity, description, delay_minutes,
            length_meters, affected_road_count, start_time, end_time, is_active,
            api_source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        rows = [
            (
                r["incident_id"],
                r["city"],
                r["state"],
                r["timestamp"],
                r["latitude"],
                r["longitude"],
                r["street"],
                r["incident_type"],
                r["severity"],
                r["description"],
                r["delay_minutes"],
                r["length_meters"],
                r["affected_road_count"],
                r["start_time"],
                r["end_time"],
                r["is_active"],
                "TomTom",
            )
            for r in incidents
        ]

        cur.executemany(insert_sql, rows)
        return {"count": len(rows)}

    except Exception as e:
        raise
    finally:
        cur.close()


with DAG(
    dag_id="Realtime_Traffic_ETL",
    start_date=datetime(2025, 1, 1),
    description="Real-time Traffic ETL: Extract from TomTom, Transform, Load to Snowflake",
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["ETL", "realtime", "traffic"],
) as dag:
    
    # Test API first
    test_task = test_tomtom_api()
    
    # Main ETL tasks
    flow_task = extract_transform_load_traffic_flow()
    incidents_task = extract_transform_load_traffic_incidents()
    
    test_task >> [flow_task, incidents_task]