from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Variable.get("dbt_project_dir", default_var="/opt/airflow/dbt")

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

TOMTOM_API_KEY = Variable.get("TOMTOM_KEY")
USER_DB = Variable.get("USER_DB")
USER_WH = Variable.get("USER_WH")

CALIFORNIA_CITIES = [
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
def extract_and_load_raw_traffic_flow():
    
    traffic_flows = []
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    for city in CALIFORNIA_CITIES:
        try:
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
            params = {
                "key": TOMTOM_API_KEY,
                "point": f"{city['lat']},{city['lon']}",
                "unit": "MPH"
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                flow_data = data.get('flowSegmentData', {})
                
                flow_record = {
                    'flow_id': f"{city['name']}_{timestamp.replace(' ', '_').replace(':', '')}",
                    'city': city['name'],
                    'state': 'CA',
                    'timestamp': timestamp,
                    'latitude': city['lat'],
                    'longitude': city['lon'],
                    'current_speed': flow_data.get('currentSpeed'),
                    'free_flow_speed': flow_data.get('freeFlowSpeed'),
                    'current_travel_time': flow_data.get('currentTravelTime'),
                    'free_flow_travel_time': flow_data.get('freeFlowTravelTime'),
                    'confidence': flow_data.get('confidence'),
                    'road_closure': flow_data.get('roadClosure', False),
                    'road_name': flow_data.get('frc', 'Unknown'),
                    'raw_json': json.dumps(flow_data),
                }
                
                traffic_flows.append(flow_record)
            else:
                logger.warning(f"Failed to fetch traffic for {city['name']}: {response.status_code}")
        except Exception as e:
            logger.error(f"Error extracting traffic for {city['name']}: {str(e)}")
            continue
    
    if not traffic_flows:
        logger.warning("No traffic flow records to load")
        return {"count": 0}
    
    cur = return_snowflake_conn()
    
    try:
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute(f"USE WAREHOUSE {USER_WH}")
        cur.execute("DROP TABLE IF EXISTS TRAFFIC_FLOW_RAW CASCADE")
        
        cur.execute("""
            CREATE TABLE TRAFFIC_FLOW_RAW (
                flow_id VARCHAR(100) NOT NULL,
                city VARCHAR(100) NOT NULL,
                state VARCHAR(50),
                timestamp VARCHAR(50) NOT NULL,
                latitude FLOAT,
                longitude FLOAT,
                current_speed FLOAT,
                free_flow_speed FLOAT,
                current_travel_time NUMBER(10),
                free_flow_travel_time NUMBER(10),
                confidence FLOAT,
                road_closure BOOLEAN,
                road_name VARCHAR(200),
                raw_json VARCHAR,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (flow_id, timestamp)
            )
        """)
        
        insert_sql = """
            INSERT INTO TRAFFIC_FLOW_RAW (
                flow_id, city, state, timestamp, latitude, longitude,
                current_speed, free_flow_speed, current_travel_time, 
                free_flow_travel_time, confidence, road_closure, road_name, raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = [
            (
                r['flow_id'], r['city'], r['state'], r['timestamp'],
                r['latitude'], r['longitude'], r['current_speed'],
                r['free_flow_speed'], r['current_travel_time'],
                r['free_flow_travel_time'], r['confidence'],
                r['road_closure'], r['road_name'], r['raw_json']
            )
            for r in traffic_flows
        ]
        
        cur.executemany(insert_sql, rows)
        return {"count": len(rows)}
    except Exception as e:
        raise
    finally:
        cur.close()

@task
def extract_and_load_raw_traffic_incidents():
    
    incidents = []
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    for city in CALIFORNIA_CITIES:
        try:
            url = "https://api.tomtom.com/traffic/services/5/incidentDetails"
            params = {
                "key": TOMTOM_API_KEY,
                "bbox": f"{city['lon']-0.1},{city['lat']-0.1},{city['lon']+0.1},{city['lat']+0.1}",
                "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code},startTime,endTime}}}",
                "language": "en-US"
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                incidents_data = data.get('incidents', [])
                
                for inc in incidents_data:
                    props = inc.get('properties', {})
                    geom = inc.get('geometry', {})
                    coords = geom.get('coordinates', [])
                    
                    if geom.get('type') == 'LineString' and coords and len(coords) > 0:
                        first_coord = coords[0]
                        lon = first_coord[0] if len(first_coord) > 0 else city['lon']
                        lat = first_coord[1] if len(first_coord) > 1 else city['lat']
                    elif geom.get('type') == 'Point' and coords and len(coords) >= 2:
                        lon = coords[0]
                        lat = coords[1]
                    else:
                        lon = city['lon']
                        lat = city['lat']
                    
                    incident_record = {
                        'incident_id': props.get('id', f"INC_{city['name']}_{len(incidents)}"),
                        'city': city['name'],
                        'state': 'CA',
                        'timestamp': timestamp,
                        'latitude': lat,
                        'longitude': lon,
                        'icon_category': props.get('iconCategory'),
                        'magnitude_of_delay': props.get('magnitudeOfDelay'),
                        'description': props.get('events', [{}])[0].get('description') if props.get('events') else None,
                        'start_time': props.get('startTime'),
                        'end_time': props.get('endTime'),
                        'raw_json': json.dumps(inc),
                    }
                    
                    incidents.append(incident_record)
        except Exception as e:
            logger.error(f"Error extracting incidents for {city['name']}: {str(e)}")
            continue
    
    if not incidents:
        return {"count": 0}
    
    cur = return_snowflake_conn()
    
    try:
        cur.execute("USE DATABASE USER_DB_JACKAL")
        cur.execute("USE SCHEMA RAW")
        cur.execute("DROP TABLE IF EXISTS TRAFFIC_INCIDENTS_RAW CASCADE")
        
        cur.execute("""
            CREATE TABLE TRAFFIC_INCIDENTS_RAW (
                incident_id VARCHAR(100) NOT NULL,
                city VARCHAR(100) NOT NULL,
                state VARCHAR(50),
                timestamp VARCHAR(50) NOT NULL,
                latitude FLOAT,
                longitude FLOAT,
                icon_category VARCHAR(50),
                magnitude_of_delay NUMBER(10),
                description VARCHAR(2000),
                start_time VARCHAR(50),
                end_time VARCHAR(50),
                raw_json VARCHAR,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (incident_id, timestamp)
            )
        """)
        
        insert_sql = """
            INSERT INTO TRAFFIC_INCIDENTS_RAW (
                incident_id, city, state, timestamp, latitude, longitude,
                icon_category, magnitude_of_delay, description,
                start_time, end_time, raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = [
            (
                inc['incident_id'], inc['city'], inc['state'], inc['timestamp'],
                inc['latitude'], inc['longitude'], inc['icon_category'],
                inc['magnitude_of_delay'], inc['description'],
                inc['start_time'], inc['end_time'], inc['raw_json']
            )
            for inc in incidents
        ]
        
        cur.executemany(insert_sql, rows)
        return {"count": len(rows)}
    except Exception as e:
        raise
    finally:
        cur.close()

with DAG(
    dag_id="Realtime_Traffic_ELT_DBT",
    start_date=datetime(2025, 1, 1),
    description="Real-time Traffic ELT: Extract, Load RAW traffic, Transform with dbt",
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["ELT", "realtime", "traffic", "raw", "dbt"],
) as dag:

    flow_task = extract_and_load_raw_traffic_flow()
    incidents_task = extract_and_load_raw_traffic_incidents()
    
    dbt_deps = BashOperator(
        task_id="dbt_install_dependencies",
        bash_command=f"cd {DBT_PROJECT_DIR} && rm -rf dbt_packages* && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging.traffic.stg_traffic_flow_realtime --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts.mart_realtime_traffic_dashboard --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    [flow_task, incidents_task] >> dbt_deps >> dbt_staging >> dbt_marts