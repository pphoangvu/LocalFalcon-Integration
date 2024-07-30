import os
import sys
import pandas as pd
import requests

from sqlalchemy.sql import text
from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from typing import List, Dict

# Set Configuration
script_directory = os.path.dirname(os.path.abspath(__file__))
config_directory = os.path.join(script_directory, 'config.yaml')
utils_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(script_directory))
sys.path.insert(1, os.path.dirname(utils_directory))
from dcapiutils.dcutils import DCUtils

# Set configuration variables
utils = DCUtils(config_directory)
config = utils.config
env = config['APP']['ENVIRONMENT']
debug = config['APP']['DEBUG']

# Configure the Snowflake connection 
databaseName = config['APP']['PREFECTBLOCKS']['SNOWFLAKE']['DATABASE'][env]
warehouse = config['APP']['PREFECTBLOCKS']['SNOWFLAKE']['WAREHOUSE'][env]
SNOWFLAKE_BLOCK = config['APP']['PREFECTBLOCKS']['SNOWFLAKE']['CONNECTOR_BLOCKNAME'][env]
SNOWFLAKE_AUTH = JSON.load(SNOWFLAKE_BLOCK)
username = SNOWFLAKE_AUTH.value["username"]
password = SNOWFLAKE_AUTH.value["password"]
account = SNOWFLAKE_AUTH.value["account_identifier"]
ROLE = SNOWFLAKE_AUTH.value["role"]

# Snowflake SQL connection
sql_connectionString = utils.snowflake_conn_url(databaseName, username, password, account, 'LOCAL_FALCON', warehouse, ROLE)
conn_engine = utils.snowflake_conn_engine(sql_connectionString)
Session = sessionmaker(bind=conn_engine)

# Configure APIKey
localfalcon_auth = config['APP']['PREFECTBLOCKS']['API']['API_BLOCKNAME'][env]
api_auth = JSON.load(localfalcon_auth)
api_key = api_auth.value["api_key"]
url = api_auth.value["url"]

# Expected keys in the API response
expected_keys = [
    "id", "checksum", "report_key", "timestamp", "date", "looker_date", 
    "type", "place_id", "location", "keyword", "lat", "lng", "grid_size", 
    "radius", "measurement", "data_points", "found_in", "arp", "atrp", 
    "solv", "image", "heatmap", "pdf", "public_url"
]

expected_location_keys = [
    "place_id", "name", "address", "lat", "lng", "rating", "reviews", "store_code"
]

def validate_api_response(data: Dict):
    """Validate the API response data."""
    if 'reports' not in data:
        raise ValueError("Missing 'reports' key in the API response")

    for report in data['reports']:
        # Check if all expected keys are in the report
        for key in expected_keys:
            if key not in report:
                raise ValueError(f"Missing key '{key}' in report ID {report.get('id', 'unknown')}")

        # Check if all expected location keys are in the location
        if 'location' not in report:
            raise ValueError(f"Missing 'location' key in report ID {report.get('id', 'unknown')}")
        
        for key in expected_location_keys:
            if key not in report['location']:
                raise ValueError(f"Missing key '{key}' in location of report ID {report.get('id', 'unknown')}")



# Function to fetch scan reports from the LocalFalcon API
@task
def fetch_scan_reports(api_key, limit, startdate="", enddate="", url=""):
    # API parameters
    params = {
        "api_key": api_key,
        "limit": limit,
        "start_date": startdate,
        "end_date": enddate
    }
    # Define list for reports and set token to None
    all_reports = []
    next_token = None

    # Loop through API for all records, limit is 100
    while True:
        if next_token:
            params["next_token"] = next_token

        try:
            # Sending GET request to the API
            response = requests.get(url, params=params)
            
            # If response status code is 200, retrieve next wave of data with next_token
            if response.status_code == 200:
                # Parsing JSON response
                data = response.json()
                validate_api_response(data['data'])
                reports = data.get("data", {}).get("reports", [])
                next_token = data.get("data", {}).get("next_token")
                utils.logger.info(f'Connected successfully to LocalFalcon API, retrieve next wave of records with next token')
                if not reports:
                    pass
                # Combine all wave of records into single list
                all_reports.extend(reports)
                # If next_token is not generated from API, end of data wave
                if not next_token:
                    utils.logger.info('Connected successfully to LocalFalcon API. Last wave of records has been completed.')
                    break
            elif response.status_code == 401:
                # Handling unauthorized access
                raise PermissionError("Unauthorized: Check if your API key is correct and properly authorized.")
            else:
                # Handling other HTTP status codes
                response.raise_for_status()
        
        except PermissionError as auth_err:
            raise Exception(f"Authentication Error: {auth_err}")
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"Error: {http_err}")
        except Exception as e:
            raise Exception(f"Error: {e}")

    return all_reports

# Function to process scan reports into a DataFrame
@task
def process_reports(reports):
    try:
        # Checking if reports are available
        if reports:
            # Creating a DataFrame from the reports
            utils.logger.info('Creating data frame for reports')
            df = pd.DataFrame(reports)
            # Extracting location details into separate columns
            location_details = df['location'].apply(pd.Series).add_prefix('location_')
            df = pd.concat([df, location_details], axis=1)
            # Dropping the 'location' column
            df.drop(columns=['location'], inplace=True)
            utils.logger.info('Adding load_status and load_date columns')
            df['load_status'] = 'insert'
            df['load_date'] = datetime.now()
            utils.logger.info('Processing data completed.')
            return df
        else:
            # Handling case when no reports are found
            raise ValueError("No reports found.")
    except KeyError as key_err:        
        raise Exception(f"Error: {key_err}")
    except Exception as e:
        raise Exception(f"Error: {e}")

# Function to insert data into Snowflake
@task
def insert_into_landing(reports):
    session = Session()
    try:
         # Truncate staging table
        utils.logger.info('Truncating staging table stg_all_scan_report')
        utils.truncate_table(session, 'stg_all_scan_report')
        session.close()
        utils.logger.info(f"existing records in stg_all_scan_report truncated...")
        utils.logger.info('Executed truncate statement')

        # Process reports into DataFrame
        processed_df = process_reports.fn(reports)
        
        # Write DataFrame to Snowflake staging table
        utils.logger.info('Inserting data into staging table stg_all_scan_report')
        processed_df.to_sql(con=session.connection(), name='stg_all_scan_report', schema='LOCAL_FALCON', if_exists='append', index=False, chunksize=16000)
        utils.logger.info('Executed insert statement')

        # Commit transaction
        session.commit()
        
    except Exception as e:
        # Rollback transaction in case of error
        session.rollback()
        raise Exception(f"Error: {e}")
    finally:
        # Close the session
        session.close()

# Function to upsert data into Snowflake
@task
def upsert_final_table():
    session = Session()
    try:
        merge_query = """
        MERGE INTO ALL_SCAN_REPORT AS Target
        USING STG_ALL_SCAN_REPORT AS Source
        ON Source.ID = Target.ID
        WHEN NOT MATCHED THEN
            INSERT (
                ID, 
                CHECKSUM, 
                REPORT_KEY, 
                TIMESTAMP, 
                DATE, 
                LOOKER_DATE, 
                TYPE, 
                PLACE_ID, 
                KEYWORD, 
                LAT, 
                LNG, 
                GRID_SIZE, 
                RADIUS, 
                MEASUREMENT, 
                DATA_POINTS, 
                FOUND_IN, 
                ARP, 
                ATRP, 
                SOLV, 
                IMAGE, 
                HEATMAP,
                PDF, 
                PUBLIC_URL, 
                LOCATION_PLACE_ID, 
                LOCATION_NAME, 
                LOCATION_ADDRESS, 
                LOCATION_LAT, 
                LOCATION_LNG, 
                LOCATION_RATING, 
                LOCATION_REVIEWS, 
                LOCATION_STORE_CODE,
                LOAD_STATUS,
                LOAD_DATE
            )
            VALUES (
                Source.ID, 
                Source.CHECKSUM, 
                Source.REPORT_KEY, 
                Source.TIMESTAMP, 
                Source.DATE, 
                Source.LOOKER_DATE, 
                Source.TYPE, 
                Source.PLACE_ID, 
                Source.KEYWORD, 
                Source.LAT, 
                Source.LNG, 
                Source.GRID_SIZE, 
                Source.RADIUS, 
                Source.MEASUREMENT, 
                Source.DATA_POINTS, 
                Source.FOUND_IN, 
                Source.ARP, 
                Source.ATRP, 
                Source.SOLV, 
                Source.IMAGE, 
                Source.HEATMAP,
                Source.PDF, 
                Source.PUBLIC_URL, 
                Source.LOCATION_PLACE_ID, 
                Source.LOCATION_NAME, 
                Source.LOCATION_ADDRESS, 
                Source.LOCATION_LAT, 
                Source.LOCATION_LNG, 
                Source.LOCATION_RATING, 
                Source.LOCATION_REVIEWS, 
                Source.LOCATION_STORE_CODE,
                'insert',
                GETDATE()
            )
        WHEN MATCHED AND 
            (
                (Target.CHECKSUM != Source.CHECKSUM OR (Target.CHECKSUM IS NULL AND Source.CHECKSUM IS NOT NULL) OR (Target.CHECKSUM IS NOT NULL AND Source.CHECKSUM IS NULL)) OR
                (Target.REPORT_KEY != Source.REPORT_KEY OR (Target.REPORT_KEY IS NULL AND Source.REPORT_KEY IS NOT NULL) OR (Target.REPORT_KEY IS NOT NULL AND Source.REPORT_KEY IS NULL)) OR
                (Target.TIMESTAMP != Source.TIMESTAMP OR (Target.TIMESTAMP IS NULL AND Source.TIMESTAMP IS NOT NULL) OR (Target.TIMESTAMP IS NOT NULL AND Source.TIMESTAMP IS NULL)) OR
                (Target.DATE != Source.DATE OR (Target.DATE IS NULL AND Source.DATE IS NOT NULL) OR (Target.DATE IS NOT NULL AND Source.DATE IS NULL)) OR
                (Target.LOOKER_DATE != Source.LOOKER_DATE OR (Target.LOOKER_DATE IS NULL AND Source.LOOKER_DATE IS NOT NULL) OR (Target.LOOKER_DATE IS NOT NULL AND Source.LOOKER_DATE IS NULL)) OR
                (Target.TYPE != Source.TYPE OR (Target.TYPE IS NULL AND Source.TYPE IS NOT NULL) OR (Target.TYPE IS NOT NULL AND Source.TYPE IS NULL)) OR
                (Target.PLACE_ID != Source.PLACE_ID OR (Target.PLACE_ID IS NULL AND Source.PLACE_ID IS NOT NULL) OR (Target.PLACE_ID IS NOT NULL AND Source.PLACE_ID IS NULL)) OR
                (Target.KEYWORD != Source.KEYWORD OR (Target.KEYWORD IS NULL AND Source.KEYWORD IS NOT NULL) OR (Target.KEYWORD IS NOT NULL AND Source.KEYWORD IS NULL)) OR
                (Target.LAT != Source.LAT OR (Target.LAT IS NULL AND Source.LAT IS NOT NULL) OR (Target.LAT IS NOT NULL AND Source.LAT IS NULL)) OR
                (Target.LNG != Source.LNG OR (Target.LNG IS NULL AND Source.LNG IS NOT NULL) OR (Target.LNG IS NOT NULL AND Source.LNG IS NULL)) OR
                (Target.GRID_SIZE != Source.GRID_SIZE OR (Target.GRID_SIZE IS NULL AND Source.GRID_SIZE IS NOT NULL) OR (Target.GRID_SIZE IS NOT NULL AND Source.GRID_SIZE IS NULL)) OR
                (Target.RADIUS != Source.RADIUS OR (Target.RADIUS IS NULL AND Source.RADIUS IS NOT NULL) OR (Target.RADIUS IS NOT NULL AND Source.RADIUS IS NULL)) OR
                (Target.MEASUREMENT != Source.MEASUREMENT OR (Target.MEASUREMENT IS NULL AND Source.MEASUREMENT IS NOT NULL) OR (Target.MEASUREMENT IS NOT NULL AND Source.MEASUREMENT IS NULL)) OR
                (Target.DATA_POINTS != Source.DATA_POINTS OR (Target.DATA_POINTS IS NULL AND Source.DATA_POINTS IS NOT NULL) OR (Target.DATA_POINTS IS NOT NULL AND Source.DATA_POINTS IS NULL)) OR
                (Target.FOUND_IN != Source.FOUND_IN OR (Target.FOUND_IN IS NULL AND Source.FOUND_IN IS NOT NULL) OR (Target.FOUND_IN IS NOT NULL AND Source.FOUND_IN IS NULL)) OR
                (Target.ARP != Source.ARP OR (Target.ARP IS NULL AND Source.ARP IS NOT NULL) OR (Target.ARP IS NOT NULL AND Source.ARP IS NULL)) OR
                (Target.ATRP != Source.ATRP OR (Target.ATRP IS NULL AND Source.ATRP IS NOT NULL) OR (Target.ATRP IS NOT NULL AND Source.ATRP IS NULL)) OR
                (Target.SOLV != Source.SOLV OR (Target.SOLV IS NULL AND Source.SOLV IS NOT NULL) OR (Target.SOLV IS NOT NULL AND Source.SOLV IS NULL)) OR
                (Target.IMAGE != Source.IMAGE OR (Target.IMAGE IS NULL AND Source.IMAGE IS NOT NULL) OR (Target.IMAGE IS NOT NULL AND Source.IMAGE IS NULL)) OR
                (Target.HEATMAP != Source.HEATMAP OR (Target.HEATMAP IS NULL AND Source.HEATMAP IS NOT NULL) OR (Target.HEATMAP IS NOT NULL AND Source.HEATMAP IS NULL)) OR
                (Target.PDF != Source.PDF OR (Target.PDF IS NULL AND Source.PDF IS NOT NULL) OR (Target.PDF IS NOT NULL AND Source.PDF IS NULL)) OR
                (Target.PUBLIC_URL != Source.PUBLIC_URL OR (Target.PUBLIC_URL IS NULL AND Source.PUBLIC_URL IS NOT NULL) OR (Target.PUBLIC_URL IS NOT NULL AND Source.PUBLIC_URL IS NULL)) OR
                (Target.LOCATION_PLACE_ID != Source.LOCATION_PLACE_ID OR (Target.LOCATION_PLACE_ID IS NULL AND Source.LOCATION_PLACE_ID IS NOT NULL) OR (Target.LOCATION_PLACE_ID IS NOT NULL AND Source.LOCATION_PLACE_ID IS NULL)) OR
                (Target.LOCATION_NAME != Source.LOCATION_NAME OR (Target.LOCATION_NAME IS NULL AND Source.LOCATION_NAME IS NOT NULL) OR (Target.LOCATION_NAME IS NOT NULL AND Source.LOCATION_NAME IS NULL)) OR
                (Target.LOCATION_ADDRESS != Source.LOCATION_ADDRESS OR (Target.LOCATION_ADDRESS IS NULL AND Source.LOCATION_ADDRESS IS NOT NULL) OR (Target.LOCATION_ADDRESS IS NOT NULL AND Source.LOCATION_ADDRESS IS NULL)) OR
                (Target.LOCATION_LAT != Source.LOCATION_LAT OR (Target.LOCATION_LAT IS NULL AND Source.LOCATION_LAT IS NOT NULL) OR (Target.LOCATION_LAT IS NOT NULL AND Source.LOCATION_LAT IS NULL)) OR
                (Target.LOCATION_LNG != Source.LOCATION_LNG OR (Target.LOCATION_LNG IS NULL AND Source.LOCATION_LNG IS NOT NULL) OR (Target.LOCATION_LNG IS NOT NULL AND Source.LOCATION_LNG IS NULL)) OR
                (Target.LOCATION_RATING != Source.LOCATION_RATING OR (Target.LOCATION_RATING IS NULL AND Source.LOCATION_RATING IS NOT NULL) OR (Target.LOCATION_RATING IS NOT NULL AND Source.LOCATION_RATING IS NULL)) OR
                (Target.LOCATION_REVIEWS != Source.LOCATION_REVIEWS OR (Target.LOCATION_REVIEWS IS NULL AND Source.LOCATION_REVIEWS IS NOT NULL) OR (Target.LOCATION_REVIEWS IS NOT NULL AND Source.LOCATION_REVIEWS IS NULL)) OR
                (Target.LOCATION_STORE_CODE != Source.LOCATION_STORE_CODE OR (Target.LOCATION_STORE_CODE IS NULL AND Source.LOCATION_STORE_CODE IS NOT NULL) OR (Target.LOCATION_STORE_CODE IS NOT NULL AND Source.LOCATION_STORE_CODE IS NULL))
            )
        THEN
            UPDATE SET
                Target.CHECKSUM = Source.CHECKSUM, 
                Target.REPORT_KEY = Source.REPORT_KEY, 
                Target.TIMESTAMP = Source.TIMESTAMP, 
                Target.DATE = Source.DATE, 
                Target.LOOKER_DATE = Source.LOOKER_DATE, 
                Target.TYPE = Source.TYPE, 
                Target.PLACE_ID = Source.PLACE_ID, 
                Target.KEYWORD = Source.KEYWORD, 
                Target.LAT = Source.LAT, 
                Target.LNG = Source.LNG, 
                Target.GRID_SIZE = Source.GRID_SIZE, 
                Target.RADIUS = Source.RADIUS, 
                Target.MEASUREMENT = Source.MEASUREMENT, 
                Target.DATA_POINTS = Source.DATA_POINTS, 
                Target.FOUND_IN = Source.FOUND_IN, 
                Target.ARP = Source.ARP, 
                Target.ATRP = Source.ATRP, 
                Target.SOLV = Source.SOLV, 
                Target.IMAGE = Source.IMAGE, 
                Target.HEATMAP = Source.HEATMAP,
                Target.PDF = Source.PDF, 
                Target.PUBLIC_URL = Source.PUBLIC_URL, 
                Target.LOCATION_PLACE_ID = Source.LOCATION_PLACE_ID, 
                Target.LOCATION_NAME = Source.LOCATION_NAME, 
                Target.LOCATION_ADDRESS = Source.LOCATION_ADDRESS,
                Target.LOCATION_LAT = Source.LOCATION_LAT, 
                Target.LOCATION_LNG = Source.LOCATION_LNG, 
                Target.LOCATION_RATING = Source.LOCATION_RATING, 
                Target.LOCATION_REVIEWS = Source.LOCATION_REVIEWS, 
                Target.LOCATION_STORE_CODE = Source.LOCATION_STORE_CODE,
                Target.LOAD_STATUS = 'update',
                Target.LOAD_DATE = GETDATE();
        """
        session.execute(text(merge_query))
        utils.logger.info('Executed merge statement')
        
        # Commit transaction
        session.commit()
        
    except Exception as e:
        # Rollback transaction in case of error
        session.rollback()
        raise Exception(f"Error: {e}")
    finally:
        # Close the session
        session.close()

# Main function
@flow(retries=0, retry_delay_seconds=3)
def localfalcon_main_flow(startdate='', enddate=''):
    try:
        # Declare logger
        utils.logger = get_run_logger()
        current_time = datetime.now()

        if startdate == '' and enddate == '':
            startdate = (current_time - timedelta(days=1)).strftime('%m/%d/%Y')
            enddate = current_time.strftime('%m/%d/%Y')

        utils.logger.info(f"Start Date: {startdate}")
        utils.logger.info(f"End Date: {enddate}")

        utils.logger.info('Start main flow...')
        try:
            # Fetch scan reports from API
            utils.logger.info('Calling function to LocalFalcon API...')
            reports_df = fetch_scan_reports(api_key, 100, startdate, enddate, url)
        except Exception as e:
            raise Exception(f"Error fetching scan reports: {e}")

        try:
            # Insert data into Snowflake
            utils.logger.info('Calling function to insert into staging Snowflake...')
            utils.logger.info('Running in '+ env + ' environment, snowflake database ' + databaseName + ' under warehouse ' + warehouse)
            insert_into_landing(reports_df)
        except Exception as e:
            raise Exception(f"Error inserting data into Snowflake staging:: {e}")

        try:
            # Upsert data into Snowflake
            utils.logger.info('Calling function to upsert into final Snowflake...')
            upsert_final_table()
            utils.logger.info('Data upserted successfully into Snowflake.')
        except Exception as e:
            raise Exception(f"Error upserting data into Snowflake:: {e}")

    except Exception as e:
        raise Exception(f"An unexpected error occurred in the main flow:: {e}")

if __name__ == "__main__":
    localfalcon_main_flow('','')
