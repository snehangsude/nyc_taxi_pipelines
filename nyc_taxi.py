"""
Module Name: nyc_taxi.py
Description: This module contains functions for extracting, transforming, and loading NYC taxi data. 
Link: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page 
Author: Snehangsu De
Date: Feb 26, 2024
License: MIT License
"""

import os
import sys
import dlt
import logging
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dlt.sources.helpers import requests
from tenacity import retry, stop_after_attempt, wait_fixed

parser = argparse.ArgumentParser(description="Arguments for data processing process.")
parser.add_argument("--year", type=str, default=2023, required=True, help="Default value for year to ingest data for.")
parser.add_argument("--taxi", type=str, default='G', required=True, help="Value of taxi type.'G' for green.'Y' for yellow.'H' for hire.")
args = parser.parse_args()

# Create .logs folder if it doesn't exist
if not os.path.exists(dlt.config["runtime.logs_path"].split("/")[0]):
    os.makedirs(dlt.config["runtime.logs_path"].split("/")[0])


logging.basicConfig(
    filename=dlt.config["runtime.logs_path"] + f"{args.taxi}_nyc_taxi_{args.year}.log",
    filemode= 'w',
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


ORIGIN_URL = dlt.config["runtime.origin_url"]
YM_PAIRS= [(args.year, month) for month in range(1, 13)]



@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def fetch_data(url):
    """
    Retry decorator to fetch data from a given URL.
    Args:
        url (str): The URL to fetch data from.
    Returns:
        requests.Response: The response object.
    Raises:
        requests.exceptions.RequestException: If an error occurs during the HTTP request.
    """
    response = requests.get(url)
    response.raise_for_status()
    return response


@dlt.source
def nyc_taxi_source(taxi):
    """
    Data source function for NYC taxi data.
    Yields:
        callable: Functions to extract different types of NYC taxi data.
    """
    if taxi.upper() == 'G':
        return [nyc_taxi_green]
    elif taxi.upper() == 'Y':
        return [nyc_taxi_yellow]
    elif taxi.upper() == "H":
        return [nyc_taxi_hire]



@dlt.resource(
    table_name= 'yellow_taxi_trips',
    write_disposition="append", 
    schema_contract={"data_type": "evolve", "columns": "evolve"}
)
def nyc_taxi_yellow():
    """
    Extracts and transforms yellow taxi trip data.
    Yields:
        pyarrow.RecordBatch: Batches of yellow taxi trip data.
    """
    yellow_taxi_schema = pa.schema([
        ('VendorID', pa.int64()),
        ('tpep_pickup_datetime', pa.timestamp('us')),
        ('tpep_dropoff_datetime', pa.timestamp('us')),
        ('passenger_count', pa.float64()),
        ('trip_distance', pa.float64()),
        ('RatecodeID', pa.float64()),
        ('store_and_fwd_flag', pa.string()),
        ('PULocationID', pa.int64()),
        ('DOLocationID', pa.int64()),
        ('payment_type', pa.int64()),
        ('fare_amount', pa.float64()),
        ('extra', pa.float64()),
        ('mta_tax', pa.float64()),
        ('tip_amount', pa.float64()),
        ('tolls_amount', pa.float64()),
        ('improvement_surcharge', pa.float64()),
        ('total_amount', pa.float64()),
        ('congestion_surcharge', pa.float64()),
        ('airport_fee', pa.float64())
    ])

    for year, months in YM_PAIRS:

        url = f'{ORIGIN_URL}/yellow_tripdata_{year}-{months:02d}.parquet'

        try:
            response = fetch_data(url)
            logging.info(f'SUCCESS: URL response gathered for {url}')
        except Exception as e:
            logging.error(f'CRITICAL: {e} response generated for {url}')
            sys.exit()

        table = pq.read_table(pa.BufferReader(response.content), schema=yellow_taxi_schema)
        yield table.to_batches(max_chunksize=65536)
        logging.info(f"Loaded Yellow Taxi data for Year: {year} & Month: {months:02d}")

    

@dlt.resource(
    table_name= 'green_taxi_trips',
    write_disposition="append", 
    schema_contract={"data_type": "evolve", "columns": "evolve"}
)
def nyc_taxi_green():
    """
    Extracts and transforms green taxi trip data.
    Yields:
        pyarrow.RecordBatch: Batches of green taxi trip data.
    """
    green_taxi_schema  = pa.schema([
        ('VendorID', pa.int64()),            
        ('lpep_pickup_datetime', pa.timestamp('us')),    
        ('lpep_dropoff_datetime', pa.timestamp('us')),   
        ('store_and_fwd_flag', pa.string()),      
        ('RatecodeID', pa.float64()),              
        ('PULocationID', pa.int64()),            
        ('DOLocationID', pa.int64()),            
        ('passenger_count', pa.float64()),         
        ('trip_distance', pa.float64()),           
        ('fare_amount', pa.float64()),             
        ('extra', pa.float64()),                   
        ('mta_tax', pa.float64()),                 
        ('tip_amount', pa.float64()),              
        ('tolls_amount', pa.float64()),            
        ('ehail_fee', pa.float64()),               
        ('improvement_surcharge', pa.float64()),   
        ('total_amount', pa.float64()),            
        ('payment_type', pa.float64()),            
        ('trip_type', pa.float64()),               
        ('congestion_surcharge', pa.float64())    
    ])
    
    for year, months in YM_PAIRS:

        url = f'{ORIGIN_URL}/green_tripdata_{year}-{months:02d}.parquet'

        try:
            response = fetch_data(url)
            logging.info(f'SUCCESS: URL response gathered for {url}')
        except Exception as e:
            logging.error(f'CRITICAL: {e} response generated for {url}')
            sys.exit()
        
        table = pq.read_table(pa.BufferReader(response.content), schema=green_taxi_schema)
        yield table.to_batches(max_chunksize=65536)
        logging.info(f"Loaded Green Taxi data for Year: {year} & Month: {months:02d}")



@dlt.resource(
    table_name= 'hire_taxi_trips',
    write_disposition="append", 
    schema_contract={"data_type": "evolve", "columns": "evolve"}
)
def nyc_taxi_hire():
    """
    Extracts and transforms hire taxi trip data.
    Yields:
        pyarrow.RecordBatch: Batches of hire taxi trip data.
    """    
    fhvhv_taxi_schema = pa.schema([
        ('hvfhs_license_num', pa.string()),    
        ('dispatching_base_num', pa.string()), 
        ('originating_base_num', pa.string()), 
        ('request_datetime', pa.timestamp('us')),        
        ('on_scene_datetime', pa.timestamp('us')),       
        ('pickup_datetime', pa.timestamp('us')),         
        ('dropoff_datetime', pa.timestamp('us')),        
        ('PULocationID', pa.int64()),          
        ('DOLocationID', pa.int64()),          
        ('trip_miles', pa.float64()),          
        ('trip_time', pa.float64()),             
        ('base_passenger_fare', pa.float64()), 
        ('tolls', pa.float64()),               
        ('bcf', pa.float64()),                 
        ('sales_tax', pa.float64()),           
        ('congestion_surcharge', pa.float64()),
        ('airport_fee', pa.float64()),         
        ('tips', pa.float64()),                
        ('driver_pay', pa.float64()),          
        ('shared_request_flag', pa.string()),  
        ('shared_match_flag', pa.string()),    
        ('access_a_ride_flag', pa.string()),   
        ('wav_request_flag', pa.string()),     
        ('wav_match_flag', pa.string()),       
    ])

    for year, months in YM_PAIRS:
        
        url = f'{ORIGIN_URL}/fhvhv_tripdata_{year}-{months:02d}.parquet'

        try:
            response = fetch_data(url)
            logging.info(f'SUCCESS: URL response gathered for {url}')
        except Exception as e:
            logging.error(f'CRITICAL: {e} response generated for {url}')
            sys.exit()
        
        table = pq.read_table(pa.BufferReader(response.content), schema=fhvhv_taxi_schema)
        yield table.to_batches(max_chunksize=65536)
        logging.info(f"Loaded Hire Taxi data for Year: {year} & Month: {months:02d}")


if __name__ == "__main__":
    time1 = datetime.now()
    pipeline = dlt.pipeline(
        export_schema_path=dlt.config["runtime.export_schema_path"],
        import_schema_path=dlt.config["runtime.import_schema_path"],
        pipeline_name=dlt.config["runtime.pipeline_name"],
        destination=dlt.config["runtime.destination"],
        dataset_name=dlt.config["runtime.dataset_name"],
        progress="log"
    )
    load_info = pipeline.run(nyc_taxi_source(args.taxi))
    
    logging.info(load_info)
    logging.info(f"Total time taken: {datetime.now() - time1}")

    