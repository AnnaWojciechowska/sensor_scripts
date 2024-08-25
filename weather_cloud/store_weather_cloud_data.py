# !/usr/bin/env python3
#Anna Wojciechowska, Oslo, August 2025
#  Script to process data from weather cloud
#  The scripts reads csv "weather_cloud_data" directory and after successful read moved the file to "processed_data".



from influxdb import DataFrameClient

from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests.exceptions import Timeout, ConnectionError

from datetime import datetime

import pandas as pd
import numpy as np
import json

import sys
import os
import glob

import argparse

import traceback
import logging

from datetime import datetime as dt

def store_points(df, tags, fields):
    try:
        result = INFLUX_WRITE_CLIENT.write_points(df,'weather_cloud',tag_columns = tags, field_columns = fields,protocol='line')
        if result:
            LOGGER.info(f" {df.shape[0]} data points written.")
            return (result,df.shape[0])
        else:  
            LOGGER.error(f"No data written.")
            return (result,0)
    except ConnectionError:
            # thrown if influxdb is down or (spelling) errors in connection configuration
            LOGGER.error("ConnectionError, check connection setting and if influxdb is up: 'systemctl status influxdb'.")
            LOGGER.error(traceback.format_exc())
            sys.exit(1)
    except Timeout:
            LOGGER.error("Timeout, check influx timeout setting and network connection.")
            LOGGER.error(traceback.format_exc())
    except InfluxDBClientError:
            LOGGER.error("InfluxDBClientError, check if database exist in influx: 'SHOW DATABASES'.")
            LOGGER.error(traceback.format_exc())
            sys.exit(1)
    except InfluxDBServerError:
            LOGGER.error("InfluxDBServerError")
            LOGGER.error(traceback.format_exc())
            sys.exit(1)
  
def set_up_log(log_dir, log_filename):
    # to do create log when does not exist even if folder exists
    ''' creates a log in script running directory '''
    script_run_dir = os.getcwd()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    #check if log dir exist, create both log dir and log file if necessary
    if not os.path.isdir(os.path.join(script_run_dir, log_dir)):
        os.mkdir(os.path.join(script_run_dir, log_dir))
    file_handler = logging.FileHandler(os.path.join(script_run_dir, log_dir, log_filename), mode='w')
    file_handler.setFormatter(logging.Formatter(fmt='%(asctime)s [%(pathname)s:%(lineno)d] [%(levelname)s] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S'))
    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger

def get_script_name():
    ''' gets a script name, log are named after the script '''
    script_path = sys.argv[0]
    path_elems = script_path.split('/')
    script_name = path_elems[len(path_elems) - 1]
    res = script_name.split('.py')
    return res[0]

def proces_csv_and_store(full_file_path, write_run):
    df = pd.read_csv(full_file_path, sep='\t')
    columns_to_drop = ['Heat index (°C)', 'Gust of wind (m/s)', 'Average wind direction (°)']
    df.drop(columns=columns_to_drop, inplace=True)
    new_columns = ['Date (Europe/Oslo)', 'temp_c', 'wind_chill_c', 'dew_point_c', 'humidity_percent', 'average_wind_speed_m_s', 'atm_pressure_hpa', 'uv_index', 'alt_m', 'lat', 'lon']
    df.columns = new_columns
    df = df.dropna(subset=['atm_pressure_hpa']).copy()
    df['time'] = pd.to_datetime(df['Date (Europe/Oslo)']).dt.tz_localize('Europe/Oslo')
    df['time'] = df['time'].dt.tz_convert('UTC')
    df.drop(columns=['Date (Europe/Oslo)'], inplace=True)
    df.set_index('time', inplace=True)
    fields = df.columns.to_list()
    df['sensor_type'] = 'skywatch_bl_500'
    df['position'] = 'Hospitveien 12b'
    tags = ['sensor_type', 'position']
    if write_run:
        return store_points(df, tags, fields)

def process_data(write_run):
    ''' reads all csv from DATA_DIR, after successful writing they are moved to PROCESSED_DIR '''
    SCRIPT_DIR = os.getcwd()
    DATA_DIR = 'weather_cloud_data'
    if not os.path.exists(os.path.join(SCRIPT_DIR, DATA_DIR)):
        LOGGER.error(f"{os.path.join(SCRIPT_DIR, DATA_DIR)} data folder is missng, aborting.")
        sys.exit(1)

    PROCESSED_DIR = 'sensor_processed'
    if not os.path.exists(os.path.join(SCRIPT_DIR,  PROCESSED_DIR)):
        os.mkdir(os.path.join(SCRIPT_DIR,  PROCESSED_DIR))

    os.chdir(DATA_DIR)
    files = glob.glob("*.csv")
    for f in files:
        start_processing = dt.now()
        LOGGER.info(f"Trying to process: {f}")
        full_file_path = os.path.join(SCRIPT_DIR, DATA_DIR, f)
        write_res = proces_csv_and_store(full_file_path, write_run)
        print(write_res)
        if (write_run and write_res[0]):
            dest_file_path = os.path.join(SCRIPT_DIR, PROCESSED_DIR, f)
            os.rename(full_file_path, dest_file_path)
            end_processing = dt.now()
            LOGGER.info(f"{f} processed")
            LOGGER.info(f"processed {write_res[1]} datapoints in: { end_processing - start_processing} [ms]")
    LOGGER.info(f"total processed {len(files)} files")


START_SCRIPT_TIME = dt.now()
LOGNAME = get_script_name() + '.log'
LOG_DIR = 'logs'
LOGGER = set_up_log(LOG_DIR, LOGNAME)

LOGGER.info("start script")

if not os.path.exists(os.path.join(os.getcwd(), 'influxdb_credentials')):
    LOGGER.error("influxdb_credentials file is missing")
    sys.exit(1)
influx_auth = json.load(open(os.path.join(os.getcwd(), 'influxdb_credentials')))

INFLUX_WRITE_CLIENT = DataFrameClient(
    host = 'localhost',
    port = 8086,
    database ='weather_cloud',
    username = influx_auth['username'],
    password = influx_auth['password'])

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--dry-run', action='store_true',
    help="does not write to database, just show result in csv file")
args = parser.parse_args()

process_data(not args.dry_run)

LOGGER.info(f"end script script, duration: {dt.now() - START_SCRIPT_TIME} [ms]")