#Anna Wojciechowska, Oslo, December 2022

#  Script to process data from sensor generated csv files
#  The scripts reads all csv files in "sensor_data" directory and after successful read moved the file to "processed_data".
#  
#  This script is wrting to 
#  database: 'pressure_sensor'
#  measurement: 'pressure' 
#  tags: 'place'
#  fields: 'pressure_mbar' (unit mBars), 'temp_c' (unit degrees Celcius )
  
# TAGS
# +---------+--------+--------------+--------------+
# |tag name | place  | sensor_model | sensor_name  |
# +---------+--------+--------------+--------------+
# | type    | string | int          | string       |
# +---------+--------+--------------+--------------+
#sensor model is currently set to 0, place is not yet used, to be added in the future

# FIELDS
# +-------------+--------------+---------+
# |field name   | pressure_mbar| temp_c  |
# +-------------+--------------+---------
# | type        |    integer   | float   |
# +-------------+--------------+---------+


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


def read_settings_line(settings_line):
    if settings_line == 'Default mission information for csv file header':
        return "not_set", "not_named", "UTC+0"
    res = settings_line.split(' ')
    position = res[0]
    model = res[1]
    utc_shift = res[2].split(',')[0]
    return position, model, utc_shift

def get_metadata(file_path):
    try:
        with open(file_path, 'r') as file:
            first_line = file.readline().strip()
            LOGGER.info(f"Processing {file_path}.")
            return read_settings_line(first_line)
    except FileNotFoundError:
        LOGGER.errof(f" {file_path} was not found.")
    except IOError:
        LOGGER.errof(f"An io error occurred trying to read the file {file_path}.")

def get_utc_time_offset(utc_string):
    #reading utf time offset information
    # tested against utc_string = ["UTC+0", "UTC-1", "UTC+2" , "utc-0"]
    offset_list = utc_string.lower().split('utc')
    offset_sign  = offset_list[1][0]
    offset_val = offset_list[1][1:]
    offset_int_val = int(offset_val)
    if offset_sign == '-':
        offset_int_val *= -1
    return  offset_int_val

def store_points(df):
    try:
        result = INFLUX_WRITE_CLIENT.write_points(df,'pressure',tag_columns = ['sensor_model', 'sensor_position'], field_columns = ['pressure_mbar', 'temp_c', 'utc_offset'],protocol='line')
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

# influx cannot handle large data frame to be written in one go - it would generte excepton
# influxdb.exceptions.InfluxDBClientError: 413: {"error":"Request Entity Too Large"}
# hence data needs to be sliced to hourly chunks: 24 hour dataframe from single file will be sliced to 24 hourly chunks
def slice_data_and_store(df):
    # the stored time is from start_timestap to df.time.max()
    start_timestamp = df.time.min()
    time_range = df.time.max() -  start_timestamp
    start_timestamp_pd_type = pd.Timestamp(year=start_timestamp.year, month=start_timestamp.month, day=start_timestamp.day, hour=start_timestamp.hour, minute=0)
    hours = round(time_range.seconds/3600)

    file_datapoints_count = 0
    for i in range (0,hours+1):
        
        lower_limit = start_timestamp_pd_type +  pd.Timedelta(i, "h")
        upper_limit = start_timestamp_pd_type +  pd.Timedelta(i+1, "h")
        # select slice of data for given hour
        df_hour = df[(df.time >= lower_limit) & (df.time < upper_limit)]
        df_hour.set_index('time', inplace=True)
        res = store_points(df_hour)
        LOGGER.info(f"Processed chunk of {res[1]} datapoints.")
        file_datapoints_count += res[1]
    LOGGER.info(f"Processed total of {file_datapoints_count} datapoints from a file.")
    return res



def proces_csv_and_store(file_path, write_run): 
    ''' reads from csv at file_path and stores to influx '''
    ''' returns true if writen, together with datapoints count'''
    if os.stat(file_path).st_size > 0:
        sensor_meta_data = get_metadata(file_path)
        if sensor_meta_data == None:
            # return False, since not written, and 0 datapoints
            return (False ,0)
        df = pd.read_csv(file_path, skiprows=1) 
        if df.shape[0] > 0:
            df['sensor_position'] = sensor_meta_data[0]
            df['sensor_model'] = sensor_meta_data[1]
            utc_time_offset = get_utc_time_offset(sensor_meta_data[2])
            utc_offset_time_delta = pd.Timedelta(days=0, hours=utc_time_offset)
            df['frac_string'] = df['frac.seconds'].apply(lambda x: str(x))
            df['dt_string'] = df['DateTime'].apply(lambda x: str(x))
            df['time'] = pd.to_datetime( df['dt_string'] + '.' + df['frac_string'])
            df['utc_offset'] = utc_time_offset
            #inflxdb default time zone is UTC, thus all data will be stored in UTC+0
            df['time'] = df['time'] - utc_offset_time_delta
            df = df.drop(columns=['dt_string', 'frac_string', 'POSIXt', 'DateTime', 'frac.seconds' ])
            df = df.rename(columns={"Pressure.mbar": "pressure_mbar", "TempC": "temp_c"})
            if write_run:
                return slice_data_and_store(df)
        else: 
            # no datapoints
            return (False,0)
    else: 
        #file is 0 size:
        return (False,0)


           
def process_data(write_run):
    ''' reads all csv from DATA_DIR, after successful writing they are moved to PROCESSED_DIR '''
    SCRIPT_DIR = os.getcwd()
    DATA_DIR = 'sensor_data'
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
    database ='sensor',
    username = influx_auth['username'],
    password = influx_auth['password'])

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--dry-run', action='store_true',
    help="does not write to database, just show result in csv file")
args = parser.parse_args()

process_data(not args.dry_run)

LOGGER.info(f"end script script, duration: {dt.now() - START_SCRIPT_TIME} [ms]")
