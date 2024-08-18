#Anna Wojciechowska, Oslo, August 2024

#  Script to write to influx db 1.8 location of testing data
#  
#  This script is wrting to 
#  database: 'sensor'
#  measurement: 'test_location' 
#  tags: 'place'
#  fields: 'pressure_mbar' (unit mBars), 'temp_c' (unit degrees Celcius )
# TAGS
# +---------+---------------+--------+----------------+
# |tag name | location_name | code   | 'water_type_m' |
# +---------+---------------+--------+----------------+
# | type    | string        | string | string         |
# +---------+---------------+--------+----------------+

# FIELDS
# +-------------+-------+-------+-------------+----------------+
# |field name   | lat   |  lon  | max_depth_m | sensor_depth_m |
# +-------------+-------+-------+-------------+----------------+
# | type        | float | float | float       | float           |
# +-------------+-------+-------+------+------+----------------+


from influxdb import DataFrameClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError


import pandas as pd
import json

import sys
import os

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



def store_points(df, measurement_name, tags, fields):
    try:
        result = INFLUX_WRITE_CLIENT.write_points(df,measurement_name,tag_columns = tags, field_columns = fields,protocol='line')
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
    except InfluxDBClientError:
            LOGGER.error("InfluxDBClientError, check if database exist in influx: 'SHOW DATABASES'.")
            LOGGER.error(traceback.format_exc())
            sys.exit(1)
    except InfluxDBServerError:
            LOGGER.error("InfluxDBServerError")
            LOGGER.error(traceback.format_exc())
            sys.exit(1)



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

locations_df = pd.read_table("./sensor_location_data.csv", sep = ',')
if not args.dry_run:
    tags = ['location_name', 'code', 'water_type_m']
    fields = ['lat_deg', 'lon_deg', 'max_depth_m', 'sensor_depth_m']
    locations_df['time'] = pd.to_datetime(dt.now())
    locations_df.set_index('time', inplace=True)
    store_points(locations_df, "testing_locations", tags, fields)

LOGGER.info(f"end script script, duration: {dt.now() - START_SCRIPT_TIME} [ms]")
