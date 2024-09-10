#Anna Wojciechowska, Oslo August 2024
# script to download wave forecast file from met.no threads server
import requests

import pytz
from datetime import datetime as dt
from datetime import timedelta

import sys
import os

import argparse


import traceback
import logging

def get_script_name():
    ''' gets a script name, log are named after the script '''
    script_path = sys.argv[0]
    path_elems = script_path.split('/')
    script_name = path_elems[len(path_elems) - 1]
    res = script_name.split('.py')
    return res[0]

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

def download_forecast():
    # since bolge server is in UTC I need to localize the time to Oslo CEST +2/3
    oslo_time = pytz.timezone("Europe/Oslo")
    now = oslo_time.localize(dt.now())
    formatted_date = now.strftime('%d_%m_%Y')
    hour_suffix  = '00'
    if now.hour // 12 == 1:
        hour_suffix  = '12'
    file_name = f"MyWave_wam800_c4WAVE{hour_suffix}_{formatted_date}.nc"
    download_directory = "forecast_files"
    download_location = (os.path.join(os.getcwd(), download_directory, file_name))
    url  = f"https://thredds.met.no/thredds/fileServer/fou-hi/mywavewam800s/MyWave_wam800_c4WAVE{hour_suffix}.nc"
    response = requests.get(url)
    if response.status_code == 200:
        LOGGER.info(f"Status code ok: {response.status_code}")
        with open(download_location, 'wb') as file:
            file.write(response.content)
        LOGGER.info(f"Downloaded: {file_name}")
    else:
        LOGGER.error(f"Not ok status code: {response.status_code}")
    
START_SCRIPT_TIME = dt.now()
LOGNAME = get_script_name() + '.log'
LOG_DIR = 'logs'
LOGGER = set_up_log(LOG_DIR, LOGNAME)

if __name__ == "__main__":
    LOGGER.info("start script")
    download_forecast()
    LOGGER.info("end script")
