#Anna Wojciechowska, Oslo September 2024
# script to test cron job
# * * * * *  date; cd /home/anna/python_scripts/sensor_scripts/forecast && /usr/bin/python3 /home/anna/python_scripts/sensor_scripts/forecast/test_cron.py  >> /home/anna/python_scripts/sensor_scripts/forecast/logs/test_cron_cron.log 2>&1


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

LOGNAME = get_script_name() + '.log'
LOG_DIR = 'logs'
LOGGER = set_up_log(LOG_DIR, LOGNAME)

if __name__ == "__main__":
    LOGGER.info("start script")
    msg = "Message"
    LOGGER.error(f"this is an error {msg}")
    if dt.now().minute % 2 == 1:
        print(cause_error)
    LOGGER.info("end script")
