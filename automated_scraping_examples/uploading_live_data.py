'''
Script for uploading live data of a tracked Instrument Group.

Created on Monday 6th May 2024.
@author: Harry New

'''

from backtesting_server import BacktestingServer
from ig_package import IG
import logging
import json

# - - - - - - - - - - - - - - - - 

global logger 
logger = logging.getLogger()

# - - - - - - - - - - - - - - - -

if __name__ == "__main__":
  # Initialising logging.
  with open("logging_config.json") as f:
    config_dict = json.load(f)
    logging.config.dictConfig(config_dict)

  # Creating server object.
  server = BacktestingServer(
    standard_details={

    },
    sql_details={

    })
  
  # Connecting to the official database.
  server.connect(database="official")

  # Connecting to IG.
  ig_details = {

  }
  ig = IG(API_key=ig_details["key"],username=ig_details["username"],password=ig_details["password"],acc_type=ig_details["acc_type"],acc_number=ig_details["acc_number"])

  # Getting instrument group.
  for instrument_group in server.instrument_groups:
    if instrument_group.name == "Indices":
      indices_group = instrument_group

  # Uploading live data from instrumnet group.
  server.upload_live_data(instrument_group=indices_group)