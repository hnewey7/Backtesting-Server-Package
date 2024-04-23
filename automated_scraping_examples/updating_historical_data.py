'''
Script for automatically updating all historical data on the Backtesting Server.

Created on Tuesday 23rd April 2024.
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
  ig = IG(API_key=ig_details["key"],username=ig_details["username"],password=ig_details["password"])

  # Updating all historical data within server.
  server.update_historical_data(ig)