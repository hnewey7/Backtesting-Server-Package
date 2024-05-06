'''
Script for adding a new Instrument Group to the Backtesting Server.

Created on Monday 29th April 2024.
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

  # Adding new instrument group.
  server.add_instrument_group("Indices_1")