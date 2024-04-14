'''
Module for testing the BacktestingServer object.

Created on Tuesday 9th April 2024.
@author: Harry New

'''

import pytest
from backtesting_server import BacktestingServer
from SERVER_DETAILS import get_standard_server_details, get_mysql_server_details

# - - - - - - - - - - - - - - - - - - -

def test_BacktestingServer_init() -> None:
  """ Testing the initialisation of the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())

  # Handling assertions.
  assert server.standard_details
  assert server.sql_details
  for key in ["server","username","password"]:
    assert server.standard_details[key] 
    assert server.sql_details[key]

@pytest.mark.parametrize("iteration", [i for i in range(100)])
def test_BacktestingServer_connect(iteration) -> None:
  """ Testing the connect method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  channel, cursor = server.connect(database="test")

  # Handling assertions.
  assert channel
  assert cursor
  assert server.channel
  assert server.cursor

def test_BacktestingServer_connect_invalid() -> None:
  """ Testing invalid details with the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details={"server":"", "username":"", "password":""},sql_details={"server":"", "username":"", "password":""})
  # Connecting to the server.
  channel, cursor = server.connect(database="test")

  # Handling assertions.
  assert channel == None
  assert cursor == None
  assert server.channel == None
  assert server.cursor == None

def test_BacktestingServer_check_historical_data_summary_exists() -> None:
  """ Testing check_historical_data_summary_exists() method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")

  # Deleting any existing tables.
  try:
    server.cursor.execute("DROP TABLE HistoricalDataSummary;")
  except:
    pass
  assert not server._check_historical_data_summary_exists()

  # Creating table.
  server._create_historical_data_summary()
  assert server._check_historical_data_summary_exists()