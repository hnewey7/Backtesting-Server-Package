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

def test_BacktestingServer_connect() -> None:
  """ Testing the connect method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  channel, cursor = server.connect()

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
  channel, cursor = server.connect()

  # Handling assertions.
  assert channel == None
  assert cursor == None
  assert server.channel == None
  assert server.cursor == None