'''
Module for testing the BacktestingServer object.

Created on Tuesday 9th April 2024.
@author: Harry New

'''

import ig_package
import paramiko.ssh_exception
import pytest
import paramiko
from backtesting_server import BacktestingServer
from SERVER_DETAILS import get_standard_server_details, get_mysql_server_details, get_ig_details

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

@pytest.mark.parametrize("iteration", [i for i in range(10)])
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
  with pytest.raises(paramiko.ssh_exception.NoValidConnectionsError):
    # Creating backtesting server object.
    server = BacktestingServer(standard_details={"server":"", "username":"", "password":""},sql_details={"server":"", "username":"", "password":""})
    # Connecting to the server.
    server.connect(database="test")

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

  # Deleting table after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")

def test_BacktestingServer_check_instrument_in_historical_data() -> None:
  """ Testing check_instrument_in_historical_data() method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Creating table.
  server._create_historical_data_summary()

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')
  # Inserting test instrument.
  server.cursor.execute(f"INSERT INTO HistoricalDataSummary (InstrumentName, Epic)\
  VALUES ('{test_instrument.name}', '{test_instrument.epic}');")

  # Checking instrument.
  assert server._check_instrument_in_historical_data(test_instrument)

   # Deleting table after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  

def test_BacktestingServer_add_historical_data() -> None:
  """ Testing add_historical_data() method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Creating table.
  server._create_historical_data_summary()

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')

  # Adding historical data
  server._add_historical_data_summary(test_instrument)

  # Checking if added to summary.
  server.cursor.execute(f"SELECT * FROM HistoricalDataSummary WHERE Epic='{test_instrument.epic}';")
  assert len(server.cursor.fetchall()) > 0
  # Checking if table added.
  try:
    server.cursor.execute(f"SELECT * FROM {test_instrument.name.replace(' ','_')}_HistoricalDataset;")
    assert True
  except:
    assert False

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f"DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;")

def test_BacktestingServer_upload_historical_data() -> None:
  """ Testing upload_historical_data() method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Creating table.
  server._create_historical_data_summary()

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')
  data = test_instrument.get_historical_prices("SECOND","2024:04:15-14:13:00","2024:04:15-14:14:00")

  # Uploading historical data.
  server.upload_historical_data(test_instrument,data)

  # Checking tables were created.
  server.cursor.execute("SHOW TABLES;")
  result_string = str(server.cursor.fetchall())
  assert "historicaldatasummary" in result_string
  assert f"{test_instrument.name.replace(" ","_")}_historicaldataset".lower() in result_string

  # Checking data added.
  data = data.dropna()
  for datapoint in data.index:
    server.cursor.execute(f'SELECT * FROM {test_instrument.name.replace(" ","_")}_historicaldataset WHERE DatetimeIndex = "{datapoint}"')
    result = server.cursor.fetchall()
    assert result[0][1] == data["Open"][datapoint]
    assert result[0][2] == data["High"][datapoint]
    assert result[0][3] == data["Low"][datapoint]
    assert result[0][4] == data["Close"][datapoint]

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f"DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;")