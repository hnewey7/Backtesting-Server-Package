'''
Module for testing the BacktestingServer object.

Created on Tuesday 9th April 2024.
@author: Harry New

'''

import ig_package
import paramiko.ssh_exception
import pytest
import paramiko
import pymysql
from backtesting_server import BacktestingServer
from SERVER_DETAILS import get_standard_server_details, get_mysql_server_details, get_ig_details
from datetime import datetime, timedelta
import time

# - - - - - - - - - - - - - - - - - - -

def reset_mysql_tables(server: BacktestingServer) -> None:
  """ Resetting tables in the MySQL server.
  
    Parameters
    ----------
    server: BacktestingServer
      Server containing tables."""
  # Getting all table names.
  server.cursor.execute("SHOW TABLES")
  results = server.cursor.fetchall()
  # Dropping tables.
  for table in results:
    server.cursor.execute("DROP TABLE {};".format(table[0]))

# - - - - - - - - - - - - - - - - - - -
# INITIALISATION TESTS.

def test_init() -> None:
  """ Testing the initialisation of the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())

  # Handling assertions.
  assert server.standard_details == get_standard_server_details()
  assert server.sql_details == get_mysql_server_details()
  assert server.channel == None
  assert server.cursor == None 

@pytest.mark.parametrize(("standard_details","sql_details"), 
                         [(None,None),
                          ("Test","Test"),
                          (1,1),
                          (1.2,1.2)])
def test_init_invalid_type(standard_details,sql_details) -> None:
  """ Testing initialisation with invalid parameters."""
  with pytest.raises(TypeError):
    server = BacktestingServer(standard_details=standard_details,sql_details=sql_details)

@pytest.mark.parametrize(("standard_details","sql_details"),
                         [({'server':'','username':''},{'server':'','username':'','password':''}),
                          ({'server':'','username':'','password':''},{'server':'','username':''}),
                          ({'server':'','username':'','password':'','bs':''},{'server':'','username':'','password':''}),
                          ({'server':'','username':'','password':''},{'server':'','username':'','password':'','bs':''})])
def test_init_invalid_keys(standard_details,sql_details) -> None:
  """ Testing initialisation with invalid keys in details."""
  with pytest.raises(KeyError):
    server = BacktestingServer(standard_details=standard_details,sql_details=sql_details)

# - - - - - - - - - - - - - - - - - - -
# CONNECTION TESTS.

@pytest.mark.parametrize("iteration", [i for i in range(10)])
def test_connect(iteration) -> None:
  """ Testing the connect method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  channel, cursor = server.connect(database="test")

  # Handling assertions.
  assert channel
  assert type(channel) == paramiko.Channel
  assert cursor
  assert type(cursor) == pymysql.cursors.Cursor
  assert server.channel
  assert type(server.channel) == paramiko.Channel
  assert server.cursor
  assert type(server.cursor) == pymysql.cursors.Cursor

def test_connect_invalid() -> None:
  """ Testing invalid details with the BacktestingServer object."""
  with pytest.raises(paramiko.ssh_exception.NoValidConnectionsError):
    # Creating backtesting server object.
    server = BacktestingServer(standard_details={"server":"", "username":"", "password":""},sql_details={"server":"", "username":"", "password":""})
    # Connecting to the server.
    server.connect(database="test")

def test_connect_invalid_database() -> None:
  """ Testing connect method with an invalid database."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  with pytest.raises(pymysql.err.OperationalError):
    channel, cursor = server.connect(database="invalid")

# - - - - - - - - - - - - - - - - - - -
# HISTORICAL DATA TESTS.

def test_check_historical_data_summary_exists() -> None:
  """ Testing check_historical_data_summary_exists() method within the BacktestingServer object.
    - Connects to server and 'test' database.
    - Deletes any existing historical data summary.
    - Asserts if method returns false.
    - Creates a new data.
    - Asserts if method returns true."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Checking table doesn't exist.
  assert not server._check_historical_data_summary_exists()

  # Creating table.
  server._create_historical_data_summary()
  assert server._check_historical_data_summary_exists()

  # Deleting table after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")

def test_check_instrument_in_historical_data() -> None:
  """ Testing check_instrument_in_historical_data() method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)
  # Creating table.
  server._create_historical_data_summary()

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')

  # Checking before instrument is added.
  assert not server._check_instrument_in_historical_data(test_instrument)

  # Inserting test instrument.
  server.cursor.execute(f"INSERT INTO HistoricalDataSummary (InstrumentName, Epic)\
  VALUES ('{test_instrument.name}', '{test_instrument.epic}');")

  # Checking instrument.
  assert server._check_instrument_in_historical_data(test_instrument)

   # Deleting table after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  

def test_add_historical_data() -> None:
  """ Testing add_historical_data() method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)
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

  # Checking through check instrument in historical data method.
  assert server._check_instrument_in_historical_data(test_instrument)

  # Checking if table added.
  try:
    server.cursor.execute(f"SELECT * FROM {test_instrument.name.replace(' ','_')}_HistoricalDataset;")
    assert True
  except:
    assert False

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f"DROP TABLE {test_instrument.name.replace(' ','_')}_HistoricalDataset;")

def test_upload_historical_data() -> None:
  """ Testing upload_historical_data() method within the BacktestingServer object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)
  # Creating table.
  server._create_historical_data_summary()

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')
  data = test_instrument.get_historical_prices("SECOND","2024:04:15-14:13:00","2024:04:15-14:14:00")

  # Uploading historical data.
  server.upload_historical_data(test_instrument,dataset=data)

  # Checking tables were created.
  server.cursor.execute("SHOW TABLES;")
  result_string = str(server.cursor.fetchall())
  assert "historicaldatasummary" in result_string
  assert f"{test_instrument.name.replace(' ','_')}_historicaldataset".lower() in result_string

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
  server.cursor.execute(f'DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;')

def test_upload_historical_data_with_group() -> None:
  """ Testing the upload historical data method with the group features."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)
  # Creating table.
  server._create_historical_data_summary()

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')

  # Creating an instrument group.
  server.add_instrument_group("test")

  # Uploading instrument with group tag.
  server.upload_historical_data(test_instrument,True,groups=server.instrument_groups)

  # Checking if tag in historical data summary.
  server.cursor.execute("Select InstrumentName From HistoricalDataSummary WHERE InstrumentGroup = 'test';")
  result = server.cursor.fetchall()
  assert result[0][0] == test_instrument.name

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f'DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  server.cursor.execute("DROP TABLE InstrumentGroups;")

def test_upload_clean_historical_data() -> None:
  """ Testing upload clean historical data method which uploads range of data with various resolutions for initial backtesting."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')

  # Uploading to server with live tracking enabled.
  server.upload_historical_data(test_instrument,live_tracking=True)

  # Running clean historical data.
  server._upload_clean_historical_data(test_instrument)

  # Handling checks.
  server.cursor.execute(f'Select * FROM {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  results = server.cursor.fetchall()
  single_result = results[0]
  assert len(results) > 10
  assert len(single_result) == 5

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f'DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;')

def test_upload_on_existing_historical_data() -> None:
  """ Testing the upload on existing historical data method."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')

  # Uploading to server with live tracking enabled.
  server.upload_historical_data(test_instrument,live_tracking=True)

  # Getting current datetime.
  previous_datetime = datetime.now() - timedelta(days=10)
  
  # Uploading on existing historical data.
  server._upload_on_existing_historical_data(test_instrument,previous_datetime)

  # Handling checks.
  server.cursor.execute(f'Select * FROM {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  results = server.cursor.fetchall()
  single_result = results[0]
  assert len(results) > 1
  assert len(single_result) == 5

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f'DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;')

def test_update_historical_data() -> None:
  """ Testing update historical data method."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')

  # Uploading to server with live tracking enabled.
  server.upload_historical_data(test_instrument,live_tracking=True)

  # Running update historical data.
  server.update_historical_data(ig)

  # Handling checks.
  server.cursor.execute(f'Select * FROM {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  initial_results = server.cursor.fetchall()
  single_result = initial_results[0]
  assert len(initial_results) > 10
  assert len(single_result) == 5

  # Running update historical data on existing data.
  server.update_historical_data(ig)

  # Handling checks.
  server.cursor.execute(f'Select * FROM {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  results = server.cursor.fetchall()
  single_result = results[0]
  assert len(results) >= len(initial_results)
  assert len(single_result) == 5

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f'DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;')

def test_update_historical_data_with_groups() -> None:
  """ Testing the update historical data method but with specific instrument groups."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'])
  test_instrument = ig.search_instrument('FTSE 100')
  test_2_instrument = ig.search_instrument('SP 500')

  # Adding an instrument group.
  server.add_instrument_group("test")
  server.add_instrument_group("test2")

  # Uploading to server with live tracking enabled.
  server.upload_historical_data(test_instrument,live_tracking=True,groups=[server.instrument_groups[0]])
  server.upload_historical_data(test_2_instrument,live_tracking=True,groups=[server.instrument_groups[1]])

  # Running update historical data.
  server.update_historical_data(ig,groups=[server.instrument_groups[0]])

  # Handling checks of instrument that should have updated.
  server.cursor.execute(f'Select * FROM {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  initial_results = server.cursor.fetchall()
  single_result = initial_results[0]
  assert len(initial_results) > 10
  assert len(single_result) == 5
  # Handling checks of instrument that didn't update.
  server.cursor.execute(f'Select * FROM {test_2_instrument.name.replace(" ","_")}_HistoricalDataset;')
  second_results = server.cursor.fetchall()
  assert len(second_results) == 0

  # Running update historical data on existing data.
  server.update_historical_data(ig,groups=[server.instrument_groups[0]])

  # Handling checks of instrument that should have updated.
  server.cursor.execute(f'Select * FROM {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  results = server.cursor.fetchall()
  single_result = results[0]
  assert len(results) >= len(initial_results)
  assert len(single_result) == 5
  # Handling checks of instrument that didn't update.
  server.cursor.execute(f'Select * FROM {test_2_instrument.name.replace(" ","_")}_HistoricalDataset;')
  second_other_results = server.cursor.fetchall()
  assert len(second_other_results) == 0

  # Deleting tables after testing.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")  
  server.cursor.execute(f'DROP TABLE {test_instrument.name.replace(" ","_")}_HistoricalDataset;')
  server.cursor.execute(f'DROP TABLE {test_2_instrument.name.replace(" ","_")}_HistoricalDataset;')
  server.cursor.execute("DROP TABLE InstrumentGroups;")

# - - - - - - - - - - - - - - - - - - -
# INSTRUMENT GROUP TESTS.

def test_create_instrument_groups_table() -> None:
  """ Testing the create instrument groups table method."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)
  # Checking no table.
  assert not server._check_instrument_groups_table()
  # Creating table.
  server._create_instrument_groups_table()
  # Checking table is present.
  assert server._check_instrument_groups_table()
  # Deleting table.
  server.cursor.execute("DROP TABLE InstrumentGroups;")

def test_add_instrument_group() -> None:
  """ Testing adding a new instrument group."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Adding the instrument group.
  server.add_instrument_group("test")
  
  # Checking the instrument group is present in server.
  server.cursor.execute("SELECT * FROM InstrumentGroups WHERE GroupName = 'test';")
  result = server.cursor.fetchall()
  assert len(result) == 1

  # Checking instrument group object is present.
  assert len(server.instrument_groups) == 1

  # Removing table.
  server.cursor.execute("DROP TABLE InstrumentGroups;")

def test_del_instrument_group() -> None:
  """ Testing deleting an instrument group method."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Creating the instrument groups table.
  server._create_instrument_groups_table()
  # Adding the instrument group.
  server.add_instrument_group("test")

  # Checking the instrument group is present.
  server.cursor.execute("SELECT * FROM InstrumentGroups WHERE GroupName = 'test';")
  result = server.cursor.fetchall()
  assert len(result) == 1
  assert len(server.instrument_groups) == 1

  # Removing instrument group
  server.del_instrument_group("test")

  # Checking the instrument group is present.
  server.cursor.execute("SELECT * FROM InstrumentGroups WHERE GroupName = 'test';")
  result = server.cursor.fetchall()
  assert len(result) == 0
  assert len(server.instrument_groups) == 0

  # Removing table.
  server.cursor.execute("DROP TABLE InstrumentGroups;")

def test_get_instrument_groups() -> None:
  """ Testing the get instrument groups method."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Adding instrument groups.
  server.add_instrument_group("test")

  # Getting instrument grous.
  instrument_groups = server._get_instrument_groups()
  assert len(instrument_groups) == 1
  assert instrument_groups[0].name == "test"
  assert instrument_groups[0].cursor == server.cursor

  # Removing table.
  server.cursor.execute("DROP TABLE InstrumentGroups;")

def test_update_groups_in_historical_data() -> None:
  """ Testing method to update the groups type in the historical data summary table."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting table.
  reset_mysql_tables(server)
  # Creating hisorical data summary.
  server._create_historical_data_summary()

  # Checking data type of group column.
  server.cursor.execute("SHOW FIELDS FROM HistoricalDataSummary WHERE Field = 'InstrumentGroup';")
  results = server.cursor.fetchall()
  assert results[0][1] == "set('')"

  # Adding instrument groups.
  server.add_instrument_group("test")

  # Checking data type of group column.
  server.cursor.execute("SHOW FIELDS FROM HistoricalDataSummary WHERE Field = 'InstrumentGroup';")
  results = server.cursor.fetchall()
  assert results[0][1] == "set('test')"

  # Adding instrument groups.
  server.add_instrument_group("test2")

  # Checking data type of group column.
  server.cursor.execute("SHOW FIELDS FROM HistoricalDataSummary WHERE Field = 'InstrumentGroup';")
  results = server.cursor.fetchall()
  assert results[0][1] == "set('test','test2')"

  # Deleting instrument groups.
  server.del_instrument_group("test2")

  # Checking data type of group column.
  server.cursor.execute("SHOW FIELDS FROM HistoricalDataSummary WHERE Field = 'InstrumentGroup';")
  results = server.cursor.fetchall()
  assert results[0][1] == "set('test')"

  # Deleting instrument groups.
  server.del_instrument_group("test")

  # Checking data type of group column.
  server.cursor.execute("SHOW FIELDS FROM HistoricalDataSummary WHERE Field = 'InstrumentGroup';")
  results = server.cursor.fetchall()
  assert results[0][1] == "set('')"

  # Removing table.
  server.cursor.execute("DROP TABLE HistoricalDataSummary;")
  server.cursor.execute("DROP TABLE InstrumentGroups;")