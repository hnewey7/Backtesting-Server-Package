'''
Unit tests for the Instrument Group object.

Created on Friday 3rd May 2024
@author: Harry New

'''

from src.backtesting_server import BacktestingServer,InstrumentGroup
from SERVER_DETAILS import *
from ig_package import IG

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

# - - - - - - - - - - - - - - -

def test_init() -> None:
  """ Testing initialisation of the Instrument Group object."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)
  # Creating Instrument Group.
  instrument_group = InstrumentGroup("test",server.cursor)

  # Handling checks.
  assert instrument_group.name == "test"
  assert instrument_group.cursor == server.cursor

def test_init_with_instruments() -> None:
  """ Testing initialisation of the Instrument Group object with an instrument group and instruments present."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Connecting to IG.
  ig = IG(ig_details["key"],ig_details["username"],ig_details["password"])

  # Getting test instrument.
  test_instrument = ig.search_instrument("FTSE 100")

  # Adding group with instruments.
  instrument_group = server.add_instrument_group("test")

  # Uploading instrument with group.
  server.upload_instrument(test_instrument,True,groups=[instrument_group])

  # Getting instrument names and epics from group.
  instrument_names = instrument_group._get_instrument_names()
  instrument_epics = instrument_group._get_instrument_epics()

  # Handling checks.
  for name in instrument_names:
    assert name == test_instrument.name
  for epic in instrument_epics:
    assert epic == test_instrument.epic

def test_get_instruments() -> None:
  """ Testing the get_instruments() method."""
  # Creating backtesting server object.
  server = BacktestingServer(standard_details=get_standard_server_details(),sql_details=get_mysql_server_details())
  # Connecting to the server.
  server.connect(database="test")
  # Resetting tables.
  reset_mysql_tables(server)

  # Connecting to IG.
  ig = IG(ig_details["key"],ig_details["username"],ig_details["password"])

  # Getting test instrument.
  test_instrument = ig.search_instrument("FTSE 100")

  # Adding group with instruments.
  instrument_group = server.add_instrument_group("test")

  # Uploading instrument with group.
  server.upload_instrument(test_instrument,True,groups=[instrument_group])

  # Getting instruments from Instrument Group.
  instruments = instrument_group.get_instruments(ig)

  assert instruments[0].name == test_instrument.name
  assert instruments[0].epic == test_instrument.epic
  assert instruments[0].IG_obj == test_instrument.IG_obj