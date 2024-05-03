'''
Module for uploading backtesting results to MySQL server.

Created on Tuesday 19th March 2024.
@author: Harry New

'''
from __future__ import annotations

import json
import logging.config
import paramiko
import sys
import paramiko.channel
import pymysql
import pymysql.cursors
import ig_package
import pandas as pd
from datetime import datetime
import time


# - - - - - - - - - - - - - -

global logger 
logger = logging.getLogger()

# - - - - - - - - - - - - - -

class BacktestingServer():
  """ Object representing the SQL server, allowing users to interact without having to directly connect.
        - Handles backtesting strategies.
        - Allows results to be uploaded."""
  
  def __init__(self, standard_details:dict, sql_details:dict) -> None:
    """
        Parameters
        ----------
        standard_details: dict
          Details for the standard server including 'server', 'username' and 'password'.
        sql_details: dict
          Details for the sql server including 'server', 'username' and 'password'."""
    # Checking parameter type.
    if type(standard_details) != dict and type(sql_details) != dict:
      logger.info("Invalid parameter type, please input dict type only.")
      raise TypeError
    # Checking keys.
    details = {
      'server': '', 
      'username': '',
      'password': ''
    }
    if standard_details.keys() != details.keys() or sql_details.keys() != details.keys():
      logger.info("Invalid keys in details provided.")
      raise KeyError
    else:
      # Getting details.
      self.standard_details = standard_details
      self.sql_details = sql_details
      
      self.channel: paramiko.Channel = None
      self.cursor: pymysql.cursors.Cursor = None

    self.instrument_groups: list[InstrumentGroup] = []

  def connect(self, database:str) -> tuple[paramiko.Channel, pymysql.cursors.Cursor] | tuple[None, None]:
    """ Connecting to MySQL server using SSH.
        
        Parameters
        ----------
        database: str
          Name of database to connect to.

        Returns
        -------
        paramiko.Channel
          Channel to MySQL server.
        pymysql.cursors.Cursor
          Cursor to execute SQL queries."""
    try:
      # Connecting through SSH.
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      logger.info("Connecting to server: {}".format(self.standard_details["server"]))
      ssh.connect(self.standard_details["server"],username=self.standard_details["username"],password=self.standard_details["password"],timeout=20,allow_agent=False,look_for_keys=False)

      # Connecting to MySQL server.
      logger.info("Connecting to MySQL server.")
      transport = ssh.get_transport()
      channel = transport.open_channel("direct-tcpip", ('127.0.0.1', 3306), ('localhost', 3306))
      c = pymysql.connect(database=database, user=self.sql_details['username'], password=self.sql_details['password'], defer_connect=True, autocommit=True)
      c.connect(channel)

      logger.info("Successfully connected to MySQL server.")
      # Getting cursor to execute commands.
      cursor = c.cursor()
      # Adding channel and cursor to server.
      self.channel = channel
      self.cursor = cursor

      if self._check_instrument_groups_table:
        # Getting all instrument groups from server.
        self.instrument_groups = self._get_instrument_groups()

      return channel, cursor
    except Exception as e:
      logger.info("Unable to connect to MySQL server.")
      raise e

  def upload_instrument(self, instrument:ig_package.Instrument, live_tracking:bool=False, dataset:pd.DataFrame=[], groups: list[InstrumentGroup] = []) -> None:
    """ Uploading instrument to the backtesting server.
    
        Parameters
        ----------
        instrument: ig_package.Instrument
          Instrument the historical data corresponds to.
        live_tracking: bool = False
          OPTIONAL Enable/disable live tracking of instrument.
        dataset: pd.DataFrame = []
          OPTIONAL DataFrame containing the data to be uploaded.
        groups: list[InstrumentGroup] = []
          OPTIONAL List of Instrument Groups to add the instrument to."""
    # Checking if historical data summary exists.
    if not self._check_historical_data_summary_exists():
      # Creating summary table.
      self._create_historical_data_summary()
    # Checking if data is already present.
    if not self._check_instrument_in_historical_data(instrument):
      # Adding new instrument.
      self._add_historical_data_summary(instrument, live_tracking, groups)

    # Checking if data.
    if len(dataset) > 0:
      # Filtering out NaN values.
      dataset = dataset.dropna()
      # Inserting each row into database.
      logger.info("Inserting data into server-side dataset.")
      for data_point in dataset.index:
        try:
          insert_statement = f'INSERT INTO {instrument.name.replace(" ","_")}_HistoricalDataset (DatetimeIndex, Open, High, Low, Close) VALUES (%s, %s, %s, %s, %s)'
          values = [
            (str(data_point), float(dataset["Open"][data_point]), float(dataset["High"][data_point]), float(dataset["Low"][data_point]), float(dataset["Close"][data_point])),
          ]
          self.cursor.executemany(insert_statement, values)
        except pymysql.err.IntegrityError:
          logging.info("Data point is already present in historical dataset.")

  def update_historical_data(self, ig:ig_package.IG, groups:list[InstrumentGroup] = []) -> None:
    """ Updating new historical data on instruments being tracked with the BacktestingServer.
    
        Parameters
        ----------
        ig: ig_package.IG
          IG object to interact with IG Group's API.
        groups: list[InstrumentGroup] = []
          OPTIONAL List of Instrument Groups to update."""
    if len(groups) == 0:
      # Requesting all tracked instruments from the HistoricalDataSummary.
      self.cursor.execute("SELECT InstrumentName, Epic FROM HistoricalDataSummary WHERE LiveTracking=True;")
      results = self.cursor.fetchall()
    else:
      # Requesting specific groups.
      results = []
      for group in groups:
        self.cursor.execute("SELECT InstrumentName, Epic FROM HistoricalDataSummary WHERE LiveTracking=True and InstrumentGroup='{}';".format(group.name))
        results.extend(self.cursor.fetchall())

    # Getting tracked names and epics.
    tracked_names = []
    tracked_epics = []
    for instrument in results:
      tracked_names.append(instrument[0])
      tracked_epics.append(instrument[1])
    
    for index,name in enumerate(tracked_names):
      # Getting previous datetime.
      self.cursor.execute(f'SELECT DatetimeIndex FROM {name.replace(" ","_")}_HistoricalDataset ORDER BY DatetimeIndex DESC LIMIT 1;')
      previous_datetime = self.cursor.fetchall()
      # Getting instrument.
      instrument = ig_package.Instrument(epic=tracked_epics[index],IG_obj=ig)
      # Checking if previous datetime.
      if len(previous_datetime) == 0:
        # Uploading initial backtesting range of data.
        self._upload_clean_historical_data(instrument)
      else:
        previous_datetime = str(previous_datetime[0][0]).replace("-",":").replace(" ","-")
        previous_datetime = datetime.strptime(previous_datetime,"%Y:%m:%d-%H:%M:%S")
        # Uploading on existing historical data.
        self._upload_on_existing_historical_data(instrument,previous_datetime)
 
  def _check_historical_data_summary_exists(self) -> bool:
    """ Checking if the historical data summary table exists on the MySQL server. Handles this by requesting entire table and checking for response.
        
        Returns
        -------
        bool
          Depending on whether the summary table exists or not."""
    try:
      self.cursor.execute('SELECT * FROM HistoricalDataSummary;')
      logger.info("Historical Data Summary exists.")
      return True
    except:
      logger.info("Historical Data Summary does not exist.")
      return False

  def _check_instrument_in_historical_data(self, instrument:ig_package.Instrument) -> bool:
    """ Checking if instrument is already in historical data.
        
        Parameters
        ----------
        instrument: ig_package.Instrument
          Instrument to be checked.
        
        Returns
        -------
        bool
          Boolean depending if instrument is present in historical data."""
    # Checking historical data summary for instrument.
    self.cursor.execute(f'SELECT * FROM HistoricalDataSummary WHERE Epic="{instrument.epic}";')
    result = self.cursor.fetchall()
    if len(result) == 0:
      logger.info(f"Instrument ({instrument.name}) could not be found in the historical data summary.")
      return False
    else:
      logger.info(f"Instrument ({instrument.name}) is already in the historical data summary.")
      return True

  def _create_historical_data_summary(self) -> None:
    """ Creating the historical data summary on the MySQL server."""
    try:
      self.cursor.execute('CREATE TABLE HistoricalDataSummary (\
      ID INT NOT NULL AUTO_INCREMENT,\
      InstrumentName VARCHAR(20),\
      Epic VARCHAR(100),\
      LiveTracking BOOL DEFAULT False,\
      InstrumentGroup SET("") DEFAULT NULL,\
      PRIMARY KEY (ID)\
      );')
      logger.info("Created Historical Data Summary.")
    except:
      logger.info("Failed to create Historical Data Summary.")

  def _add_historical_data_summary(self, instrument: ig_package.Instrument, live_tracking:bool=False, groups: list[InstrumentGroup] = []) -> None:
    """ Adding instrument to the historical data summary and creating new table for historical data.
    
        Parameters
        ----------
        instrument: ig_package.Instrument
          Instrument to add to the historical data summary.
        live_tracking: bool
          OPTIONAL Enable/disable live tracking of instrument.
        groups: list[InstrumentGroups]
          OPTIONAL List of Instrument Groups."""
    logger.info("Adding instrument to HistoricalDataSummary and creating a new table.")
    # Creating string for Instrument Groups.
    if len(groups) != 0:
      group_string = ""
      for group in groups:
        group_string += f"{group.name},"
      group_string = f"{group_string[:-1]}"
      # Adding instrument to historical data summary.
      self.cursor.executemany(f"INSERT INTO HistoricalDataSummary (InstrumentName, Epic, LiveTracking, InstrumentGroup) VALUES (%s, %s, %s, %s)", [(instrument.name, instrument.epic, live_tracking, group_string)])
    else:
      # Adding instrument to historical data summary.
      self.cursor.executemany(f"INSERT INTO HistoricalDataSummary (InstrumentName, Epic, LiveTracking) VALUES (%s, %s, %s)", [(instrument.name, instrument.epic, live_tracking)])
    # Creating new table for storing historical data.
    new_name = instrument.name.replace(" ","_")
    self.cursor.execute(f"CREATE TABLE {new_name}_HistoricalDataset (\
    DatetimeIndex DATETIME NOT NULL,\
    Open FLOAT(20),\
    High FLOAT(20),\
    Low FLOAT(20),\
    Close FLOAT(20),\
    PRIMARY KEY (DatetimeIndex)\
    );")
  
  def _upload_clean_historical_data(self, instrument: ig_package.Instrument) -> None:
    """ Uploading historical data for an instrument with no previous data.
        
        Parameters
        ----------
        instrument: ig_package.Instrument
          Instrument to upload data for."""
    # Getting current time.
    current_epoch = time.time()
    current_datetime = datetime.fromtimestamp(current_epoch)
    # Listing resolutions.
    resolutions = [
      ("MONTH", 31 * 24 * 60 * 60),
      ("WEEK", 7 * 24 * 60 * 60),
      ("DAY", 24 * 60 * 60),
      ("HOUR_4", 4 * 60 * 60),
      ("HOUR_3", 3 * 60 * 60),
      ("HOUR_2", 2 * 60 * 60),
      ("HOUR",  60 * 60),
    ]
    for resolution in resolutions:
      # Calculating start time and end time
      start_time = current_epoch - 10 * resolution[1]
      start_time = datetime.fromtimestamp(start_time)
      start_time_str = start_time.strftime("%Y:%m:%d-%H:%M:%S")
      end_time_str = current_datetime.strftime("%Y:%m:%d-%H:%M:%S")
      # Getting historical prices.
      historical_data = instrument.get_historical_prices(resolution[0],start=start_time_str,end=end_time_str)
      # Uploading data.
      self.upload_instrument(instrument,dataset=historical_data)

  def _upload_on_existing_historical_data(self, instrument:ig_package.Instrument, previous_datetime: datetime) -> None:
    """ Uploading up-to-date historical data on instrument already with existing historical data.
    
    Parameters
    ----------
    instrument: ig_package.Instrument
      Instrument for uploading recent historical data on.
    previous_datetime: datetime
      Datetime of last historical price."""
    # Getting current time.
    current_epoch = time.time()
    current_datetime = datetime.fromtimestamp(current_epoch)
    current_str = current_datetime.strftime("%Y:%m:%d-%H:%M:%S")
    # Getting previous datetime as epoch.
    previous_epoch_time = previous_datetime.timestamp()
    previous_str = previous_datetime.strftime("%Y:%m:%d-%H:%M:%S")
    # Listing resolutions.
    resolutions = [
      ("MINUTE", 60),
      ("MINUTE_15", 15*60),
      ("HOUR", 60*60),
      ("HOUR_4", 4*60*60),
      ("DAY", 24*60*60),
      ("WEEK", 7*24*60*60),
      ("MONTH", 31*24*60*60)
    ]
    # Collecting data for each resolution.
    for resolution in resolutions:
      if current_epoch - previous_epoch_time < 100 * resolution[1]:
        # Getting historical prices.
        historical_data = instrument.get_historical_prices(resolution[0],start=previous_str,end=current_str)
        # Uploading data.
        self.upload_instrument(instrument,dataset=historical_data)

  def add_instrument_group(self, name: str) -> InstrumentGroup | None:
    """ Creating an instrument group list for grouping various instruments together.
    
      Parameters
      ----------
      name: str
        Name of the instrument group.
        
      Returns
      -------
      InstrumentGroup | None
        Instrument group created."""
    # Creating instrument groups table if not present.
    if not self._check_instrument_groups_table():
      self._create_instrument_groups_table()
    # Creating historical data summary.
    if not self._check_historical_data_summary_exists():
      self._create_historical_data_summary()
      
    try:
      # Inserting new group.
      self.cursor.execute(f'INSERT INTO InstrumentGroups (GroupName)\
      VALUES ("{name}");')
      logger.info(f"Added group, {name}, to the instrument groups table.")
      # Creating instrument object.
      new_group = InstrumentGroup(name,self.cursor)
      if not self.instrument_groups:
        self.instrument_groups = [new_group]
      else:
        self.instrument_groups.append(new_group)
      # Updating group data type in historical summary.
      self._update_groups_in_historical_data()
      return new_group
    except:
      logger.info(f"Unable to add, {name}, to the instrument groups table.")
      return None
  
  def del_instrument_group(self, name: str) -> InstrumentGroup | None:
    """ Deleting the instrument group.
    
      Parameters
      ----------
      name: str
        Name of the instrument group.
        
      Returns
      -------
      InstrumentGroup
        Instrument group deleted."""
    # Getting instrument group.
    for instrument_group in self.instrument_groups:
      if instrument_group.name == name:
        # Remove group from server.
        self.cursor.execute(f"DELETE FROM InstrumentGroups WHERE GroupName = '{name}';")
        # Removing instrument group object.
        self.instrument_groups.remove(instrument_group)
        logger.info(f"Successfully removed Instrument Group, {name}.")
        # Updating group data type in historical summary.
        self._update_groups_in_historical_data()
        return instrument_group
    logger.info(f"Unable to find Instrument Group, {name}.")
    return None
    
  def _get_instrument_groups(self) -> list[InstrumentGroup] | None:
    """ Getting all instrument groups from the Backtesting Server.
    
      Returns
      -------
      list[InstrumentGroup] | None
        List of all InstrumentGroups stored on the server or None."""
    try:
      # Getting instrument groups from groups table.
      self.cursor.execute("SELECT GroupName FROM InstrumentGroups;")
      results = self.cursor.fetchall()
      # Creating InstrumentGroup objects.
      instrument_groups = []
      for instrument_group in results:
        new_instrument_group = InstrumentGroup(instrument_group[0],self.cursor)
        if not instrument_groups:
          instrument_groups = [new_instrument_group]
        else:
          instrument_groups.append(new_instrument_group)
      logger.info("Successfully got all Instrument Groups.")
      return instrument_groups
    except:
      logger.info("Could not load Instrument Groups.")
      return None
  
  def _create_instrument_groups_table(self) -> None:
    """ Creating instrument groups table on the database."""
    # Creating instrument groups table.
    try:
      self.cursor.execute('CREATE TABLE InstrumentGroups (\
      ID INT NOT NULL AUTO_INCREMENT,\
      GroupName VARCHAR(20),\
      PRIMARY KEY (ID)\
      );')
      logger.info("Successfully created Instrument Groups table.")
    except:
      logger.info("Unable to create the Instrument Groups table.")

  def _check_instrument_groups_table(self) -> bool:
    """ Checking if instrument groups tables exists within the database.
    
      Returns 
      -------
      bool
        Boolean if instrument groups table exists or not."""
    try:
      # Getting instrument groups from groups table.
      self.cursor.execute("SELECT * FROM InstrumentGroups;")
      logger.info("Successfully found Instrument Groups table in database.")
      return True
    except:
      logger.info("Could not find Instrument Groups table in database.")
      return False
  
  def _update_groups_in_historical_data(self) -> None:
    """ Updating the Historical Data Summary for any new Instrument Groups that have been added."""
    try:
      # Getting instrument groups from the table.
      self.cursor.execute("SELECT GroupName FROM InstrumentGroups;")
      results = self.cursor.fetchall()
      # Creating new query to send.
      if len(results) != 0:
        group_list = ""
        for group in results:
          group_list += f"'{group[0]}',"
      else:
        group_list = "'',"
      self.cursor.execute(f"ALTER TABLE HistoricalDataSummary\
      MODIFY COLUMN InstrumentGroup SET({group_list[:-1]}) DEFAULT NULL;")
      logger.info("Successfully updated the Historical Data Summary.")
    except:
      logger.info("Could not update the Historical Data Summary.")

# - - - - - - - - - - - - - -

class InstrumentGroup():
  """ Class for representing a group of instruments on the Backtesting Server. These instruments can have entire actions performed on them, allowing for easy management of specific instruments."""

  def __init__(self, name:str, cursor:pymysql.cursors.Cursor) -> None:
    self.name: str = name
    self.cursor: pymysql.cursors.Cursor = cursor

    # Getting instrument names.
    self.instrument_names: list[str] = self._get_instrument_names()

  def _get_instrument_names(self) -> list[str] | None:
    """ Getting all instrument names associated with the Instrument Group.
    
      Returns
      -------
      list[str] | None
        List of instrument names or None."""
    try:
      # Selecting instrument names with group tag.
      self.cursor.execute('Select InstrumentName from HistoricalDataSummary WHERE InstrumentGroup = {}'.format(self.name))
      results = self.cursor.fetchall()
      # Creating list of names.
      instrument_names: list[str] = []
      for instrument_name in results:
        instrument_names.append(instrument_name[0])
      logger.info("Successfully got all instrument names for {}.".format(self.name))
      return instrument_names
    except:
      logger.info("Could not get instrument names for {}.".format(self.name))
      return None
    
# - - - - - - - - - - - - - -

if __name__ == "__main__":

  with open("logging_config.json") as f:
    config_dict = json.load(f)
    logging.config.dictConfig(config_dict)

  backtesting = BacktestingServer()
  channel,cursor = backtesting.connect()
  cursor.execute("DESC Strategies;")
  result = cursor.fetchall()
  print(result)