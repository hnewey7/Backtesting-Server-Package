'''
Module for uploading backtesting results to MySQL server.

Created on Tuesday 19th March 2024.
@author: Harry New

'''

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
    # Getting details.
    self.standard_details = standard_details
    self.sql_details = sql_details
    
    self.channel: paramiko.Channel = None
    self.cursor: pymysql.cursors.Cursor = None

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
      return channel, cursor
    except Exception as e:
      logger.info("Unable to connect to MySQL server.")
      raise e

  def upload_historical_data(self, instrument:ig_package.Instrument, dataset:pd.DataFrame) -> None:
    """ Uploading historical data to the backtesting server.
    
        Parameters
        ----------
        instrument: ig_package.Instrument
          Instrument the historical data corresponds to.
        dataset: pd.DataFrame
          DataFrame containing the data to be uploaded."""
    # Checking if historical data summary exists.
    if not self._check_historical_data_summary_exists():
      # Creating summary table.
      self._create_historical_data_summary()
    # Checking if data is already present.
    if not self._check_instrument_in_historical_data(instrument):
      # Adding new instrument.
      self._add_historical_data_summary(instrument)

    # Filtering out NaN values.
    dataset = dataset.dropna()
    # Inserting each row into database.
    logger.info("Inserting data into server-side dataset.")
    for data_point in dataset.index:
      insert_statement = f'INSERT INTO {instrument.name.replace(" ","_")}_HistoricalDataset (DatetimeIndex, Open, High, Low, Close) VALUES (%s, %s, %s, %s, %s)'
      values = [
        (str(data_point), float(dataset["Open"][data_point]), float(dataset["High"][data_point]), float(dataset["Low"][data_point]), float(dataset["Close"][data_point])),
      ]
      self.cursor.executemany(insert_statement, values)

  def _check_historical_data_summary_exists(self) -> bool:
    """ Checking if the historical data summary table exists on the MySQL server.
        
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
      PRIMARY KEY (ID)\
      );')
      logger.info("Created Historical Data Summary.")
    except:
      logger.info("Failed to create Historical Data Summary.")

  def _add_historical_data_summary(self, instrument: ig_package.Instrument) -> None:
    """ Adding instrument to the historical data summary and creating new table for historical data.
    
        Parameters
        ----------
        instrument: ig_package.Instrument
          Instrument to add to the historical data summary."""
    logger.info("Adding instrument to HistoricalDataSummary and creating a new table.")
    # Adding instrument to historical data summary.
    self.cursor.execute(f"INSERT INTO HistoricalDataSummary (InstrumentName, Epic)\
    VALUES ('{instrument.name}', '{instrument.epic}');")
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