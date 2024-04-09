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

  def connect(self) -> tuple[paramiko.Channel, pymysql.cursors.Cursor]:
    """ Connecting to MySQL server using SSH.
        
        Returns
        -------
        paramiko.Channel
          Channel to MySQL server.
        pymysql.cursors.Cursor
          Cursor to execute SQL queries."""
    # Connecting through SSH.
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logger.info("Connecting to server: {}".format(self.standard_details["server"]))
    ssh.connect(self.standard_details["server"],username=self.standard_details["username"],password=self.standard_details["password"])

    # Connecting to MySQL server.
    transport = ssh.get_transport()
    channel = transport.open_channel("direct-tcpip", ('127.0.0.1', 3306), ('localhost', 3306))
    c = pymysql.connect(database='trading_bot', user=self.sql_details['username'], password=self.sql_details['password'], defer_connect=True)
    c.connect(channel)

    # Getting cursor to execute commands.
    cursor = c.cursor()
    return channel, cursor

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