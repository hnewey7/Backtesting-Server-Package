'''
Storing details of MySQL server.

Created on Friday 22nd March 2024.
@author: Harry New

'''

# SSH details.
standard_server_details = {
  "server":"",
  "username":"",
  "password":""
}

# MySQL details.
mysql_server_details = {
  "server":"",
  "username":"",
  "password":""
}

# - - - - - - - - - - - - - - - -

def get_standard_server_details() -> dict:
  """ Getting standard SSH details."""
  return standard_server_details

def get_mysql_server_details() -> dict:
  """ Getting MySQL server details."""
  return mysql_server_details