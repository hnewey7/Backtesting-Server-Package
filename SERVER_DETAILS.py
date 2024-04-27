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

# IG Group details.
ig_details = {
  "key":"",
  "username":"",
  "password":""
}

# - - - - - - - - - - - - - - - -

def get_standard_server_details() -> dict:
  """ Getting standard SSH details.
      
      Returns
      -------
      dict
        Contains 'server', 'username' and 'password'."""
  return standard_server_details

def get_mysql_server_details() -> dict:
  """ Getting MySQL server details.

    Returns
    -------
    dict
      Contains 'server', 'username' and 'password'."""
  return mysql_server_details

def get_ig_details() -> dict:
  """ Getting IG Group details.
  
    Returns
    -------
    dict
      Contains 'key', 'username' and 'password'."""
  return ig_details