'''
Module for unit testing the Historical Price Gap class.

Created on Sunday 19th May 2024.
@author: Harry New

'''

import pytest
from datetime import datetime

import ig_package
from src.backtesting_server import HistoricalPriceGap
from SERVER_DETAILS import get_standard_server_details, get_mysql_server_details, get_ig_details

# - - - - - - - - - - - - - - - - - -

@pytest.mark.parametrize(["instrument_name","start_datetime","end_datetime","opening_hours","closing_hours"],[
  ("FTSE 100",datetime(2024,5,19,11),datetime(2024,5,19,12),None,None),
  ("BP Group PLC",datetime(2024,5,19,11),datetime(2024,5,19,12),"08:00","16:30")])
def test_init(instrument_name: str, start_datetime: datetime, end_datetime: datetime, opening_hours: str, closing_hours: str) -> None:
  """ Testing the initialisation.
    
    Parameters
    ----------
    instrument_name: str
      Name of instrument to init HistoricalPriceGap object with.
    start_datetime: datetime
      Start datetime.
    end_datetime: datetime
      End datetime.
    opening_hours: str
      Opening hours of instrument.
    closing_hours: str
      Closing hours of instrument."""
  # Getting test instrument.
  ig_details = get_ig_details()
  ig = ig_package.IG(API_key=ig_details['key'],username=ig_details['username'],password=ig_details['password'],acc_type=ig_details["acc_type"],acc_number=ig_details["acc_number"])
  test_instrument = ig.search_instrument(instrument_name)

  # Creating historical price gap.
  price_gap = HistoricalPriceGap(test_instrument,start_datetime,end_datetime)
  
  # Checking attributes.
  assert hasattr(price_gap,"instrument")
  assert hasattr(price_gap,"open_time")
  assert hasattr(price_gap,"close_time")
  assert hasattr(price_gap,"start_datetime")
  assert hasattr(price_gap,"end_datetime")
  assert hasattr(price_gap,"time_range")

  # Checking values of attributes.
  assert price_gap.instrument == test_instrument
  assert price_gap.open_time == opening_hours
  assert price_gap.close_time == closing_hours
  assert price_gap.start_datetime == start_datetime
  assert price_gap.end_datetime == end_datetime