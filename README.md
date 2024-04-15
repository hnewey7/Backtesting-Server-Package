# Python Package for transferring backtesting data to a MySQL server.

## A package allowing transferring of historical price data and strategy results to a MySQL server.

This project is intended to allow users to connect to a MySQL server, transferring backtesting data between the two. Historical data can be uploaded and stored for later use, meaning backtesting won't be limited by the trading API.

Key functions included in this package are:
* Connecting to the MySQL server.
* Uploading historical data.

![Class diagram](https://github.com/hnewey7/Backtesting-Server-Package/blob/main/class_diagram.png?raw=true)

## How to utilise this package

1. Use ```pip install backtesting-server``` and ```pip install ig-package``` in the command prompt. ***Note: ig-package is not essential but will be extremely useful.***
2. Import the BacktestingServer object into your script:
   ```python
   from backtesting-server import BacktestingServer
   ```
3. Initialise the BacktestingServer object including your standard server details and your MySQL server details.
  ```python
   server = BacktestingServer(
    standard_details = {
      "server": "",
      "username": "",
      "password": ""
   }, sql_details = {
      "server": "",
      "username": "",
      "password": ""
   })
   ```

## How to contribute to this package

This project is fairly specific to my own personal MySQL server and the way I wish to carry out backtesting. However if you do want to contribute to the package, make sure you have your own MySQL server running and have my other IG Package. 

## Fixing bugs

Please make sure to report any bugs found as issues on Github. If you then want to submit a pull request, make sure to reference the issue.

## Future Development

1. Adding methods to upload backtesting strategy results.