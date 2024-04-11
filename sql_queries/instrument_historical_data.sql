/* Module for all queries relating to a single Instrument Historical Data table.*/

-- Creating instrument historical data table.
CREATE TABLE InstrumentHistoricalData (
ID INT NOT NULL AUTO_INCREMENT,
Resolution ENUM('SECOND', 'MINUTE', 'MINUTE_2', 'MINUTE_3', 'MINUTE_5', 'MINUTE_10', 'MINUTE_15', 'MINUTE_30', 'HOUR', 'HOUR_2', 'HOUR_3', 'HOUR_4', 'DAY', 'WEEK', 'MONTH'),
StartDatetime DATETIME NOT NULL,
EndDatetime DATETIME DEFAULT CURRENT_TIMESTAMP,
Active BOOLEAN DEFAULT TRUE,
PRIMARY KEY (ID)
);

-- Adding to table.
INSERT INTO InstrumentHistoricalData (Resolution, StartDatetime, Active)
VALUES ("SECOND", "2024-01-01 00:00:00", True);

-- Deleting row from table by ID.
DELETE FROM InstrumentHistoricalData WHERE ID=1;

-- Removing table.
DROP TABLE InstrumentHistoricalData;

-- Getting historical data table.
SELECT * FROM InstrumentHistoricalData;