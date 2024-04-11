/* Module for all queries related to the Historical Data Summary table.*/

-- Creating historical data summary.
CREATE TABLE HistoricalDataSummary (
ID INT NOT NULL AUTO_INCREMENT,
InstrumentName VARCHAR(20),
Epic VARCHAR(100),
PRIMARY KEY (ID)
);

-- Adding to table.
INSERT INTO HistoricalDataSummary (InstrumentName, Epic)
VALUES ("FTSE 100", "UK.FTSE.100");

-- Deleting row from table based on ID.
DELETE FROM HistoricalDataSummary WHERE ID=1;

-- Deleting row from table based on instrument name.
DELETE FROM HistoricalDataSummary WHERE InstrumentName="FTSE 100";

-- Deleting row from table based on epic.
DELETE FROM HistoricalDataSummary WHERE Epic="UK.FTSE.100";

-- Removing table.
DROP TABLE HistoricalDataSummary;

-- Getting table summary.
SELECT * FROM HistoricalDataSummary;