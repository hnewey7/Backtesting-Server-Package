/* Module for all queries related to the Historical Data Summary table.*/

-- Creating historical data summary.
CREATE TABLE HistoricalDataSummary (
ID INT NOT NULL AUTO_INCREMENT,
InstrumentName VARCHAR(20),
Epic VARCHAR(100),
LiveTracking BOOL DEFAULT False,
InstrumentGroup SET("") DEFAULT NULL,
PRIMARY KEY (ID)
);

-- Adding to table.
INSERT INTO HistoricalDataSummary (InstrumentName, Epic, LiveTracking)
VALUES ("FTSE 10", "UK.FTSE.10", True);

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

-- Checking if instrument is present through the epic.
SELECT * FROM HistoricalDataSummary WHERE Epic="UK.FTSE.100";

-- Getting tracked instruments.
SELECT Epic FROM HistoricalDataSummary WHERE LiveTracking=True;

-- Altering groups column depending on table.
ALTER TABLE HistoricalDataSummary
MODIFY COLUMN InstrumentGroup SET("ExampleStrategy") DEFAULT NULL;

-- Adding group to instrument.
UPDATE HistoricalDataSummary 
SET InstrumentGroup = "ExampleStrategy"
WHERE ID = 1;

-- Removing group from instrument.
UPDATE HistoricalDataSummary 
SET InstrumentGroup = ""
WHERE ID = 1;

-- Adding InstrumentGroup column to existing table.
ALTER TABLE HistoricalDataSummary
ADD COLUMN InstrumentGroup SET("") DEFAULT NULL;