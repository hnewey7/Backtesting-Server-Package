/* Module for all queries relating to a single historical dataset. */

-- Creating historical dataset.
CREATE TABLE HistoricalDataset (
DatetimeIndex DATETIME NOT NULL,
Open FLOAT(12),
High FLOAT(12),
Low FLOAT(12),
Close FLOAT(12),
PRIMARY KEY (DatetimeIndex)
);

-- Adding to table.
INSERT INTO HistoricalDataset (DatetimeIndex, Open, High, Low, Close)
VALUES ("2024-04-11 21:21:00", 100, 120, 90, 110);

-- Getting historical dataset.
SELECT * FROM HistoricalDataset;