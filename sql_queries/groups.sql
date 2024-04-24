/* Module for all queries related to groups of instruments.*/

-- Creating group tables.
CREATE TABLE InstrumentGroups (
ID INT NOT NULL AUTO_INCREMENT,
GroupName VARCHAR(20),
PRIMARY KEY (ID)
);

-- Showing group table.
SELECT * FROM InstrumentGroups;

-- Getting all group names.
SELECT GroupName FROM InstrumentGroups;

-- Removing table.
DROP TABLE InstrumentGroups;

-- Adding new group.
INSERT INTO InstrumentGroups (GroupName)
VALUES ("ExampleStrategy");

-- Removing group.
DELETE FROM InstrumentGroups WHERE GroupName = "ExampleStrategy";