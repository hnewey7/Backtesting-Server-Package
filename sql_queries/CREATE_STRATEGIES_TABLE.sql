-- Creating Strategies table
CREATE TABLE Strategies (
[ID] INT NOT NULL IDENTITY(1, 1),
[Name] VARCHAR(20) NOT NULL,
[Description] TEXT NOT NULL,
[Author] VARCHAR(20) NOT NULL,
PRIMARY KEY (ID)
);