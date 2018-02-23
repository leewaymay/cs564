SELECT DISTINCT itemID FROM Item WHERE Currently = (SELECT MAX(Currently) FROM Item)
