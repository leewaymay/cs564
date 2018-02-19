drop table if exists Item;
drop table if exists User;
drop table if exists Category;
drop table if exists Bid;

create table Item (
	ItemID INTEGER PRIMARY KEY,
	Name TEXT,
	Currently REAL,
	Buy_Price REAL,
	First_Bid REAL,
	Number_of_Bids INTEGER,
	Started TEXT,
	Ends TEXT,
	Description TEXT,
	SellerID TEXT,
	FOREIGN KEY (SellerID) REFERENCES User(UserID) 
);

create table User (
	UserID TEXT PRIMARY KEY,
	Rating INTEGER,
	Location TEXT,
	Country TEXT
);

create table Category (
	ItemID INTEGER,
	Category TEXT,
	FOREIGN KEY (ItemID) REFERENCES Item(ItemID) 
);

create table Bid (
	ItemID INTEGER,
	BidTime TEXT,
	Amount REAL,
	BidderID TEXT,
	FOREIGN KEY (ItemID) REFERENCES Item(ItemID), 
	FOREIGN KEY (BidderID) REFERENCES User(UserID) 
);

