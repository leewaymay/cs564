SELECT Count(*) FROM (SELECT DISTINCT I.sellerID FROM Item I, Bid B WHERE I.SellerID = B.BidderID);
