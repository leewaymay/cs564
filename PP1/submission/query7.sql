SELECT COUNT(*) FROM (SELECT DISTINCT C.Category FROM Category C, Bid B WHERE B.Amount > 100 AND B.itemID = C.itemID)
