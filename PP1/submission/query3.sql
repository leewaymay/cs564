SELECT count(*) FROM (SELECT COUNT(*) AS countAuc FROM Category GROUP BY itemID) WHERE countAuc = 4
