import web
import datetime

db = web.database(dbn='sqlite',
        db='AuctionBase.db' #TODO: add your SQLite database filename
    )

######################BEGIN HELPER METHODS######################

# Enforce foreign key constraints
# WARNING: DO NOT REMOVE THIS!
def enforceForeignKey():
    db.query('PRAGMA foreign_keys = ON')

# initiates a transaction on the database
def transaction():
    return db.transaction()
# Sample usage (in auctionbase.py):
#
# t = sqlitedb.transaction()
# try:
#     sqlitedb.query('[FIRST QUERY STATEMENT]')
#     sqlitedb.query('[SECOND QUERY STATEMENT]')
# except Exception as e:
#     t.rollback()
#     print str(e)
# else:
#     t.commit()
#
# check out http://webpy.org/cookbook/transactions for examples

# returns the current time from your database
def getTime():
    query_string = 'select Time from CurrentTime'
    results = query(query_string)
    # alternatively: return results[0]['time']
    return results[0].Time

# modify the current time in your database
def changeTime(newTime):
    # update the time
    query_string = 'update CurrentTime set time = $newTime'
    try:
        db.query(query_string, {'newTime': newTime})
    except Exception as e:
        print("catch exception ", e.message)
        return -1, "Failed to update time"
    return 0, None
# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will throw an Exception!
def getItemById(item_id):
    query_string = 'select * from Items where itemID = $itemID'
    result = query(query_string, {'itemID': item_id})
    if len(result) == 0:
        return None
    return result[0]


def addBid(itemID, userID, price):
    currTime = getTime()
    query_string = 'insert into Bids values ($itemID, $userID, $price, $currTime)'
    try:
        result = db.query(query_string, {'itemID': itemID, 'userID': userID, 'price': price, 'currTime': currTime})
    except Exception as e:
        print("catch error ", e.message)
        return -1, "Can not add bid."

    print(result, "type ", type(result))
    return 0, None


def searchBid(itemID, userID, minPrice, maxPrice, itemDesp, category, status):
    conditions = ["b.itemID = i.itemID", "b.itemID = c.itemID"]
    if itemID:
        conditions.append('b.itemID = ' + itemID)
    if userID:
        conditions.append('b.userID = ' + userID)
    if minPrice:
        conditions.append('b.amount >=' + minPrice)
    if maxPrice:
        conditions.append('b.amount <=' + maxPrice)
    if category:
        conditions.append('c.category = ' + category)
    if itemDesp:
        conditions.append('i.description like \'%{}%\''.format(itemDesp))

    query_string = 'select * from Bids b, Items i, Categories c where ' + ' and '.join(conditions)
    candidates = query(query_string)
    results = []
    if status == 'all':
        return candidates, None
    else:
        for candidate in candidates:
            if checkTime(candidate.Started, candidate.Ends, status):
                results.append(candidate)
    return results, None

def getDetails(keys):
    seg = keys.split('&')
    itemID = seg[0]
    price = seg[-1]
    userID = '&'.join(seg[1:-1])
    query_string1 = 'select * from Items where itemID = ' + itemID
    query_string2 = 'select Time from Bids where userID = \'' + userID + '\' and itemID = ' + itemID + ' and amount = ' + price
    query_string3 = 'select Category from Categories where itemID = ' + itemID
    result = [{'Status': 'Open'}]
    result += query(query_string1) + query(query_string2) + query(query_string3)
    return result


def checkTime(startTime, endTime, status):
    currTime = getTime()
    currTime = datetime.datetime.strptime(currTime, "%Y-%m-%d %H:%M:%S")
    startTime = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")
    endTime = datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S")
    if status == "open":
        return currTime >= startTime and currTime < endTime
    if status == 'close':
        return currTime >= endTime
    if status == 'notStarted':
        return currTime < startTime

# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}):
    return list(db.query(query_string, vars))

#####################END HELPER METHODS#####################

#TODO: additional methods to interact with your database,
# e.g. to update the current time
