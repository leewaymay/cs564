import web
import datetime

db = web.database(dbn='sqlite',
        db='AuctionBase' #TODO: add your SQLite database filename
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
    query_string = 'select Buy_Price, Currently from Items where itemID = $itemID'
    current_price = query(query_string, {'itemID': itemID});
    if current_price:
        print(current_price)
        if current_price[0]['Buy_Price'] and current_price[0]['Buy_Price'] <= current_price[0]['Currently']:
            return -1, "Can not add bid."
    currTime = getTime()
    query_string = 'insert into Bids values ($itemID, $userID, $price, $currTime)'
    try:
        result = db.query(query_string, {'itemID': itemID, 'userID': userID, 'price': price, 'currTime': currTime})
    except Exception as e:
        print("catch error ", e.message)
        return -1, "Can not add bid."

    print(result, "type ", type(result))
    return 0, None


def searchAuctions(itemID, minPrice, maxPrice, itemDesp, category, status):

    conditions = ["i.itemID = c.itemID"]
    conddict = {}
    if itemID:
        conditions.append('i.itemID = $itemID')
        conddict['itemID'] = itemID
    if minPrice:
        conditions.append('i.Currently >= $minPrice')
        conddict['minPrice'] = minPrice
    if maxPrice:
        conditions.append('i.Currently <= $maxPrice')
        conddict['maxPrice'] = maxPrice
    if category:
        conditions.append('c.category = $category')
        conddict['category'] = category
    if itemDesp:
        conditions.append('i.description like $itemDesp')
        conddict['itemDesp'] = "%" + itemDesp + "%"
    query_string = 'select DISTINCT i.itemID, i.Started, i.Ends from Items i, Categories c where ' + ' and '.join(conditions)
    candidates = query(query_string, conddict)
    results = []
    if status == 'all':
        return candidates, "{} auction(s) found".format(len(candidates))
    else:
        for candidate in candidates:
            if getStatus(candidate) == status:
                results.append(candidate)
    return results, "{} auction(s) found".format(len(results))

def getDetails(keys):
    itemID = keys
    query_string1 = 'select * from Items where itemID = $itemID'
    query_string2 = 'select * from Bids where itemID = $itemID order by Amount DESC'
    query_string3 = 'select Category from Categories where itemID = $itemID'
    result = query(query_string1, {'itemID': itemID}) + query(query_string2, {'itemID': itemID}) + \
             [{'Category': ', '.join([c['Category'].encode("ASCII") for c in query(query_string3, {'itemID': itemID})])}]
    status = getStatus(result[0])
    result += [{'Status ': status}]
    if status == 'close':
        if 'Amount' in result[1]:
            result += [{'Winner ': result[1]['UserID']}]
        else:
            result += [{'Winner ': 'None'}]
    return result


def getStatus(instance):
    currTime = getTime()
    currTime = datetime.datetime.strptime(currTime, "%Y-%m-%d %H:%M:%S")
    startTime = datetime.datetime.strptime(instance.Started, "%Y-%m-%d %H:%M:%S")
    endTime = datetime.datetime.strptime(instance.Ends, "%Y-%m-%d %H:%M:%S")
    if currTime > endTime:
        return 'close'
    elif currTime >= startTime and currTime <= endTime:
        query_string = 'select Buy_Price, Currently from Items where itemID = $itemID'
        current_price = query(query_string, {'itemID': instance.ItemID})
        if current_price[0]['Buy_Price'] and current_price[0]['Buy_Price'] <= current_price[0]['Currently']:
            return 'close'
        else:
            return 'open'
    return 'notStarted'




# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}):
    return list(db.query(query_string, vars))

#####################END HELPER METHODS#####################

#TODO: additional methods to interact with your database,
# e.g. to update the current time
