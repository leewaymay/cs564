
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
import glob
from json import loads
from re import sub

columnSeparator = "|"


f_Item = open('RelationItem.dat','w')
f_Bid = open('RelationBid.dat','w')
f_Category = open('RelationCategory.dat', 'w')
f_User = open('RelationUser.dat', 'w')



# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

def addQuote(s): 
    return '"' + sub(r'"', '""', s) + '"'
"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        #print "lalala"
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        
        for item in items:
            #f_Item.write([item['ItemID'], item['Ends'], item['First_Bid'], item['Name'], item['Started'], item['Number_of_Bids'],
            #    item['Currently'],  item['Description']])
            to_write = ''
            keys = ['ItemID', 'Name', 'Currently', 'Buy_Price', 'First_Bid', 'Number_of_Bids', 'Started', 'Ends', 'Description']
            for key in keys:
                record = 'NULL' if (key not in item.keys() or (not item[key])) else item[key]
                if (key == 'Ends' or key == 'Started'):
                    record = transformDttm(record) if record != 'NULL' else record 
                if (key == 'Currently' or key == 'Buy_Price' or key == 'First_Bid'):
                    record = transformDollar(record) if record != 'NULL' else record 
                to_write += addQuote(record) + columnSeparator
                #to_write += record + columnSeparator                
            to_write = to_write + item['Seller']['UserID'] + '\n'
            f_Item.write(to_write)

            f_User.write(addQuote(item['Seller']['UserID'])+columnSeparator+addQuote(item['Seller']['Rating'])+columnSeparator+addQuote(item['Location'])+columnSeparator
                + addQuote(item['Country'])+'\n')
            if item['Bids']:
                for subitem in item['Bids']:
                    if subitem['Bid'] and subitem['Bid']['Bidder']:
                        location = "NULL" if ('Location' not in subitem['Bid']['Bidder'].keys() or (not subitem['Bid']['Bidder']['Location'])) else subitem['Bid']['Bidder']['Location']
                        country = "NULL" if ('Country' not in subitem['Bid']['Bidder'].keys() or (not subitem['Bid']['Bidder']['Country'])) else subitem['Bid']['Bidder']['Country']
                        userid = "NULL" if not subitem['Bid']['Bidder']['UserID'] else subitem['Bid']['Bidder']['UserID']
                        rating = "NULL" if not subitem['Bid']['Bidder']['Rating'] else subitem['Bid']['Bidder']['Rating']

                        f_User.write(addQuote(userid)+columnSeparator+addQuote(rating)+columnSeparator+addQuote(location)+columnSeparator+addQuote(country)+'\n')
                    if subitem['Bid']:
                        itemid = item['ItemID']
                        time = transformDttm(subitem['Bid']['Time']) if subitem['Bid']['Time'] else 'NULL'
                        amount = transformDollar(subitem['Bid']['Amount']) if subitem['Bid']['Amount'] else 'NULL'
                        bidder = "NULL" if not subitem['Bid']['Bidder']['UserID'] else subitem['Bid']['Bidder']['UserID']

                        f_Bid.write(addQuote(itemid) + columnSeparator + addQuote(time) + columnSeparator + addQuote(amount) + columnSeparator + addQuote(bidder) + '\n')

            if item['Category']:
                for cat in item['Category']:
                    f_Category.write(addQuote(item['ItemID']) + columnSeparator + addQuote(cat) + '\n')
            #print item.keys()
            """
            TODO: traverse the items dictionary to extract information from the
            given `json_file' and generate the necessary .dat files to generate
            the SQL tables based on your relation design
            """
            #print items.keys()
            #pass

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    
    if len(argv) < 2:
        for f in glob.glob('*.json'):
            if isJson(f):
                parseJson(f)
                print "Success parsing " + f
    else:
    # loops over all .json files in the argument
        for f in argv[1:]:
            if isJson(f):
                parseJson(f)
                print "Success parsing " + f
    f_Item.close()
    f_User.close()
    f_Category.close()
    f_Bid.close()

if __name__ == '__main__':
    main(sys.argv)
