#!/usr/bin/env python

import sys; sys.path.insert(0, 'lib') # this line is necessary for the rest
import os                             # of the imports to work!

import web
import sqlitedb
from jinja2 import Environment, FileSystemLoader
from datetime import datetime

###########################################################################################
##########################DO NOT CHANGE ANYTHING ABOVE THIS LINE!##########################
###########################################################################################

######################BEGIN HELPER METHODS######################

# helper method to convert times from database (which will return a string)
# into datetime objects. This will allow you to compare times correctly (using
# ==, !=, <, >, etc.) instead of lexicographically as strings.

# Sample use:
# current_time = string_to_time(sqlitedb.getTime())

def string_to_time(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

# helper method to render a template in the templates/ directory
#
# `template_name': name of template file to render
#
# `**context': a dictionary of variable names mapped to values
# that is passed to Jinja2's templating engine
#
# See curr_time's `GET' method for sample usage
#
# WARNING: DO NOT CHANGE THIS METHOD
def render_template(template_name, **context):
    extensions = context.pop('extensions', [])
    globals = context.pop('globals', {})

    jinja_env = Environment(autoescape=True,
            loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
            extensions=extensions,
            )
    jinja_env.globals.update(globals)

    web.header('Content-Type','text/html; charset=utf-8', unique=True)

    return jinja_env.get_template(template_name).render(context)

#####################END HELPER METHODS#####################

urls = ('/currtime', 'curr_time',
        '/selecttime', 'select_time',
        '/', 'homepage',
        '/search', 'search',
        '/viewMore/(.*)', 'viewMore',
        '/add_bid', 'add_bid'
        # TODO: add additional URLs here
        # first parameter => URL, second parameter => class name
        )

class homepage:
    # A simple GET request, to '/'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        return render_template('app_base.html')

class add_bid:
    # A simple GET request, to '/add_bid'
    #

    def GET(self):
        return render_template('add_bid.html')

    def POST(self):
        post_params = web.input()
        itemID = post_params['itemID']
        userID = post_params['userID']
        price = post_params['price']
        result, msg = sqlitedb.addBid(itemID, userID, price)
        if result == -1:
            return render_template('add_bid.html', add_result=False, message=msg)
        else:
            return render_template('add_bid.html', add_result=True)

class search:
    # A simple GET request, to '/'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        return render_template('search.html')

    def POST(self):
        post_params = web.input()
        itemID = post_params['itemID']
        minPrice = post_params['minPrice']
        maxPrice = post_params['maxPrice']
        itemDesp = post_params['itemDesp']
        category = post_params['category']
        status = post_params['status']
        if itemID or minPrice or maxPrice or itemDesp or category:
            result, msg = sqlitedb.searchAuctions(itemID, minPrice, maxPrice, itemDesp, category, status)
            message = 'Search performed. '+msg
        else:
            result = []
            message = 'No inputs given'
            print("Nothinginput");
        return render_template('search.html', search_result=result, message=message)

class viewMore:
    def GET(self, keys):
        result = sqlitedb.getDetails(keys)
        return render_template('view_more.html', search_result=result)

class curr_time:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        current_time = sqlitedb.getTime()
        return render_template('curr_time.html', time = current_time)

class select_time:
    # Aanother GET request, this time to the URL '/selecttime'
    def GET(self):
        return render_template('select_time.html')

    # A POST request
    #
    # You can fetch the parameters passed to the URL
    # by calling `web.input()' for **both** POST requests
    # and GET requests
    def POST(self):
        post_params = web.input()
        MM = post_params['MM']
        dd = post_params['dd']
        yyyy = post_params['yyyy']
        HH = post_params['HH']
        mm = post_params['mm']
        ss = post_params['ss']
        enter_name = post_params['entername']


        selected_time = '%s-%s-%s %s:%s:%s' % (yyyy, MM, dd, HH, mm, ss)
        update_message = '(Hello, %s. Previously selected time was: %s.)' % (enter_name, selected_time)
        # TODO: save the selected time as the current time in the database
        result, msg = sqlitedb.changeTime(selected_time)
        if result == -1:
            return render_template('select_time.html', message=msg)

        # Here, we assign `update_message' to `message', which means
        # we'll refer to it in our template as `message'
        return render_template('select_time.html', message = update_message)

###########################################################################################
##########################DO NOT CHANGE ANYTHING BELOW THIS LINE!##########################
###########################################################################################

if __name__ == '__main__':
    web.internalerror = web.debugerror
    app = web.application(urls, globals())
    app.add_processor(web.loadhook(sqlitedb.enforceForeignKey))
    app.run()
