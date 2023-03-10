'''
'''
import sys
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_dir)

import logging
from flask import Flask # Response, make_response
from findabug import FindABug
from time import perf_counter
from sqlalchemy import create_engine
from exceptions import FindABugError, FindABugQueryError
from query import FindABugQuery
import traceback

# Instantiate and configure the logger. 
logging.basicConfig(filename='find-a-bug.log', filemode='a')
logger = logging.getLogger('find-a-bug')

# Tells Flask the name of the current module. 
app = Flask(__name__)

######################################################################
host = 'localhost'
dialect = 'mariadb'
driver = 'mariadbconnector'
user = 'root'
pwd = 'Doledi7-Bebyno2'
dbname = 'findabug'

url = f'{dialect}+{driver}://{user}:{pwd}@{host}/{dbname}'
# Hopefully means that only one engine is created.
engine = create_engine(url)
######################################################################

@app.errorhandler(FindABugError)
@app.errorhandler(FindABugQueryError)
def handle_error(err):
    '''
    Error handling for issues which involve the client-side query or the
    host-side code (so basically any exception which occurs. 
    '''
    return str(err), err.status_code

@app.errorhandler(Exception)
def handle_unknown_error(err):
    '''
    Error handling when unanticipated exceptions are raised. 
    '''
    report = traceback.format_exc().split('\n')
    return '<br>'.join(report), 500, {'Content-Type':'text/html'}


@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200


@app.route('/info')
def database_info():
    # Start a connection to the FindABug database. 
    
    fab = FindABug(engine)
    info_df = fab.info()
    
    return info_df.to_json(orient='records')
 

@app.route('/<string:url_query>/<url_options>')
def query_database(url_query=None, url_options=None):

    t_init = perf_counter()

    fab = FindABug(engine)
    Q = FindABugQuery(url_query, url_options)
    
    query, csv = fab.query_database(Q) 
    
    t_final = perf_counter()

    def response():
        yield f'{len(csv)} results in {t_final - t_init} seconds <br>'
        
        # Also want to print the raw SQL query. 
        yield '<br>'
        yield str(query.statement.compile())
        yield '<br>'
        yield '<br>'

        for row in csv:
            yield row + '<br>'
    
    return response(), 200, {'Content-Type':'text/html'}

    


