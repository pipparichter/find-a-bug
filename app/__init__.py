'''
'''
import sys
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_dir)

import logging
from flask import Flask, request # Response, make_response
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

def detect_response_type():
    '''
    Detects how to format the response depending on whether or not the request
    comes from the browser or the API.

    returns:
        : sep (str): The newline character to use when generating a response
            string.
        : content_type (str): The type of response content (plain text or html)
    '''
    user_agent = request.headers.get('User-Agent')
    
    sep, content_type = None, None
    if 'python-requests' in user_agent:
        sep = '\n'
        content_type = 'text/plain'
    else:
        sep = '<b>'
        content_type = 'text/html'

    return sep, content_type


@app.errorhandler(FindABugError)
@app.errorhandler(FindABugQueryError)
def handle_error(err):
    '''
    Error handling for issues which involve the client-side query or the
    host-side code (so basically any exception which occurs. 
    '''
    # Log the error. 
    logger.error(str(err))
    return str(err), err.status_code

@app.errorhandler(Exception)
def handle_unknown_error(err):
    '''
    Error handling when unanticipated exceptions are raised. 
    '''
    # Log the error. 
    logger.error(str(err))

    report = traceback.format_exc().split('\n')
    return '<br>'.join(report), 500, {'Content-Type':'text/html'}


@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200


@app.route('/info')
def info():
    # Start a connection to the FindABug database. 
    
    fab = FindABug(engine)
    
    # Adjust some things according to where the request comes from.
    sep, content_type = detect_response_type()
    
    # Log the query to the log file. 
    logger.info('Query to database: DATABASE INFO')
    
    return sep.join(fab.info()), 200, {'Content-Type':content_type}
 

@app.route('/<string:url_query>/<url_options>/count')
def count(url_query=None, url_options=None):
    '''
    '''
    t_init = perf_counter()

    fab = FindABug(engine)
    fabq = FindABugQuery(url_query, url_options)
    q, n = fab.query(fabq) 
    
    t_final = perf_counter()
    
    # Adjust some things according to where the request comes from.
    sep, content_type = detect_response_type()
    
    response = f'Result in {t_final - t_init} seconds' + sep
    response += sep
    response = str(q.statement.compile(bind=engine)) + sep
    response += sep
    response += str(n)
     
    # Log the query to the log file. 
    logger.info(f'Query to database: {str(fabq)}')
 
    return response(), 200, {'Content-Type':content_type}


@app.route('/<string:url_query>/<url_options>')
def query(url_query=None, url_options=None):
    '''
    '''
    
    t_init = perf_counter()

    fab = FindABug(engine)
    fabq = FindABugQuery(url_query, url_options)
    q, csv = fab.query(fabq) 
    
    t_final = perf_counter()
    
    # Adjust some things according to where the request comes from.
    sep, content_type = detect_response_type()
    
    def response():
        yield f'{len(csv)} results in {t_final - t_init} seconds' + sep
        
        # Also want to print the raw SQL query. 
        yield sep
        yield str(q.statement.compile(bind=engine))
        yield sep
        yield sep
        yield '*' * 10 # Dividing line.

        for row in csv:
            yield row + sep
       
    # Log the query to the log file. 
    logger.info(f'Query to database: {str(fabq)}')
    
    return response(), 200, {'Content-Type':content_type}

    


