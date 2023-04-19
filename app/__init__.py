'''
'''
import sys
import os
from datetime import datetime

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
    
    return '\n'.join(report), 500, {'Content-Type':'text/plain'}


@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200


@app.route('/<type_>')
def handle(type_):
    '''
    '''
    # Start a connection to the FindABug database. 
    fab = FindABug(engine)
    
    if type_ == 'info':
        # Log the query to the log file. 
        logger.info('Query to database: DATABASE INFO')
        return '\n'.join(fab.info()), 200, {'Content-Type':'text/plain'}

    else:
        # Decided to store information in request headers as opposed to a URL. 
        cols_string = request.headers.get('cols', '')
        filters_string = request.headers.get('filters', '')

        # If no page is specified, grab everything. If not, only grab 100 things from a particular page. 
        page = request.headers.get('page', None) # Not sure if this is received as an int. 
        
        t_init = perf_counter()

        fab = FindABug(engine)

        if page is not None:
            fabq = FindABugQuery(cols_string, filters_string, type_, page_size=100, page=int(page))
        else:
            fabq = FindABugQuery(cols_string, filters_string, type_)

        csv = fab.query(fabq) 
        
        t_final = perf_counter()
        
        return respond(fabq, csv, t_final - t_init)


def respond(fabq, csv, t):   
    '''
    '''
    def response():
        yield f'{len(csv)} results in {t} seconds\n'
        
        # Also want to print the raw SQL query. 
        yield '\n'
        yield str(fabq.stmt)
        yield '\n'
        yield '\n'
        yield '-' * 20 # Dividing line.
        yield '\n'
        for row in csv:
            yield row + '\n'
       
    # Log the query to the log file. 
    now = datetime.now()
    timestamp = now.strftime('%d/%m/%Y %H:%M')
    logger.info(f'{timestamp} Query to database: {str(fabq)}')
    
    return response(), 200, {'Content-Type':'text/plain'}


