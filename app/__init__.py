'''
'''
import sys
import os
from datetime import datetime
import io
import pandas as pd
import logging
from flask import Flask, request # Response, make_response
from time import perf_counter
from sqlalchemy import create_engine
from app.query import FindABugQuery
import traceback
import configparser

# Instantiate and configure the logger. 
logging.basicConfig(filename='find-a-bug.log', filemode='a')
logger = logging.getLogger('find-a-bug')

# Tells Flask the name of the current module. 
app = Flask(__name__)


# Read in the config file, which is in the project root directory. 
config = configparser.ConfigParser()
# with open('/home/prichter/Documents/find-a-bug/find-a-bug.cfg', 'r', encoding='UTF-8') as f:
with open(os.path.join(os.path.dirname(__file__), '../',  'find-a-bug.cfg'), 'r', encoding='UTF-8') as f:
    config.read_file(f)

URL = '{dialect}+{driver}://{user}:{password}@{host}/{name}'.format(**dict(config.items('db')))
ENGINE = create_engine(URL)


@app.errorhandler(Exception)
def handle_unknown_error(err):
    '''Error handling when unanticipated exceptions are raised. '''
    # Log the error. 
    logger.error(str(err))
    report = traceback.format_exc().split('\n')
    
    return '\n'.join(report), 500, {'Content-Type':'text/plain'}


@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200


@app.route('/info')
def info():
    '''Return information about the database.'''
    return 'TODO', 200


@app.route('/<resource>')
def handle(resource=None):
    '''
    '''

    t_init = perf_counter()
    fabq = FindABugQuery(request.url, ENGINE)
    result = fabq.execute()
    df = pd.DataFrame.from_records(result, columns=result[0]._fields)

    t_final = perf_counter()
        
    return respond(df, t_final - t_init)


def respond(df, t):   

    def response():
        yield f'{len(df)} results in {t} seconds\n'
        yield '\n'
        yield '-' * 30 # Dividing line.
        yield '\n'
        yield '\t'.join(df.columns)
        for row in df.itertuples():
            '\t'.join(row)
       
    # Log the query to the log file. 
    # now = datetime.now()
    # timestamp = now.strftime('%d/%m/%Y %H:%M')
    # logger.info(f'{timestamp} Query to database: {str(fabq)}')
    
    return response(), 200, {'Content-Type':'text/plain'}


