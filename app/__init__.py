import sys
import os
from datetime import datetime
import pandas as pd
import logging
from flask import Flask, request # Response, make_response
from time import perf_counter
from sqlalchemy import create_engine
from app.query import FindABugQuery
import requests
import traceback
import numpy as np
import re
from tabulate import tabulate

from typing import List, Generator, Dict, Tuple

# Instantiate and configure the logger. 
logging_format = '%(levelname)s : %(asctime)s : %(message)s'
# Set logging level to info so everything gets added to the log file. 
logging.basicConfig(filename='find-a-bug.log', filemode='a', format=logging_format, level=logging.INFO)
logger = logging.getLogger('find-a-bug')

# Tells Flask the name of the current module. 
app = Flask(__name__)

ENGINE = create_engine(f'mariadb+pymysql://root:Doledi7-Bebyno2@localhost/findabug')

def get_page(url:str) -> Tuple[int, str]:
    '''Retrieve the page from the URL, if one is specified, as well as the 
    URL without the page in the query list.append
    
    :param url: The URL sent by the client to the web application.
    :return: A tuple containing the extracted page number and the URL without a page={page} query. 
    '''
    if 'page' in url:
        page = int(re.search('page=([0-9]+)', url).group(1))
        url = re.sub('[?]*page=([0-9]+)[&]*', '', url)
        return page, url
    else:
        return 0, url

        
@app.errorhandler(Exception)
def handle_unknown_error(err):
    '''Error handling when unanticipated exceptions are raised. '''

    err_type, err_value, err_traceback = sys.exc_info()
    info = traceback.format_exception(err_type, err_value, err_traceback)[-2]
    info = info.lower().strip()
    msg = f'{type(err)} in {info}: {str(err).strip()}'
    msg = msg.replace('\n', '') # Remove any remaining newlines.

    logger.error(msg) # Log the error
    # report = traceback.format_exc() # Return the full traceback. 
    return msg, 500, {'Content-Type':'text/plain'}


@app.route('/')
def welcome():
    logger.info(str(request.url))
    return 'Welcome to Find-A-Bug!', 200, {'Content-Type':'text/plain'}


@app.route('/info/<resource>')
def info(resource:str):
    '''Return information about the database.

    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: Information about the resource table in the format requested in the fmt query parameter.
    '''

    assert resource in ['annotations', 'metadata', 'sequences'], 'app.__init__.sql: Invalid resource name. Must be one of: annotations, metadata, sequences.'
    data = pd.read_csv(f'./text/{resource}_column_descriptions.csv')
    # fmt, url = get_format(request.url) # Get the format of the output from the URL, if specified. 

    if resource == 'annotations':
        return data.to_csv(), 200, {'Content-Type':'text/plain'}
    elif resource == 'sequences':
        return data.to_csv(), 200, {'Content-Type':'text/plain'}
    elif resource == 'metadata': # Not yet implemented because I am lazy.
        return data.to_csv(), 200, {'Content-Type':'text/plain'}


@app.route('/sql/<resource>')
def sql(resource:str=None) -> Tuple[str, int]:
    '''Returns the SQL query submitted to the Find-A-Bug database to the client. 
      
    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: The SQL query and the status code. 
    '''
    assert resource in ['annotations', 'metadata', 'sequences'], 'app.__init__.sql: Invalid resource name. Must be one of: annotations, metadata, sequences.'
    logger.info(str(request.url))

    url = request.url.replace('sql', 'get', 1) # Convert the URL to one which mirrors a resource request. 
    page, url = get_page(url)

    fabq = FindABugQuery(url, ENGINE, page=page)

    return str(fabq), 200, {'Content-Type':'text/plain'}
    

@app.route('/count/<resource>')
def count(resource:str=None):
    '''Handles a request for a count of the results which match the specified criteria.

    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: The number of entries in the database matching the specidied criteria. 
    '''

    assert resource in ['annotations', 'metadata', 'sequences'], 'app.__init__.handle: Invalid resource name. Must be one of: annotations, metadata, sequences.'
    logger.info(str(request.url))

    url = request.url.replace('count', 'get', 1) # Convert the URL to one which mirrors a resource request. 

    fabq = FindABugQuery(url, ENGINE, page=None)
    result = fabq.execute()

    return str(len(result)), 200, {'Content-Type':'text/plain'}


@app.route('/get/<resource>')
def get(resource:str=None) -> Tuple[requests.Response, int, Dict[str, str]]:
    '''Handles a data retrieval request to the server. 

    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: CSV data corresponding to the request parameters.
    '''
    assert resource in ['annotations', 'metadata', 'sequences'], 'app.__init__.handle: Invalid resource name. Must be one of: annotations, metadata, sequences.'
    logger.info(str(request.url))

    # Make sure to remove page specifications from the URL. 
    page, url = get_page(request.url)

    fabq = FindABugQuery(url, ENGINE, page=page)
    result = fabq.execute()

    if len(result) == 0:
        # In case of no results.
        return 'No results', 200
    else:
        # Send a response with in the specified format. 
        data = pd.DataFrame.from_records(result, columns=result[0]._fields)
        t_final = perf_counter()
        # Only include the timing if a pretty-print output format is requested. 
        return data.to_csv(), 200, {'Content-Type':'text/plain'}





