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


def get_format(url:str) -> Tuple[str, str]:
    '''Retrieve the format from the URL, if one is specified, as well as the 
    URL without the format specification.
    
    :param url: The URL sent by the client to the web application.
    :return: A tuple containing the format and the URL without a fmt={fmt} specification. 
    '''
    if 'fmt' in url:
        page = int(re.search('fmt=([a-z]+)', url).group(1))
        url = re.sub('[?]*fmt=([a-z]+)[&]*', '', url)
        return fmt, url
    else:
        return None, url


def format_data(data:pd.DataFrame, fmt:str=None) -> str:
    '''Format a pandas DataFrame according to the desired format.
    
    :param data: A pandas DataFrame to format.
    :param fmt: The format to put the DataFrame into. Right now, supports CSV. If None,
        the DataFrame is formatted prettily using tabulate for browser output. 
    :return: A string containing the formatted data. 
    '''
    if fmt is None: # Format the data prettily for browser output. 
        raise Exception('TODO')
        # data = tabulate(data)
    elif fmt == 'csv': # Format the DataFrame as a CSV.
        data = data.to_csv() 
    return data


@app.errorhandler(Exception)
def handle_unknown_error(err):
    '''Error handling when unanticipated exceptions are raised. '''
    # Log the error. 
    # logger.error(str(err))
    # exception = sys.exception() # Access the exception. 
    err = traceback.format_exception_only(type(err), err) # Returns a list of strings, usually just one string. 
    logger.error(err[0].split('\n')[0])

    # report = traceback.format_exc() # Return the full traceback. 
    return 'Exception raised, see log for details.', 500, {'Content-Type':'text/plain'}


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
    fmt, url = get_format(request.url) # Get the format of the output from the URL, if specified. 

    if resource == 'annotations':
        return format_data(data, fmt=fmt), 200, {'Content-Type':'text/plain'}
    elif resource == 'sequences':
        return format_data(data, fmt=fmt), 200, {'Content-Type':'text/plain'}
    elif resource == 'metadata': # Not yet implemented because I am lazy.
        return 'TODO', 200, {'Content-Type':'text/plain'}


@app.route('/sql/<resource>')
def sql(resource:str=None) -> Tuple[str, int]:
    '''Returns the SQL query submitted to the Find-A-Bug database to the client. 
      
    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: The SQL query and the status code. 
    '''
    assert resource in ['annotations', 'metadata', 'sequences'], 'app.__init__.sql: Invalid resource name. Must be one of: annotations, metadata, sequences.'
    logger.info(str(request.url))

    url = request.url.replace('sql', 'api', 1) # Convert the URL to one which mirrors a resource request. 
    page, url = get_page(url)

    fabq = FindABugQuery(url, ENGINE, page=page)

    return str(fabq), 200, {'Content-Type':'text/plain'}
    

@app.route('/api/<resource>')
def handle(resource:str=None) -> Tuple[requests.Response, int, Dict[str, str]]:
    '''Handles a resource request to the server. 

    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: Calls the respond function (defined below) using the data obtained for the query.
    '''
    assert resource in ['annotations', 'metadata', 'sequences'], 'app.__init__.handle: Invalid resource name. Must be one of: annotations, metadata, sequences.'
    logger.info(str(request.url))

    t_init = perf_counter()

    # Make sure to remove both the page and format specifications from the URL. 
    page, url = get_page(request.url)
    fmt, url = get_format(url)

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
        # response = '' if fmt is None else f'{len(data)} results in {np.round(t_final - t_init, 3)} seconds\n\n\n\n'
        response = ''
        response += format_data(data, fmt='csv')
        return response, 200, {'Content-Type':'text/plain'}


    # I don't think I need the generator anymore if I limit the result to 500. 
    # def response() -> Generator[str, None, None]:
    #     yield f'{len(df)} results in {np.round(t, 3)} seconds\n'
    #     yield '-' * 50 # Dividing line.
    #     yield '\n\n'
    #     yield ','.join(df.columns) + '\n'
    #     for row in df.itertuples():
    #         # First row element is the index, which we do not want to include. 
    #         yield ','.join([str(elem) for elem in row[1:]]) + '\n'
    
    # return response(), 200, {'Content-Type':'text/plain'}



