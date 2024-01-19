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

from typing import List, Generator, Dict, Tuple

# Instantiate and configure the logger. 
logging.basicConfig(filename='find-a-bug.log', filemode='a')
logger = logging.getLogger('find-a-bug')

# Tells Flask the name of the current module. 
app = Flask(__name__)

ENGINE = create_engine(f'mariadb+pymysql://root:Doledi7-Bebyno2@localhost/findabug')


@app.errorhandler(Exception)
def handle_unknown_error(err):
    '''Error handling when unanticipated exceptions are raised. '''
    # Log the error. 
    # logger.error(str(err))
    report = traceback.format_exc()
    
    return report, 500, {'Content-Type':'text/plain'}


@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200


@app.route('/info')
def info():
    '''Return information about the database.'''
    return 'TODO', 200


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


@app.route('/<resource>')
def handle(resource:str=None) -> Tuple[requests.Response, int, Dict[str, str]]:
    '''Handles a resource request to the server. 

    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: Calls the respond function (defined below) using the data obtained for the query.
    '''

    assert resource in ['annotations', 'metadata', 'sequences'], 'app.__init__.handle: Invalid resource name. Must be one of: annotations, metadata, sequences.'

    t_init = perf_counter()

    page, url = get_page(request.url)

    fabq = FindABugQuery(url, ENGINE, page=page)
    result = fabq.execute()

    if len(result) == 0:
        df = pd.DataFrame.from_records(result, columns=result._fields)
    else:
        df = pd.DataFrame.from_records(result, columns=result[0]._fields)
    t_final = perf_counter()
    
    return respond(df, t_final - t_init)


def respond(df:pd.DataFrame, t:float) -> Tuple[Generator[str, None, None], int, Dict[str, str]]:   
    '''Generates a response to send out to the requesting client.
    
    :param df: A DataFrame containing the information from the query. 
    :param t: The time it took to retrieve the query information. 
    :return: A string generator to produce the response, as well as the response status code and a
        dictionary specifying the content type.    
    '''

    def response() -> Generator[str, None, None]:
        yield f'{len(df)} results in {np.round(t, 3)} seconds\n'
        yield '-' * 50 # Dividing line.
        yield '\n\n'
        yield ','.join(df.columns) + '\n'
        for row in df.itertuples():
            # First row element is the index, which we do not want to include. 
            yield ','.join([str(elem) for elem in row[1:]]) + '\n'
    
    return response(), 200, {'Content-Type':'text/plain'}



