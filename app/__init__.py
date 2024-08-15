import sys
import os
from datetime import datetime
import pandas as pd
import logging
from flask import Flask, request, Response # Response, make_response
from time import perf_counter
import requests
import traceback
import numpy as np
import re
from utils.query import Query, Filter 
from utils.database import Database
import traceback

from typing import List, Generator, Dict, Tuple



# Tells Flask the name of the current module. 
app = Flask(__name__)

def make_query(database, table_name:str, filter_string:str=None, page:int=None):
    query = Query(database, table_name, page=page, page_size=1000)
    if filter_string is not None:
        filter_ = Filter(database, table_name, filter_string)
        filter_(query)
    return query


@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200, {'Content-Type':'text/plain'}


@app.route('/count/<table_name>')
def count(table_name:str=None, debug:bool=False) -> Tuple[requests.Response, int, Dict[str, str]]:
    url = request.url # Get the URL that was sent to the app. How does this work, I wonder?

    url = url.replace('https://microbes.gps.caltech.edu/count/', '') # Remove the front part from the URL. 
    filter_string = None if '?' not in url else url.split('?')[-1] # Extract the filter information, if present.

    database = Database(reflect=True)

    try:
        query = make_query(database, table_name, filter_string=filter_string)
        result = query.count(database)
        database.close()
        return str(result), 200, {'Content-Type':'text/plain'}

    except Exception as err:

        database.close()
        return traceback.format_exc(), 500, {'Content-Type':'text/plain'} 


@app.route('/get/<table_name>')
def get(table_name:str=None, debug:bool=False) -> Tuple[requests.Response, int, Dict[str, str]]:
    '''Handles a data retrieval request to the server.'''
    url = request.url # Get the URL that was sent to the app. How does this work, I wonder?
    page = 0
    if '[page]' in url: # Removes the page from the URL string. 
        page = re.search('\[page\](\d+)', url).group(1)
        url = url.replace(f'[page]{page}', '')

    url = url.replace('https://microbes.gps.caltech.edu/get/', '') # Remove the front part from the URL. 
    filter_string = None if '?' not in url else url.split('?')[-1] # Extract the filter information, if present.
    database = Database(reflect=True)

    try:
        query = make_query(database, table_name, filter_string=filter_string, page=page)            
        result = query.get(database, debug=debug)
        database.close()

        if debug: # If in debug mode, don't try to convert the output to a CSV.
            return result, 200, {'Content-Type':'text/plain'}

        data = pd.DataFrame.from_records([row._asdict() for row in result]) #, columns=result._fields)
        data = '' if len(data) == 0 else data.to_csv() # Just return an empty string if there are no results. 
        return data, 200, {'Content-Type':'text/plain'} 

    except Exception as err:

        database.close()
        return traceback.format_exc(), 500, {'Content-Type':'text/plain'}


@app.route('/debug/<cmd>/<table_name>')
def debug(cmd:str=None, table_name:str=None):

    if cmd == 'count':
        return count(table_name=table_name, debug=True)
    elif cmd == 'get':
        return get(table_name=table_name, debug=True)



