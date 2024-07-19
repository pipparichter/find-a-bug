import sys
import os
from datetime import datetime
import pandas as pd
import logging
from flask import Flask, request # Response, make_response
from time import perf_counter
import requests
import traceback
import numpy as np
import re
from utils.query import Query, Filter 
from utils.database import Database

from typing import List, Generator, Dict, Tuple



# Tells Flask the name of the current module. 
app = Flask(__name__)



@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200, {'Content-Type':'text/plain'}



@app.route('/get/<resource>')
def get(resource:str=None) -> Tuple[requests.Response, int, Dict[str, str]]:
    '''Handles a data retrieval request to the server. 

    :param resource: One of 'annotations', 'metadata', or 'sequences'. Indicates the table to access. 
    :return: CSV data corresponding to the request parameters.
    '''
    page = 0
    if '#' in request.url:
        url, page = url.split('#')

    url = url.replace('https://microbes.gps.caltech.edu/get/', '') # Parse the URL string.
    
    table_name, filter_string = url.split('?')
    database = Database()

    query = Query(database, table_name, page=page)
    filter_ = Filter(database, filter_string)

    result = filter_(query).execute(database)

    if len(result) == 0: # In case of no results.
        return 'No results', 200
    else: 
        data = pd.DataFrame.from_records(result, columns=result[0]._fields)
        return data.to_csv(), 200, {'Content-Type':'text/plain'}





