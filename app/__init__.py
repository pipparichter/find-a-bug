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
import traceback

from typing import List, Generator, Dict, Tuple



# Tells Flask the name of the current module. 
app = Flask(__name__)



@app.route('/')
def welcome():
    return 'Welcome to Find-A-Bug!', 200, {'Content-Type':'text/plain'}



@app.route('/get/<table_name>')
def get(table_name:str=None) -> Tuple[requests.Response, int, Dict[str, str]]:
    '''Handles a data retrieval request to the server.'''
    url = request.url # Get the URL that was sent to the app. How does this work, I wonder?
    page = 0
    
    if '[page]' in url: # Removes the page from the URL string. 
        page = re.search('\[page\](\d+)', url).group(1)
        url = url.replace(f'[page]{page}', '')

    url = url.replace('https://microbes.gps.caltech.edu/get/', '') # Remove the front part from the URL. 

    database = Database(reflect=True)

    # from sqlalchemy import inspect 
    # return f"{str(list(inspect(database.engine).get_table_names()))} {str(list(inspect(database.engine).get_columns('metadata')))}"

    try:
        query = Query(database, table_name, page=int(page))
        filter_string = None if '?' not in url else url.split('?')[-1] # Extract the filter information, if present.
        if filter_string is not None:
            filter_ = Filter(database, table_name, filter_string)
            filter_(query)
            
        result = query.submit(database)
        database.close()

        data = pd.DataFrame.from_records([row._asdict() for row in result]) #, columns=result._fields)
        data = '' if len(data) == 0 else data.to_csv() # Just return an empty string if there are no results. 
        return data, 200, {'Content-Type':'text/plain'}

    except Exception as err:

        database.close()
        return traceback.format_exc(), 500, {'Content-Type':'text/plain'}





