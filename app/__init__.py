import sys
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_dir)

import logging
from flask import Flask # Response, make_response
from backend import FindABug
# from markupsafe import escape
from time import perf_counter
import pandas as pd
from sqlalchemy import create_engine
from check import check_query

# Instantiate and configure the logger. 
logging.basicConfig(filename='find-a-bug.log', filemode='a')
logger = logging.getLogger('find-a-bug')

# Tells Flask the name of the current module, for some reason. 
app = Flask(__name__)

# NOTE: Needed an absolute filepath here for some reason. 
def load_config(filepath='/home/prichter/find-a-bug/app/find-a-bug.cfg'):
    '''
    '''
    config = {}
    # Read in the config file. 
    with open(filepath, 'r') as f:
        settings = f.read().splitlines()
        for setting in settings:
            setting = setting.strip() # Make sure no more whitespace. 
            try:
                var, val = setting.split('=')
                config[var] = val
            except: # In case there are random trailing lines. 
                pass
    # Make sure everything in the configurations file is cofectly specified. 
    # check_config(config)
   
    dialect = config['DIALECT']
    driver = config['DRIVER']
    user = config['USER']
    pwd = config['PWD']
    host = config['HOST']
    dbname = config['DBNAME']
    # Construct the URL using the settings in the config file. 
    url = f'{dialect}+{driver}://{user}:{pwd}@{host}/{dbname}'
    
    return {'url':url, 'data_dir':config['DATA_DIR']}


# TODO: Might be worth using the Flask session construct to store the FindABug
# information. 

def parse_options(options_string):
    '''
    Parse the string of options. String should be of the form
    field=value;operator+field=value;operator+....

    args:
        : options_string (string)
    returns:
        : options (dict)
    '''
    # TODO: Add some kind of checking. 

    options = {}
    for option in options_string.split('+'):
        
        # Special case where the option is specifying output format.
        if option[:3] == 'out':
            options['out'] = option.split('=')[1]
        
        else:
            field = option.split('=')[0]
            filter_ = option.split('=')[1]
            operator, value = filter_.split(';')
            
            # The only types in the database (and I think all that there
            # will be) are floats, ints, and strings. 
            # TODO: There is probably a better way to handle this... 
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    # If neither of these two conversions work, just
                    # treat the value like a string. 
                    pass

            if field not in options:
                options[field] = [(operator, value)]
            else:
                options[field].append((operator, value))

    return options


@app.route('/')
def welcome():
    return 'SUCCESS'


@app.route('/info')
def database_info():
    # Start a connection to the FindABug database. 
    engine = create_engine(load_config()['url'])
    
    return load_config()['url']
    fab = FindABug(engine)
    
    # options = parse_options(options)
    # JSON is the default return format, but CSV can be specified. 
    # out = options.pop('out', 'json')
    # Only one option (so far) should be sepcified when using info. 
    
    info_df = fab.info()
    
    return info_df.to_json(orient='records')
    # if out == 'csv':
    #     return info_df.to_csv()
    # elif out == 'json':
    #     return info_df.to_json(orient='records')
    # else:
    #     msg = f'Output type {out} is not supported.'
    #     raise ValueError(msg)
 

@app.route('/<string:query>/<options>')
def query_database(query=None, options=None):

    t_init = perf_counter()

    # Start a connection to the FindABug database. 
    engine = create_engine(load_config()['url'], echo=True)
    fab = FindABug(engine)
    # Make sure the query is valid.
    # check_query(query, fab)

    options = parse_options(options)
    
    # In case multiple query fields are specified.
    query = query.split('+')
    result = fab.query_database(query, options=options) 
    
    # TODO: Add FASTA output option. 
    
    t_final = perf_counter()

    def response():
        yield f'{len(result)} results in {t_final - t_init} seconds <br>'
        yield '<br>'

        for row in result:
            yield row + '<br>'
    
    return response(), 200, {'Content-Type':'text/html'}

    


