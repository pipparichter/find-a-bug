'''
Use this file to load data from FASTA and CSV files into the MySQL database. 
'''
import sqlalchemy
from sqlalchemy import Integer, Float, text, Boolean
from sqlalchemy.dialects.mysql import VARCHAR, TEXT, LONGTEXT, BLOB
import pandas as pd
import numpy as np
import os

host = 'localhost'
dialect = 'mariadb'
driver = 'pymysql'
user = 'root'
pwd = 'Doledi7-Bebyno2'
dbname = 'findabug'

url = f'{dialect}+{driver}://{user}:{pwd}@{host}/{dbname}'
data_dir='/var/lib/pgsql/data'


def get_sql_dtypes(df):
    '''
    '''
    dtypes = {}
    for col_name in list(df.columns):
        
        t = type(df[col_name].iloc[0]) # Get the column datatype.  
        # Convert the datatype for SQLAlchemy usage. 
        if t == str:
            # Special cases for amino acid sequences because they are so long. 
            if col_name == 'sequence':
                dtypes[col_name] = LONGTEXT
            elif col_name in ['gene_name', 'genome_id']:
                dtypes[col_name] = VARCHAR(150)
            else:
                # Get the maximum length to be accomodated. 
                max_length = max(df[col_name].apply(len))
                dtypes[col_name] = VARCHAR(max_length)
        
        elif t in [int, np.int64]:
            dtypes[col_name] = Integer
        elif t in [np.float64, float]:
            dtypes[col_name] = Float
        elif t in [bool, np.bool_]:
            dtypes[col_name] = Boolean
        else:
            msg = 'Type ' + str(t) + ' is not handled by create_table.'
            raise Exception(msg)
    
    return dtypes


def create_table(df, tablename, engine, index=None, primary_key=None, **kwargs):
    '''
    Uploads a pandas DataFrame to the SQL database. If specified, appends the
    DataFrame information to an existing table. 
    '''
    # Specifications for DataFrame upload.  
    kwargs = {'chunksize':1000, 'method':'multi', 'index':False, 'if_exists':'append'}
 
    df.to_sql(tablename, engine, dtype=get_sql_dtypes(df), **kwargs)
    
    if primary_key is not None:
        with engine.connect() as conn:
            conn.execute(text(f'ALTER TABLE {tablename} ADD PRIMARY KEY ({primary_key})'))
    if index is not None:
        with engine.connect() as conn:
            index_name = index + '_idx'
            conn.execute(text(f'CREATE INDEX {index_name} ON {tablename} ({index})'))
    
    print(f'Upload to table {tablename} successful.')
    

