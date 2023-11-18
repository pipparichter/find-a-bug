import configparser
import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
from utils import upload_to_sql_table, pd_from_fasta, URL, load_config_paths
from typing import Dict

BACTERIA_METADATA_PATH = load_config_paths()['bacteria_metadata_path']
ARCHAEA_METADATA_PATH = load_config_paths()['archaea_metadata_path']

TABLE_NAME = 'gtdb_r207_metadata'

# Almost certainly a better way to do what this function does. 
def parse_taxonomy_col(col:pd.Series, prefix:str='') -> pd.DataFrame:
    '''Takes a DataFrame subset containing taxonomy data as input (e.g. ncbi_taxonomy) as input, and returns
    a new DataFrame with the column split into individual columns.'''
    m = {'o':f'{prefix}_order', 'd':f'{prefix}_domain', 'p':f'{prefix}_phylum', 'c':f'{prefix}_class', 'f':f'{prefix}_family', 'g':f'{prefix}_genus', 's':f'{prefix}_species'}
    rows = []
    for row in col: # Iterate over taxonomy strings in column.
        new_row = {k:'none' for k in m.keys()}
        if row == 'none': # This is an edge case. Just fill in all none values if this happens. 
            rows.append(new_row)
        else:
            for tax in row.strip().split(';'):
                flag, entry = tax[0], tax[3:] # Can't use split('__') because of edge cases where there is a __ in the species name. 
                if flag in m.keys() and len(entry) > 0: # Also handles case of empty entry. 
                    new_row[m[flag]] = entry
            rows.append(new_row)
    return pd.DataFrame.from_records(rows)


def parse_taxonomy(df:pd.DataFrame) -> pd.DataFrame:
    '''Genome taxonomy is represented as super long strings separated using semicolons. This expands
    the taxonomy string into multiple columns. '''
    dfs = []
    for col in [c for c in df.columns if 'taxonomy' in c]:
        prefix = col[:-len('_taxonomy')] # e.g. ncbi.
        dfs.append(parse_taxonomy_col(df[col].values, prefix=prefix))
        df = df.drop(columns=[col]) # Drop the original column. 
    return pd.concat(dfs + [df], axis=1)


def get_converter(col:str, dtypes:Dict[str, str]=None):
    
    dtype = dtypes[col] # Get the manually-defined datatype of the column. 
    if dtype == str:
        def converter(val):
            return str(val)

    elif dtype == int:
        def converter(val):
            if val == 'none':
                return -1
            else:
                return int(val)

    elif dtype == float:
        def converter(val):
            if val == 'none':
                return -1.0
            else:
                return float(0)
    else:
        print(col, dtype)
    return converter


def get_columns(path:str):
    '''Get the column names from the DataFrame stored at the path prior to loading in the entire thing.'''
    df = pd.read_csv(path, nrows=1)
    return df.columns

 
def setup(engine): 
    '''Load metadata files into the SQL database.'''
    table_exists = False

    for path in [ARCHAEA_METADATA_PATH, BACTERIA_METADATA_PATH]: # Should be one entry per genome_id.

        dtypes = pd.read_csv('gtdb_r207_metadata_dtypes.csv').set_index('col').to_dict(orient='dict')['dtype']
        usecols = [c for c in get_columns(path) if 'silva' not in c]
        df = pd.read_csv(path, delimiter='\t', usecols=usecols, converters={c:get_converter(c, dtypes=dtypes) for c in usecols})

        df = parse_taxonomy(df) # Split up taxonomy information into multiple columns. 
        # Drop some columns I was having issues with, sometimes due to typing. I had gotten the int converter to fix it, but decided to remove instead. 
        # df = df.drop(columns=['ncbi_submitter', 'ncbi_ncrna_count', 'ncbi_rrna_count', 'ncbi_ssu_count', 'ncbi_translation_table', 'ncbi_trna_count', 'ncbi_ungapped_length'])
        df = df.rename(columns={'accession':'genome_id'})

         # Put the table into the SQL database. Add a primary key on the first pass. 
        if not table_exists:
            upload_to_sql_table(df.set_index('genome_id'), TABLE_NAME, engine, primary_key='genome_id', if_exists='replace')
            table_exists = True
        else:
            upload_to_sql_table(df.set_index('genome_id'), TABLE_NAME, engine, primary_key=None, if_exists='append')


if __name__ == '__main__':
    
    print(f'Starting engine with URL {URL}')
    engine = sqlalchemy.create_engine(URL, echo=False)
    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()
    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')