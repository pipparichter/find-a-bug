import configparser
import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
from utils import upload_to_sql_table, pd_from_fasta, URL, load_config_paths

BACTERIA_METADATA_PATH = load_config_paths()['bacteria_metadata_path']
ARCHAEA_METADATA_PATH = load_config_paths()['archaea_metadata_path']

TABLE_NAME = 'gtdb_r207_metadata'

def parse_taxonomy_col(col:pd.DataFrame, prefix:str='') -> pd.DataFrame:
    '''Takes a DataFrame subset containing taxonomy data as input (e.g. ncbi_taxonomy) as input, and returns
    a new DataFrame with the column split into individual columns.'''
    m = {'o':'order', 'd':'domain', 'p':'phylum', 'c':'class', 'f':'family', 'g':'genus', 's':'species'}
    rows = []
    for row in col: # Iterate over taxonomy strings in column.
        # This is a list of lists with [[flag, taxonomy], [flag, taxonomy], ...]
        new_row = [s.split('__') for s in row.strip().split(';')]
        new_row = list(map(new_row, lambda x : {f'{prefix}_' + m[x[0]] : x[1]}))
        rows.append(new_row) 
    return pd.DataFrame.from_records(rows)


def parse_taxonomy(df:pd.DataFrame) -> pd.DataFrame:
    '''Genome taxonomy is represented as super long strings separated using semicolons. This expands
    the taxonomy string into multiple columns. '''
    dfs = []
    for col in [c for c in df.columns if 'taxonomy' in c]:
        prefix = col[:-len('_taxonomy')] # e.g. ncbi.
        dfs.append(parse_taxonomy_col(df[[col]], prefix=prefix))
        df = df.drop(columns=[col]) # Drop the original column. 
    return pd.concat(dfs + [df], axis=1)


# def int_converter(val):
#     '''
#     Convert missing int values in the Metadata when reading the CSV file. 
#     '''
#     # If the encountered value is NaN...
#     if val == 'none':
#         # The problem columns use integers which should be positive (e.g. RNA counts). 
#         return -1
#     else:
#         return int(val)

 
def setup(engine, **kwargs): 
    '''Load metadata files into the SQL database.'''
    gene_to_genome_map = h5py.File('gene_to_genome_map.h5', 'r') # Read in the HDF file. 
    table_exists = False

    for path in [ARCHAEA_METADATA_PATH, BACTERIA_METADATA_PATH]: # Should be one entry per genome_id.
        with open(path, 'r') as f:
            df = pd.read_csv(f, delimiter='\t') # converters={c:int_converter for c in cols_to_int})
            df = parse_taxonomy(df) # Split up taxonomy information into multiple columns. 
            # Drop some columns I was having issues with, sometimes due to typing. I had gotten the int converter to fix it, but decided to remove instead. 
            df = df.drop(columns=['ncbi_submitter', 'ncbi_ncrna_count', 'ncbi_rrna_count', 'ncbi_ssu_count', 'ncbi_translation_table', 'ncbi_trna_count', 'ncbi_ungapped_length'])
            # Rename the genome ID column for consistency with the sequence data. 
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