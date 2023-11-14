import configparser
import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
from utils import upload_to_sql_table, pd_from_fasta, URL


# Read in the config file, which is in the project root directory. 
config = configparser.ConfigParser()
# with open('/home/prichter/Documents/find-a-bug/find-a-bug.cfg', 'r', encoding='UTF-8') as f:
with open(os.path.join(os.path.dirname(__file__), '../', '../', 'find-a-bug.cfg'), 'r', encoding='UTF-8') as f:
    config.read_file(f)


def parse_taxonomy_col(col:pd.DataFrame, prefix:str='') -> pd.DataFrame:
    '''Takes a DataFrame subset containing taxonomy data as input (e.g. ncbi_taxonomy) as input, and returns
    a new DataFrame with the column split into individual columns.'''
    m = {'o':'order', 'd':'domain', 'p':'phylum', 'c':'class', 'f':'family', 'g':'genus', 's':'species'}
    rows = []
    for row in col: # Iterate over taxonomy strings in column. .
        new_row = [s.split('__') for s in row.strip().split(';')]
        new_row = list(map(new_row, lambda x : [f'{prefix}_' + m[x[0]], x[1]]))
        rows.append(new_row) 
    return pd.DataFrame(rows)


def parse_taxonomy(df:pd.DataFrame) -> pd.DataFrame:
    '''
    Genome taxonomy is represented as super long strings separated using semicolons. 
    This expands the taxonomy string into multiple columns. 
    '''
    dfs = []
    for col in [c for c in df.columns if 'taxonomy' in c]:
        prefix = col[:-len('_taxonomy')] # e.g. ncbi.
        dfs.append(parse_taxonomy_col(df[[col]], prefix=prefix))
        df = df.drop(columns=[col]) # Drop the original column. 
    return pd.concat(dfs + [df], axis=1)


def int_converter(val):
    '''
    Convert missing int values in the Metadata when reading the CSV file. 
    '''
    # If the encountered value is NaN...
    if val == 'none':
        # The problem columns use integers which should be positive (e.g. RNA
        # counts). 
        return -1
    else:
        return int(val)

 
def load(engine, **kwargs): 
    '''
    Load metadata files into the SQL database. 
    '''
    paths = [setup.data_dir + '/gtdb/r207/metadata/bac120_metadata_r207.tsv',
            setup.data_dir + '/gtdb/r207/metadata/ar53_metadata_r207.tsv']
    dfs = []
    
    for path in paths:
        with open(path, 'r') as f:
            # The NCBI columns have datatype issues. Just read in as strings
            # using converters. 
            cols_to_int = ['ncbi_ncrna_count', 'ncbi_rrna_count', 'ncbi_ssu_count',
                'ncbi_translation_table', 'ncbi_trna_count',
                'ncbi_ungapped_length']
            df = pd.read_csv(f, delimiter='\t', converters={c:int_converter for c in cols_to_int})
            # Split up taxonomy information into multiple columns. 
            df = split_taxonomies(df)
            
            # TODO: This is being really weird, "invalid string value"?
            df = df.drop(columns=['ncbi_submitter'])

            # Rename the genome ID column for consistency with the sequence data. 
            df = df.rename(columns={'accession':'genome_id'})
            # Add the domain information to the DataFrame. 
            dfs.append(df)
   
    df = pd.concat(dfs).reset_index(drop=True)
             
    # Put the table into the SQL database. Add a primary key on the first pass. 
    setup.create_table(df.fillna('None'), 'gtdb_r207_metadata', engine, primary_key='genome_id')
    


if __name__ == '__main__':
    
    print(f'Starting engine with URL {URL}')
    engine = sqlalchemy.setup_engine(URL, echo=False)
    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()
    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')