import sqlalchemy
import pandas as pd
import numpy as np
from time import perf_counter

import setup

def split_taxonomies(df):
    '''
    Genome taxonomy is represented as super long strings separated using
    semicolons. This expands the taxonomy string into multiple columns. 
    '''
    tax_cats = ['domain', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    cols = df.columns
    
    for col in cols:
        if 'taxonomy' in col:
            try:
                col_prefix = col[:-len('taxonomy')]
                new_cols = [col_prefix + cat for cat in tax_cats]
                df[new_cols] = getattr(df, col).str.split(';', expand=True)
            except:
                print(f'WARNING: Could not load {col_prefix[:-1]} taxonomy.')
                pass
    
    # Drop the taxonomy columns which have been expanded. 
    return df.drop(columns=[c for c in cols if 'taxonomy' in c])


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
    
    print(f'STARTING ENGINE WITH URL {setup.url}')
    engine = sqlalchemy.create_engine(setup.url, echo=False)
    
    t_init = perf_counter()
    
    load(engine)

    t_final = perf_counter()

    print(f'\nTable uploaded in {t_final - t_init} seconds.')
 
