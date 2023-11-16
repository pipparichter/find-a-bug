'''Utilities for reading and writing from FASTA files (and probably some other generic use stuff later on).'''

import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import sqlalchemy
from sqlalchemy import Integer, Float, Boolean
from sqlalchemy.dialects.mysql import VARCHAR, LONGTEXT
import os   
import configparser
from typing import Dict, TypeVar, NoReturn
import pickle


def load_config() -> Dict[str, Dict[str, str]]:
    '''Loads information from the configuration file as a dictionary. Assumes the 
    configuration file is in the project root directory.'''

    # Read in the config file, which is in the project root directory. 
    config = configparser.ConfigParser()
    # with open('/home/prichter/Documents/find-a-bug/find-a-bug.cfg', 'r', encoding='UTF-8') as f:
    with open(os.path.join(os.path.dirname(__file__), '../', 'find-a-bug.cfg'), 'r', encoding='UTF-8') as f:
        config.read_file(f)
    return config._sections # This gets everything as a dictionary. 


def load_config_paths() -> Dict[str, str]:
    '''Loads everything under the [paths] heading in the config file as a dictionary.'''
    return load_config()['paths']


# Load the URL to use for accessing the SQL Database. Not sure if this is great practice. I might want to wrap it in a function. 
URL = '{dialect}+{driver}://{user}:{password}@{host}/{name}'.format(**load_config()['db'])


def write(text:str, path:str) -> NoReturn:
    '''Writes a string of text to a file.'''
    if path is not None:
        with open(path, 'w', encoding='UTF-8') as f:
            f.write(text)


def read(path:str) -> str:
    '''Reads the information contained in a text file into a string.'''
    with open(path, 'r', encoding='UTF-8') as f:
        text = f.read()
    return text


# NOTE: Seems as though these FASTA files have slightly different header conventions. 
def get_gene_id(head:str) -> str:
    '''Extract the unique identifier from a FASTA metadata string (the 
    information on the line preceding the actual sequence). '''
    gene_id = head.split(' # ')[0]
    gene_id = gene_id[1:] # Remove the leading carrot. 
    return gene_id

def fasta_gene_ids(path:str) -> np.array:
    '''Extract all gene gene_ids stored in a FASTA file.'''
    fasta = read(path)
    # Extract all the gene_ids from the headers, and return the result.
    gene_ids = [get_gene_id(head) for head in re.findall(r'^>.*', fasta, re.MULTILINE)]
    return np.array(gene_ids)


def fasta_seqs(path:str) -> np.array:
    '''Extract all amino acid sequences stored in a FASTA file.'''
    fasta = read(path)
    seqs = re.split(r'^>.*', fasta, flags=re.MULTILINE)[1:]
    # Strip all of the newline characters from the amino acid sequences. 
    seqs = [s.replace('\n', '') for s in seqs]
    return seqs


def fasta_size(path:str) -> int:
    '''Get the number of entries in a FASTA file.'''
    return len(fasta_gene_ids(path))


def pd_from_fasta(path:str, is_genome_file=False) -> pd.DataFrame:
    '''Load a FASTA file in as a pandas DataFrame. If the FASTA file is for a particular genome, then 
    add the genome ID as an additional column.'''
    gene_ids = fasta_gene_ids(path)
    seqs = fasta_seqs(path)
    df = pd.DataFrame({'seq':seqs, 'gene_id':gene_ids})

    if is_genome_file:
        filename = os.path.basename(path) # Extract the filename.
        df['genome_id'] = filename[:-len('_protein.faa')]# File names are of the form {genome_id}_protein.faa

    return df


def pd_to_fasta(df, path=None, textwidth=80):
    '''Convert a pandas DataFrame containing FASTA data to a FASTA file format.'''

    assert df.index.name == 'id', 'utils.pd_to_fasta: Gene ID must be set as the DataFrame index before writing.'

    fasta = ''
    for row in tqdm(df.itertuples(), desc='utils.pd_to_fasta', total=len(df)):
        fasta += '>|' + str(row.Index) + '|\n'

        # Split the sequence up into shorter, sixty-character strings.
        n = len(row.seq)
        seq = [row.seq[i:min(n, i + textwidth)] for i in range(0, n, textwidth)]

        seq = '\n'.join(seq) + '\n'
        fasta += seq
    
    # Write the FASTA string to the path-specified file. 
    write(fasta, path=path)


def get_sql_dtypes(df:pd.DataFrame):
    '''Explicitly converts the datatypes in a pandas DataFrame to their SQL equivalents. Returns
    a dictionary mapping column names to SQL types.'''
    dtypes = {'seq':LONGTEXT, 'gene_id':VARCHAR(150), 'genome_id':VARCHAR(150)}

    for col in [c for c in df.columns if c not in dtypes]:
        t =  type(df[col].iloc[0]) # Get the type of the first element in the column (assumed to be the same for the whole column.)
        if t == str:
            max_length = max(df[col].apply(len)) # Get the max length of any string in the column.
            dtypes[col] = VARCHAR(max_length)
        elif t in [int, np.int64]:
            dtypes[col] = Integer
        elif t in [np.float64, float]:
            dtypes[col] = Float
        elif t in [bool, np.bool_]:
            dtypes[col] = Boolean
        else:
            msg = 'utils.get_sql_dtypes: Type ' + str(t) + ' is not handled by create_table.'
            raise TypeError(msg=msg)

    return dtypes


def drop_sql_table(
    name:str,
    engine:sqlalchemy.engine.Engine) -> NoReturn:
    '''Deletes a SQL table from the Find-A-Bug database. The connection should already be to a specific 
    database (as specified in the find-a-bug.cfg file), so no need to specify here.'''

    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(f'DROP TABLE {name}'))


def sql_table_exists(
    name:str,
    engine:sqlalchemy.engine.Engine) -> bool:
    '''Checks for the existence of a table in the database.'''
    
    # Collect a list of all tables in the database.
    with engine.connect() as conn:
        # This returns a list of tuples, so need to extract each table name for this to work.
        tables = conn.execute(sqlalchemy.text('SHOW TABLES')).all()
        tables = [t[0] for t in tables]
    
    return name in tables


def upload_to_sql_table(  
    df:pd.DataFrame,
    name:str,
    engine:sqlalchemy.engine.Engine,
    primary_key=None,
    if_exists='fail'):
    '''Uploads a pandas DataFrame to the SQL database at URL.
    
    args:
        - df: The DataFrame to load into the SQL database.
        - name: The name of the table to create.
        - engine: An engine connected to the SQL database.
        - primary_key: The primary key of the table. 
        - if_exists: One of fail or append. Specifies behavior if table already exists.
    '''
    f = 'utils.create_table'
    assert df.index.name is not None, f'{f}: A labeled index name must be specified.'

    # The multi parameter means that multiple rows are passed at once, which is faster.  
    # Setting index to True means df.index is used (this is the default setting).
    df.to_sql(name, engine, dtype=get_sql_dtypes(df), if_exists=if_exists, chunksize=1000, method='multi')
    
    with engine.connect() as conn:
        if primary_key is not None:
            conn.execute(sqlalchemy.text(f'ALTER TABLE {name} ADD PRIMARY KEY ({primary_key})'))

    # print(f'{f}: Upload to table {name} successful.')


def get_duplicate_annotation_info():
    '''Found that I was not able to make gene_id the primary key when loading annotations into the SQL database. This
    is due to the fact that there were duplicate entries (Josh said this was expected, as genes can have multiple annotations). 
    We were curious about characteristics of these duplications. Are there duplications across genome files?
    How many duplications are there?'''
    f = 'utils.get_duplicate_annotation_info'
    
    annotations_path = load_config_paths()['annotations_path']
    annotation_files = os.listdir(annotations_path)  
    # Dictionary of the format {gene_id:{genome_ids:[], count:n}}
    info = {}

    for file in tqdm(annotation_files, desc=f):    

        genome_id = file.replace('_protein.ko.csv', '') # Add the genome ID, removing the extra stuff. 
        gene_ids = pd.read_csv(os.path.join(annotations_path, file), usecols=['gene name']).values.ravel()

        for gene_id in gene_ids:
            if gene_id in info:
                info[gene_id]['count'] = info[gene_id]['count'] + 1
            else: # If the gene has not yet been encountered... 
                info[gene_id]['count'] = 0
                info[gene_id]['genome_ids'] = []
            info[gene_id]['genome_ids'].append(genome_id) # Log which genomes the genes are encountered in. 

    # Save the information as a pickle file. 
    with open('duplicate_annotation_info.pkl', 'wb') as file:
        pickle.dump(info, file)


