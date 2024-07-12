'''Utilities for reading and writing from FASTA files (and probably some other generic use stuff later on).'''

import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import sqlalchemy
from sqlalchemy import Integer, Float, Boolean
from sqlalchemy.dialects.mysql import VARCHAR, LONGTEXT
import os   
from typing import Dict, TypeVar, NoReturn
import pickle
import subprocess


def get_database_url() -> str:
    '''Load the URL to use for accessing the SQL Database.''' 
    host = '127.0.0.1' # Equivalent to localhost, although not sure why this would work and localhost doesn't.
    dialect = 'mariadb'
    driver = 'pymysql'
    user = 'root'
    password = 'Doledi7-Bebyno2'
    name = 'findabug'
    return  f'{dialect}+{driver}://{user}:{password}@{host}/{name}'


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


def csv_size(path):
    '''Get the number of entries in a FASTA file.'''
    n = subprocess.run(f'wc -l {path}', capture_output=True, text=True, shell=True, check=True).stdout.split()[0]
    n = int(n) - 1 # Convert text output to an integer and disregard the header row.
    return n

def remove_prefix(genome_id:str) -> str:
    '''The genome file names from GTDB have a GB_ or RF_ prefix, indicating whether the source of the genome is GenBank or RefSeq. In order for the genome
    ID to match correctly with sequence entries, these prefixes need to be removed.

    :param genome_id: A genome ID with the RF_ or GB_ prefix.
    :return: The genome ID with the prefix removed.
    '''
    return genome_id[3:]


def df_from_fasta(path:str) -> pd.DataFrame:
    '''Load a FASTA file in as a pandas DataFrame. If the FASTA file is for a particular genome, then 
    add the genome ID as an additional column.'''
    gene_ids = fasta_gene_ids(path)
    seqs = fasta_seqs(path)
    df = pd.DataFrame({'seq':seqs, 'gene_id':gene_ids})
    return df


def df_to_fasta(df, path=None, textwidth=80):
    '''Convert a pandas DataFrame containing FASTA data to a FASTA file format.'''

    assert df.index.name == 'id', 'utils.df_to_fasta: Gene ID must be set as the DataFrame index before writing.'

    fasta = ''
    for row in tqdm(df.itertuples(), desc='utils.df_to_fasta', total=len(df)):
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
    dtypes = {'seq':LONGTEXT, 'gene_id':VARCHAR(150), 'genome_id':VARCHAR(150), 'prodigal_unique_id':VARCHAR(150)}

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
            raise TypeError(msg)

    return dtypes


def drop_sql_table(engine:sqlalchemy.engine.Engine, table_name:str,) -> NoReturn:
    '''Deletes a SQL table from the SQL database'''
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(f'DROP TABLE {table_name}'))


def sql_table_exists(engine:sqlalchemy.engine.Engine, table_name:str) -> bool:
    '''Checks for the existence of a table in the database.'''
    # Collect a list of all tables in the database.
    with engine.connect() as conn:
        # This returns a list of tuples, so need to extract each table name for this to work.
        tables = conn.execute(sqlalchemy.text('SHOW TABLES')).all()
        tables = [t[0] for t in tables]
    return table_name in tables


def upload_to_sql_table(engine:sqlalchemy.engine.Engine, df:pd.DataFrame, table_name:str, primary_key:str=None, if_exists='fail'):
    '''Uploads a pandas DataFrame to the SQL database.
    
    :param engine: The engine connecting the Python script to the SQL database.
    :param df: The DataFrame to load into the SQL database.
    :param table_name: The name of the table to create.
    :param primary_key: The primary key of the table. 
    :param if_exists: One of fail, replace, or append. Specifies behavior if table already exists.
    '''
    assert df.index.name is not None, f'upload_to_sql_table: A labeled index name must be specified.'

    # The multi parameter means that multiple rows are passed at once, which is faster.  
    # Setting index to True means df.index is used (this is the default setting).
    df.to_sql(table_name, engine, dtype=get_sql_dtypes(df), if_exists=if_exists, chunksize=1000, method='multi')
    if (if_exists != 'append') and (primary_key is not None): # If appending to an existing table, don't try to set the primary key again.
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(f'ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})'))


def get_file_batches(dir_path:str, batch_size:int=500) -> np.ndarray:
    '''There are too many annotation files to process all at once. They must be uploaded to the SQL database in 
    batches of size batch_size.

    :param dir_path: The location of the files. 
    :param batch_size: The size of the file batches.
    :return: An array of arrays, where each sub-array is a list of filenames.
    '''
    files = os.listdir(dir_path) # Get all files in the directory, names are {genome_id}_protein.ko.csv.
    return np.array_split(files, (len(files) // batch_size) + 1)

def batch_upload_to_sql_table(engine:sqlalchemy.engine.Engine, 
    dir_path:str=None, 
    genome_id_from_filename=None, 
    df_from_file=None, table_name:str=None, 
    primary_key:str=None, 
    unique_id_name:str=None, 
    batch_size:int=500,
    if_exists:str='replace') -> NoReturn:
    '''Set up a SQL table for a group of files by uploading in batches.
    
    :param engine: The engine connecting the Python script to the SQL database.
    :param dir_path: The location of the annotation files. 
    :param get_genome_id: A function which extracts the genome ID from the filename. This is different for different
        annotation files, as the naming schema are not consistent.
    :param read_annotation_file: A function for reading an annotation file into a pandas DataFrame. 
    :param table_name: The name of the SQL table.
    :param primary_key: The name of the field to set as primary key.
    :param unique_id_name: Name for the unique ID column. If specified, a unique ID is added to the DataFrame.
    :param batch_size: The number of files to process in each batch.
    :param if_exists: One of fail, replace, or append. Specifies behavior for the first batch only. 
    '''
    unique_id = 0 

    for batch in tqdm(get_file_batches(dir_path, batch_size=batch_size), desc='batch_upload_to_sql_table'):  
        batch_df = [] # Accumulate DataFrames over an entire batch of files.
        for filename in batch:
            genome_id = genome_id_from_filename(filename) # Extract the genome ID from the filename.
            df = df_from_file(dir_path, filename)
            df['genome_id'] = genome_id
            if unique_id_name is not None:
                df[unique_id_name] = np.arange(unique_id, unique_id + len(df)) # Add unique ID for the primary key. 
            unique_id += len(df)
            batch_df.append(df)

        batch_df = pd.concat(batch_df) # Combine all DataFrames for the batch.  
        # print(f'batch_upload_to_sql_table: Uploading {len(batch_df)} items to the SQL database.') 
        upload_to_sql_table(engine, batch_df.set_index(primary_key), table_name, primary_key=primary_key, if_exists=if_exists)
        if_exists = 'append' # Switch to append mode after the initial pass.


# It's possible I'll have to do this in chunks? Using LIMIT and OFFSET for pagination. 
def get_column_data(engine, table, column):
    '''Query the database to get information from a column.'''
    with engine.begin() as conn:
        # The execute method returns a CursorResult. Calling all returns a list of Row objects.
        # Row objects behave "as much like Python named tuples as possible." 
        rows = conn.execute(sqlalchemy.text(f'SELECT {column} from {table}')).all()
        # Because we are only selecting one column at a time, should only be one element in each Row. 
        rows = [r[0] for r in rows]
    return rows


def get_table_size(engine, table):
    '''Gets the number of entries in a table.'''
    with engine.begin() as conn:
        # The execute method returns a CursorResult. Calling all returns a list of Row objects.
        # Row objects behave "as much like Python named tuples as possible." 
        count = conn.execute(sqlalchemy.text(f'SELECT COUNT(*) from {table}')).scalar_one()
        # Because we are only selecting one column at a time, should only be one element in each Row. 
    return count 
