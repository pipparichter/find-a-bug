'''Python script for setting up SQL tables for protein annotations.'''
import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
from typing import NoReturn, List
from utils import * 
import argparse


def setup_kegg(engine, dir_path:str='/var/lib/pgsql/data/gtdb/r207/annotations/kegg/v1', table_name:str='gtdb_r207_annotations_kegg') -> NoReturn:
    '''Set up a SQL table for KEGG annotations.
    
    :param engine: The engine connecting the Python script to the SQL database.
    :param dir_path: The location of the annotation files. 
    '''
    # If the table already exists, drop it and re-upload things.
    if sql_table_exists(table_name, engine):
        drop_sql_table(table_name, engine)
        print(f'Dropped existing table {table_name}.')

    def genome_id_from_filename(filename:str) -> str:
        '''The genome IDs are given by the name of the annotation files. This function extracts the genome from the 
        filename of KEGG annotation files. 

        :param filename: The original filename.
        :return: The genome ID with the prefix removed.
        '''
        genome_id = filename.replace('_protein.ko.csv', '')
        return remove_prefix(genome_id)

    def df_from_file(dir_path:str, filename:str) -> pd.DataFrame:
        '''Read a KEGG annotation file into a pandas DataFrame with the appropriate column headings.
        
        :param data_dir: The directory in which the annotation files are stored.
        :param filename: The name of the annotation file. 
        :return: A pandas DataFrame containing the annotation data. 
        '''
        # Headers are provided in the KEGG annotation files, but should be renamed. 
        headers = ['gene_id', 'ko', 'threshold', 'score', 'e_value'] # Define the new column headers. 
        return pd.read_csv(os.path.join(dir_path, filename), header=0, names=headers) # Read in the CSV file. 

    batch_upload_to_sql_table(engine, dir_path=dir_path, genome_id_from_filename=genome_id_from_filename, primary_key='pfam_id', unique_id_name='pfam_id', df_from_file=df_from_file, table_name=table_name)


def setup_pfam(engine, dir_path:str='/var/lib/pgsql/data/gtdb/r207/annotations/pfam/v1', table_name:str='gtdb_r207_annotations_pfam') -> NoReturn: 
    '''Set up a SQL table for Pfam annotations.
    
    :param engine: The engine connecting the Python script to the SQL database.
    :param dir_path: The location of the annotation files. 
    '''
    # If the table already exists, drop it and re-upload things.
    if sql_table_exists(engine, table_name=table_name):
        drop_sql_table(engine, table_name=table_name)
        print(f'Dropped existing table {table_name}.')

    def genome_id_from_filename(filename:str) -> str:
        '''The genome IDs are given by the name of the annotation files. This function extracts the genome from the 
        filename of Pfam annotation files. 

        :param filename: The original filename.
        :return: The genome ID with the prefix removed.
        '''
        # Filenames for Pfam annotations are just the genome ID followed by the CSV extension.
        genome_id = filename.replace('.tsv', '')


    def df_from_file(dir_path:str, filename:str) -> pd.DataFrame:
        '''Read a KEGG annotation file into a pandas DataFrame with the appropriate column headings.
        
        :param data_dir: The directory in which the annotation files are stored.
        :param filename: The name of the annotation file. 
        :return: A pandas DataFrame containing the annotation data. 
        '''
        # Headers are not provided in the Pfam annotation files. 
        headers = ['gene_id', 'digest', 'length', 'analysis', 'signature_accession', 'signature_description', 'start', 'stop', 'e_value', 'match_status', 'data', 'interpro_accession', 'interpro_description']
        return pd.read_csv(os.path.join(dir_path, filename), header=None, names=headers, sep='\t') # Read in the TSV file. 
    
    batch_upload_to_sql_table(engine, dir_path=dir_path, genome_id_from_filename=genome_id_from_filename, primary_key='kegg_id', unique_id_name='kegg_id', df_from_file=df_from_file, table_name=table_name)



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('annotation_type', choices=['pfam', 'kegg'], type=str, help='The type of annotation file to create.')
    parser.add_argument('--table-name', '-n', type=str, default=None, help='The name of the SQL table to be created.')
    parser.add_argument('--dir-path', '-d', type=str, default=None, help='The data directory where the annotation files are stored.')

    args = parser.parse_args()

    # Extract any specified parameters from the command line.
    params = dict()
    if args.table_name is not None:
        params['table_name'] = args.table_name
    if args.dir_path is not None:
        params['dir_path'] = args.dir_path

    url = get_database_url()
    engine = sqlalchemy.create_engine(url, echo=False)

    t_init = perf_counter()
    if args.annotation_type == 'pfam':  
        setup_pfam(engine, **params)
    elif args.annotation_type == 'kegg':
        setup_kegg(engine, **params)

    t_final = perf_counter()
    print(f'\nTable uploaded in {t_final - t_init} seconds.')
 
