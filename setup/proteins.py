'''Python script for setting up SQL tables with amino acid sequences.'''
import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from typing import NoReturn, List, Tuple, Dict
from tqdm import tqdm
import argparse
from utils import *


def df_from_file(data_dir:str, filename:str) -> pd.DataFrame:
    '''Parse a genome FASTA file stored in the protein_aa_reps directory. These files are direct outputs from Prodigal.
    
    :param data_dir: The directory in which the FASTA files are stored.
    :param filename: The name of the FASTA file. 
    :return: a pandas DataFrame containing the sequence information for the genome. 
    '''

    def parse_header(header:str) -> Dict[str, object]:
        '''Parse the header string of an entry in a genome file. Headers are of the form:
        >{gene_id} # {nt_start} # {nt_stop} # {reverse} # ID={prodigal_id};partial={partial};start_type={start_type};rbs_motif={rbs_motif};rbs_spacer={rbs_spacer};gc_cont={gc_cont}.
        Full descriptions of each field can be found here: https://github.com/hyattpd/Prodigal/wiki/understanding-the-prodigal-output
        
        :param header: The header string for the sequence.
        :return: A dictionary mapping each field in the header to a value.
        ''' 
        header_dict = dict() # Dictionary to store the header info. 
        pattern = '>([^#]+) # (\d+) # (\d+) # ([-1]+) # (.+)' # Pattern matchin the header
        match = re.match(pattern, header)

        header_dict['gene_id'] = match.group(1)
        header_dict['nt_start'] = int(match.group(2))
        header_dict['nt_stop'] = int(match.group(3))
        header_dict['reverse'] = True if match.group(4) == '-1' else False
        
        # Iterate over the semicolon-separaed information in the final portion of the header. 
        for field, value in [item.split('=') for item in match.group(5).split(';')]:
            value = float(value) if (field == 'gc_cont') else value
            field = 'prodigal_id' if (field == 'ID') else field
            header_dict[field] = value

        return header_dict

    with open(path, 'r') as genome_file:
        content = genome_file.read() # Extract the amino acid sequences from the FASTA file. 
        seqs = re.split(r'^>.*', content, flags=re.MULTILINE)[1:]
        seqs = [s.replace('\n', '') for s in seqs] # Strip all of the newline characters from the amino acid sequences.
        headers = re.findall(r'^>.*', content, re.MULTILINE)

    df = pd.DataFrame([parse_header(header) for header in headers]) # Initialize the DataFrame with the header information. 
    df['seq'] = seqs # Add the sequences to the DataFrame. 
    return df


def genome_id_from_filename(filename:str) -> str:
    '''The genome IDs are given by the name of the FASTA files.
    :param filename: The original filename.
    :return: The genome ID with the prefix removed
    '''
    genome_id = filename.replace('fasta', '')
    return remove_prefix(genome_id)


def setup(engine:sqlalchemy.engine.Engine, bacteria_dir_path:str='/var/lib/pgsql/data/gtdb/r207/amino_acid_seqs/bacteria/', archaea_dir_path:str='/var/lib/pgsql/data/gtdb/r207/amino_acid_seqs/archaea/'):

    # If the table already exists, drop it and re-upload things.
    if sql_table_exists(table_name, engine):
        drop_sql_table(table_name, engine)
        print(f'Dropped existing table {table_name}.')

    batch_upload_to_sql_table(engine, dir_path=bacteria_dir_path, genome_id_from_filename=genome_id_from_filename, primary_key='gene_id', df_from_file=df_from_file, table_name=table_name)
    batch_upload_to_sql_table(engine, dir_path=archaea_dir_path, genome_id_from_filename=genome_id_from_filename, primary_key='gene_id', df_from_file=df_from_file, table_name=table_name, if_exists='append')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--table-name', '-n', type=str, optional=True, default=None, help='The name of the SQL table to be created.')
    parser.add_argument('--bacteria-dir-path', '-b', type=str, optional=True, default=None, help='The data directory where the archaeal sequence files are stored.')
    parser.add_argument('--archaea-dir-path', '-a', type=str, optional=True, default=None, help='The data directory where the bacterial sequence files are stored.')

    args = parser.parse_args()

    # Extract any specified parameters from the command line.
    params = dict()
    if args.table_name is not None:
        params['table_name'] = args.table_name
    if args.bacteria_dir_path is not None:
        params['bacteria_dir_path'] = args.bacteria_dir_path
    if args.archea_dir_path is not None:
        params['archaea_dir_path'] = args.archaea_dir_path

    url = get_database_url()
    engine = sqlalchemy.create_engine(url, echo=False)

    t_init = perf_counter()
    setup(engine, **params)

    t_final = perf_counter()
    print(f'\nTable uploaded in {t_final - t_init} seconds.')





 
