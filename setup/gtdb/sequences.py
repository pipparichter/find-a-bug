import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from typing import NoReturn, List, Tuple
from tqdm import tqdm
import typing

import sys
sys.path.append('../')
from utils import *

TABLE_NAME = 'gtdb_r207_amino_acid_seqs'
BACTERIA_GENOMES_PATH ='/var/lib/pgsql/data/gtdb/r207/amino_acid_seqs/bacteria/' 
ARCHAEA_GENOMES_PATH = '/var/lib/pgsql/data/gtdb/r207/amino_acid_seqs/archaea/'


def parse_genome_file(path:str) -> pd.DataFrame:
    '''Parse a genome FASTA file stored in the protein_aa_reps directory. These files are direct outputs from Prodigal
    (although the gene coordinates are not included).
    
    :param path: The path to the genome file. 
    :return: a pandas DataFrame containing 
    '''

    def parse_header(header):
        '''Parse the header string of an entry in a genome file.'''
        header_info = {} # Dictionary to store the header info. 
        # Headers are of the form >DSBS01000028.1_12 # 8070 # 9911 # 1 # ID=38_12;partial=01;start_type=GTG;rbs_motif=None;rbs_spacer=None;gc_cont=0.442
        pattern = '>([^#]+) # (\d+) # (\d+) # ([-1]+) # ID=(.+)'
        match = re.match(pattern, header)

        header_info['gene_id'] = match.group(1)
        header_info['nt_start'] = int(match.group(2))
        header_info['nt_stop'] = int(match.group(3))
        header_info['reverse'] = True if match.group(4) == '-1' else False
        
        # Iterate over the semicolon-separaed information in the final portion of the header, discarding the ID. 
        for field, value in [item.split('=') in match.group(5).split(';')[1:]]:
            if field == 'gc_cont':
                value = float(value)
            header_info[field] = value

        return header_info

    with open(path, 'r') as genome_file:
        content = genome_file.read()
        # Extract the amino acid sequences from the FASTA file. 
        seqs = re.split(r'^>.*', content, flags=re.MULTILINE)[1:]
        seqs = [s.replace('\n', '') for s in seqs] # Strip all of the newline characters from the amino acid sequences.

        headers = re.findall(r'^>.*', content, re.MULTILINE)
        # Parse he headers. This will be a string of dictionaries, which can be converted to a DataFrame. 
        header_info = [parse_header(header) for header in headers]

    df = pd.DataFrame(header_info) # Initialize the DataFrame with the header information. 
    df['seq'] = seqs # Add the sequences to the DataFrame. 

    return df


def setup(engine):
    '''Iterate over all of the genome FASTA files and load the gene IDs, genome IDs, and amino 
    acid sequences into a pandas DataFrame.'''
    table_exists = False # Make sure to append after the table is initially setupd. 
    for path in [BACTERIA_GENOMES_PATH, ARCHAEA_GENOMES_PATH]:
        
        # Each file corresponds to a different genome, either archaeal or bacterial.
        genome_files = os.listdir(path) 
        genome_file_batches = np.array_split(genome_files, (len(genome_files) // 100) + 1)
        
        for batch in tqdm(genome_file_batches, desc='setup_gtb_r207_amino_acid_seqs.setup'):
            # Process the genome files in chunks to avoid crashing the process.
            dfs = []   
            for genome_file in batch:    
                # Remove the prefix. Don't bother adding the source to this table, as it is already present in the metadata table. 
                genome_id = genome_file.replace('_protein.faa', '')[3:]    
                genome_df = parse_genome_file(os.path.join(path, genome_file))
                genome_df['genome_id'] = genome_id
                dfs.append(genome_df)
                
            df = pd.concat(dfs).fillna('None')

            # Put the table into the SQL database. Add a primary key on the first pass. 
            if not table_exists:
                upload_to_sql_table(df.set_index('gene_id'), TABLE_NAME, engine, primary_key='gene_id', if_exists='replace')
                table_exists = True
            else:
               upload_to_sql_table(df.set_index('gene_id'), TABLE_NAME, engine, primary_key=None, if_exists='append')


if __name__ == '__main__':
    url = get_database_url()
    print(f'Starting engine with URL {url}')
    engine = sqlalchemy.create_engine(url, echo=False)

    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()

    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')






 
