import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
import typing

# from utils import upload_to_sql_table, pd_from_fasta, URL, load_config_paths
from utils import *

# This is where the data is stored on the microbes server. 

BATCH_SIZE = 100 # Size of batches for handling data.
TABLE_NAME = 'gtdb_r207_amino_acid_seqs'

BACTERIA_GENOMES_PATH = load_config_paths()['bacteria_genomes_path']
ARCHAEA_GENOMES_PATH = load_config_paths()['archaea_genomes_path']

# NOTE: We can't make genome_id an index because it is not unique. So will need to manually add genome IDs to the 
# gtdb_r207_annotations_kegg table. To speed up this process, it makes sense to store a map of gene_id to genome_id
# here, although there may be a way I can get around this with joins. 

def setup(engine):
    '''Iterate over all of the genome FASTA files and load the gene IDs, genome IDs, and amino 
    acid sequences into a pandas DataFrame.'''
    f = 'setup_gtb_r207_amino_acid_seqs.setup'

    table_exists = False # Make sure to append after the table is initially setupd. 
    for path in [BACTERIA_GENOMES_PATH, ARCHAEA_GENOMES_PATH]:
        
        # Each file corresponds to a different genome, either archaeal or bacterial.
        genome_files = os.listdir(path) 
        genome_file_batches = np.array_split(genome_files, (len(genome_files) // BATCH_SIZE) + 1)
        
        for batch in tqdm(genome_file_batches, desc=f):
            # Process the genome files in chunks to avoid crashing the process.   
            # Setting is_genome_file=True automatically adds the genome_id to the DataFrame.          
            df = pd.concat([pd_from_fasta(os.path.join(path, g), is_genome_file=True) for g in batch]).fillna('None')

            # Put the table into the SQL database. Add a primary key on the first pass. 
            if not table_exists:
                upload_to_sql_table(df.set_index('gene_id'), TABLE_NAME, engine, primary_key='gene_id', if_exists='replace')
                table_exists = True
            else:
               upload_to_sql_table(df.set_index('gene_id'), TABLE_NAME, engine, primary_key=None, if_exists='append')


if __name__ == '__main__':
    print(f'Starting engine with URL {URL}')
    engine = sqlalchemy.create_engine(URL, echo=False)

    if sql_table_exists(TABLE_NAME, engine):
        drop_sql_table(TABLE_NAME, engine)
        print(f'Dropped existing table {TABLE_NAME}.')

    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()
    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')
 
