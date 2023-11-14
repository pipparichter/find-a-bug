import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
import typing
import configparser

from utils import upload_to_sql_table, pd_from_fasta, URL

# This is where the data is stored on the microbes server. 

BATCH_SIZE = 500 # Size of batches for handling data.
TABLE_NAME = 'gtdb_r207_amino_acid_seqs'

# Read in the config file, which is in the project root directory. 
config = configparser.ConfigParser()
# with open('/home/prichter/Documents/find-a-bug/find-a-bug.cfg', 'r', encoding='UTF-8') as f:
with open(os.path.join(os.path.dirname(__file__), '../', '../', 'find-a-bug.cfg'), 'r', encoding='UTF-8') as f:
    config.read_file(f)

BACTERIA_GENOMES_PATH = config.items('paths')['bacteria_genomes_path']
ARCHAEA_GENOMES_PATH = config.items('paths')['archaea_genomes_path']

# NOTE: We can't make genome_id an index because it is not unique. So will need to manually add genome IDs to the 
# gtdb_r207_annotations_kegg table. To speed up this process, it makes sense to store a map of gene_id to genome_id
# here, although there may be a way I can get around this with joins. 

def setup(engine):
    '''Iterate over all of the genome FASTA files and load the gene IDs, genome IDs, and amino 
    acid sequences into a pandas DataFrame.'''
    f = 'setup_gtb_r207_amino_acid_seqs.setup'

    # Should just make it in the current working directory, so no need to specify a path. 
    # gene_to_genome_map = pd.DataFrame(columns=['gene_id', 'genome_id']).to_hdf('gene_to_genome_map.h5', key='gene_to_genome_map')
    gene_to_genome_map = h5py.File('gene_to_genome_map.h5', 'w') 

    table_exists = False # Make sure to append after the table is initially setupd. 
    for path in [BACTERIA_GENOMES_PATH, ARCHAEA_GENOMES_PATH]:
        
        # Each file corresponds to a different genome, either archaeal or bacterial. 
        genome_file_batches = np.array_split(os.listdir(path), (n // BATCH_SIZE) + 1)
        n = len(os.listdir(path))
        
        for batch in tqdm(genome_file_batches, desc=f):
            # Process the genome files in chunks to avoid crashing the process.             
            df = pd.concat([pd_from_fasta(os.path.join(path, g), is_genome_file=True) for g in batch]).fillna('None')

            # Write the gene-to-genome information to the file. Not totally sure what append=True does?
            # df[['genome_id', 'gene_id']].to_hdf('gene_to_genome_map.h5', key='gene_to_genome_map', append=True)
            for row in df[['genome_id', 'gene_id']].itertuples():
                gene_to_genome_map[row.gene_id] = row.genome_id

            # Put the table into the SQL database. Add a primary key on the first pass. 
            if not table_exists:
                upload_to_sql_table(df, TABLE_NAME, engine, primary_key='gene_id', if_exists='replace')
                table_exists = True
            else:
               upload_to_sql_table(df, TABLE_NAME, engine, primary_key=None, if_exists='append')
    # Close the HDF file after writing is done. 
    gene_to_genome_map.close()

if __name__ == '__main__':
    print(f'Starting engine with URL {URL}')
    engine = sqlalchemy.create_engine(URL, echo=False)
    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()
    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')
 
