import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm

import sys
sys.path.append('../') # Make the utils directory accessible. 
from utils import * 

ANNOTATIONS_DIR_PATH = '/var/lib/pgsql/data/gtdb/r207/annotations/kegg/v1'
TABLE_NAME = 'gtdb_r207_annotations_kegg'

# In order to be able to remove duplicates, I need to load in the entire DataFrame at once. 
# Although I have a question about whether or not the duplicated gene_ids are within single genomes, or across several?
# I looked into it -- seems as though there are no cross-genome duplications! All instances of multiple copies of a gene ID are within
# a single genome, which means I can still batch by genome file if I merge rows. 

def setup(engine):
    '''Load an annotation file into a pandas DataFrame. This function also renames the columns to match my naming convention, 
    and drops  some columns that we are not currently using.'''

    annotation_files = os.listdir(ANNOTATIONS_DIR_PATH) 
    annotation_file_batches = np.array_split(annotation_files, (len(annotation_files) // 500) + 1)

    annotation_id = 0
    table_exists = False
    for batch in tqdm(annotation_file_batches, desc='setup_gtb_r207_annotations_kegg.setup'):    
        
        batch_df = [] # Accumulate the DataFrames for a batch. 
        for file in batch:
            # Need to lad everything in as strings, so that I can merge duplicate rows. Not sure how efficient this is. 
            df = pd.read_csv(os.path.join(ANNOTATIONS_DIR_PATH, file), header=0, names=['gene_id', 'ko', 'threshold', 'score', 'e_value'])

            genome_id = file.replace('_protein.ko.csv', '') # Add the genome ID, removing the extra stuff. 
            _, genome_id = genome_id[:2], genome_id[3:] # Remove the RS or GB prefix.

            df['genome_id'] =  genome_id
            df['annotation_id'] = np.arange(annotation_id, annotation_id + len(df)) # Add unique annotation_id for the primary key. 
            annotation_id += len(df)
            batch_df.append(df)

        batch_df = pd.concat(batch_df) # combine all DataFrames. 
         # Put the table into the SQL database. Add a primary key on the first pass. 
        if not table_exists:
            upload_to_sql_table(batch_df.set_index('annotation_id'), TABLE_NAME, engine, primary_key='annotation_id', if_exists='replace')
            table_exists = True
        else:
            upload_to_sql_table(batch_df.set_index('annotation_id'), TABLE_NAME, engine, primary_key=None, if_exists='append')


if __name__ == '__main__':
    url = get_database_url()
    print(f'Starting engine with URL {url}')
    engine = sqlalchemy.create_engine(url, echo=False)

    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()
    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')
 
