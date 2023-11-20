import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
from utils import * 

ANNOTATIONS_PATH = load_config_paths()['annotations_path']
BATCH_SIZE = 500
TABLE_NAME = 'gtdb_r207_annotations_kegg'

# In order to be able to remove duplicates, I need to load in the entire DataFrame at once. 
# Although I have a question about whether or not the duplicated gene_ids are within single genomes, or across several?
# I looked into it -- seems as though there are no cross-genome duplications! All instances of multiple copies of a gene ID are within
# a single genome, which means I can still batch by genome file if I merge rows. 

def setup(engine):
    '''Load an annotation file into a pandas DataFrame. This function also renames the columns to match my naming convention, 
    and drops  some columns that we are not currently using.'''
    f = 'setup_gtb_r207_annotations_kegg.setup'
    
    annotation_files = os.listdir(ANNOTATIONS_PATH) 
    annotation_file_batches = np.array_split(annotation_files, (len(annotation_files) // BATCH_SIZE) + 1)

    annotation_id = 0
    table_exists = False
    for batch in tqdm(annotation_file_batches, desc=f):    
        
        batch_df = [] # Accumulate the DataFrames for a batch. 
        for file in batch:
            # Need to lad everything in as strings, so that I can merge duplicate rows. Not sure how efficient this is. 
            df = pd.read_csv(os.path.join(ANNOTATIONS_PATH, file), header=0, names=['gene_id', 'ko', 'threshold', 'score', 'e_value'])
            df['genome_id'] = file.replace('_protein.ko.csv', '') # Add the genome ID, removing the extra stuff. 
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
    print(f'Starting engine with URL {URL}')
    engine = sqlalchemy.create_engine(URL, echo=False)

    if sql_table_exists(TABLE_NAME, engine):
        drop_sql_table(TABLE_NAME, engine)
        print(f'Dropped existing table {TABLE_NAME}.')
        
    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()
    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')
 
