import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm
# import h5py
from utils import upload_to_sql_table, pd_from_fasta, URL, load_config_paths

ANNOTATIONS_PATH = load_config_paths()['annotations_path']
# BATCH_SIZE = 500
TABLE_NAME = 'gtdb_r207_annotations_kegg'

# In order to be able to remove duplicates, I need to load in the entire DataFrame at once. 
# Although I have a question about whether or not the duplicated gene_ids are within single genomes, or across several?

def setup(engine):
    '''Load an annotation file into a pandas DataFrame. This function also renames the columns to match my naming convention, 
    and drops  some columns that we are not currently using.'''
    f = 'setup_gtb_r207_annotations_kegg.setup'
    
    annotation_files = os.listdir(ANNOTATIONS_PATH) 

    # gene_to_genome_map = h5py.File('gene_to_genome_map.h5', 'r') # Read in the HDF file. 
    curr_id = 0 # Need to add a unique ID for every entry, as there are duplicate gene)ids in these files. Is this OK?
    for file in tqdm(annotation_files, desc=f):    
        
        batch_df = [] # Accumulate the DataFrames for a batch. 
        for file in batch:
            # df = pd.read_csv(f, delimiter='\t', header=0, names=['gene_name', 'ko', 'threshold', 'score', 'e_value'])
            df = pd.read_csv(os.path.join(ANNOTATIONS_PATH, file), header=0, names=['gene_id', 'ko', 'threshold', 'score', 'e_value'])
            df['genome_id'] = file.replace('_protein.ko.csv', '') # Add the genome ID, removing the extra stuff. 
            # Add a column for unique primary keys. 
            df['unique_id'] = np.arange(curr_id, curr_id + df.shape[0])
            curr_id += df.shape[0]
        
         # Put the table into the SQL database. Add a primary key on the first pass. 
        if not table_exists:
            upload_to_sql_table(df.set_index('unique_id'), TABLE_NAME, engine, primary_key='gene_id', if_exists='replace')
            table_exists = True
        else:
            upload_to_sql_table(df.set_index('gene_id'), TABLE_NAME, engine, primary_key=None, if_exists='append')
# def setup(engine):
#     '''Load an annotation file into a pandas DataFrame. This function also renames the columns to match my naming convention, 
#     and drops  some columns that we are not currently using.'''
#     f = 'setup_gtb_r207_annotations_kegg.setup'
    
#     annotation_files = os.listdir(ANNOTATIONS_PATH) 
#     annotation_file_batches = np.array_split(annotation_files, (len(annotation_files) // BATCH_SIZE) + 1)

#     # gene_to_genome_map = h5py.File('gene_to_genome_map.h5', 'r') # Read in the HDF file. 
#     curr_id = 0 # Need to add a unique ID for every entry, as there are duplicate gene)ids in these files. Is this OK?
#     table_exists = False
#     duplicates = []
#     for batch in tqdm(annotation_file_batches, desc=f):    
        
#         batch_df = [] # Accumulate the DataFrames for a batch. 
#         for file in batch:
#             # df = pd.read_csv(f, delimiter='\t', header=0, names=['gene_name', 'ko', 'threshold', 'score', 'e_value'])
#             df = pd.read_csv(os.path.join(ANNOTATIONS_PATH, file), header=0, names=['gene_id', 'ko', 'threshold', 'score', 'e_value'])
#             df['genome_id'] = file.replace('_protein.ko.csv', '') # Add the genome ID, removing the extra stuff. 
#             # Add a column for unique primary keys. 
#             df['unique_id'] = np.arange(curr_id, curr_id + df.shape[0])
#             curr_id += df.shape[0]
        
#          # Put the table into the SQL database. Add a primary key on the first pass. 
#         if not table_exists:
#             upload_to_sql_table(df.set_index('unique_id'), TABLE_NAME, engine, primary_key='gene_id', if_exists='replace')
#             table_exists = True
#         else:
#             upload_to_sql_table(df.set_index('gene_id'), TABLE_NAME, engine, primary_key=None, if_exists='append')


if __name__ == '__main__':
    
    print(f'Starting engine with URL {URL}')
    engine = sqlalchemy.create_engine(URL, echo=False)
    t_init = perf_counter()
    setup(engine)
    t_final = perf_counter()
    print(f'\nTable {TABLE_NAME} uploaded in {t_final - t_init} seconds.')
 
