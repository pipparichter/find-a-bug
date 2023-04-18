import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm

import setup



def read_gene_to_genome_map(filepath='./gene_to_genome_map.csv'):
    '''
    Read the gene to genome map from a file. 
    '''
    map_ = {}
    
    with pd.read_csv(filepath, chunksize=1000) as reader:
        for chunk in reader:
            for row in chunk.itertuples():
                map_[row.gene_name] = row.genome_id

    return map_


def load(engine):
    '''
    Load an annotation file into a pandas DataFrame. This function also renames
    the columns to match my naming convention, and drops unnecessary columns
    (like the KO definition). 
    '''

    gene_to_genome_map = read_gene_to_genome_map()

    if len(gene_to_genome_map) == 0:
        # If it is not already populated, try reading in the map from the file. 
        gene_to_genome_map = read_gene_to_genome_map()
        print('LOADED gene_to_genome_map')
        if len(gene_to_genome_map) == 0:
            raise Exception('The gene-to-genome map must be populated before calling this function')
    
    dfs = []
    # Look through every annotations file in the data directory. 

    files = os.listdir(f'{setup.data_dir}/gtdb/r207/annotations/kegg/v1')    
    nfiles = len(files)
    files_chunk_size = 500
    
    # Keep track of number of things uploaded for the unique id column.
    curr_id = 0
        
    for i in tqdm(range(int(nfiles / files_chunk_size) + 1), desc='Loading annotation file chunks...'):    
        
        files_chunk = files[(files_chunk_size*i):min(nfiles, files_chunk_size*(i+1))] 
        
        dfs = []
        for f in files_chunk:
            # os.listdir doesn't spit out the entire filepath. 
            path = f'{setup.data_dir}/gtdb/r207/annotations/kegg/v1/{f}'
            # df = pd.read_csv(f, delimiter='\t', header=0, names=['gene_name', 'ko', 'threshold', 'score', 'e_value'])
            df = pd.read_csv(path, header=0, names=['gene_name', 'ko', 'threshold', 'score', 'e_value'])

            # For now, until I bother loading up all species, remove any
            # gene_names which do not have an associated genome ID. 
            try:
                # Add the genome ID to this table. 
                genome_ids = []
                for gene_name in df['gene_name']:
                    genome_ids.append(gene_to_genome_map[gene_name])
                df['genome_id'] = genome_ids
             
                # Add a column for unique primary keys. 
                df['unique_id'] = np.arange(curr_id, curr_id + df.shape[0])
                curr_id += df.shape[0]
            except KeyError:
                raise Exception('One of the genes in the annotation files was not present in the gene-to-genome map.')

            # Add to list of DataFrames...
            dfs.append(df)
        
        # Upload the chunk of data.
        if i == 0:
            # Only create indices on the first pass. 
            kwargs = {'primary_key':'unique_id', 'index':'gene_name'}
            setup.create_table(pd.concat(dfs), 'gtdb_r207_annotations_kegg', engine, **kwargs)
        else:
            setup.create_table(pd.concat(dfs), 'gtdb_r207_annotations_kegg', engine)


if __name__ == '__main__':
    
    print(f'Starting engine with URL {setup.url}')
    engine = sqlalchemy.create_engine(setup.url, echo=False)
    
    t_init = perf_counter()
    
    load(engine)

    t_final = perf_counter()

    print(f'\nTable uploaded in {t_final - t_init} seconds.')
 
