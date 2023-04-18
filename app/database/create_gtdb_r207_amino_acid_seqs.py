import pandas as pd
import sqlalchemy
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm

import setup

def write_gene_to_genome_map(gene_to_genome_map, filepath='./gene_to_genome_map.csv'):
    '''
    Write the gene to genome map to a file. 
    '''
    # Write to file in chunks so the process doesn't get killed. 
    chunk_size = 1000
    n = len(gene_to_genome_map)
    gene_names = list(gene_to_genome_map.keys())
    
    for i in tqdm(range(int(n/chunk_size) + 1), desc='Writing gene_to_genome_map...'):
        chunk = gene_names[i*chunk_size:min((i+1)*chunk_size, n)]

        df = {'gene_name':[], 'genome_id':[]}
        for gene_name in chunk:
            df['gene_name'].append(gene_name)
            df['genome_id'].append(gene_to_genome_map[gene_name])
        df = pd.DataFrame(df)
        # mode='a' indicated append mode... 
        df.to_csv(filepath, mode='a')


# NOTE: Probably want to edit this to extract more info. 
def parse_fasta(path):
    '''
    Takes a path to a FASTA (.fa) file as input. Reads through all entries and
    extracts the amino acid sequences, as well as the gene names.  
    '''
    seqs, genes = [], []
    
    # It doesn't seem like the FASTA file is too large to read in as a single
    # string.
    with open(path, 'r') as f:
        # For some reason the first entry is registering as nothing. 
        entries = f.read().split('>')[1:]
        for e in entries:
            e = e.split(' # ')
            genes.append(e[0])
            # What are all the other things in the FASTA entry? Do we need them?
            # Remove the info before the first newline to get just the sequence. 
            seqs.append(''.join(e[-1].split('\n')[1:]).replace(' ', ''))
    
    return len(entries), {'gene_name':genes, 'sequence':seqs}


def load(engine, create_gene_to_genome_map=False):
    '''
    Iterate over all of the files in proteins_faa_reps/ and load the
    gene names, genome IDs, and amino acid sequences into a pandas DataFrame. 
    '''
    gene_to_genome_map = {}

    paths = [f'{setup.data_dir}/gtdb/r207/amino_acid_seqs/bacteria/',
            f'{setup.data_dir}/gtdb/r207/amino_acid_seqs/archaea/']

    for path in paths:
        
        # Each file belongs to a different species. 
        species = os.listdir(path)
        nspecies = len(species)
        species_chunk_size = 500
        
        for i in tqdm(range(int(nspecies / species_chunk_size) + 1), desc='Loading species...'):
            
            df = pd.DataFrame(columns=['genome_id', 'gene_name', 'sequence'])
            species_chunk = species[(species_chunk_size*i):min(nspecies, species_chunk_size*(i+1))]
            
            for genome_filename in species_chunk:
                n, d = parse_fasta(path + genome_filename)

                # File names are of the form {genome_id}_protein.faa
                genome_id = genome_filename[:-len('_protein.faa')]
                # Add the genome information to this table. 
                d['genome_id'] = [genome_id for i in range(n)]
                
                # Add the genome information to the gene-to-genome map. 
                for gene_name in d['gene_name']:
                    gene_to_genome_map[gene_name] = genome_id

                # Add the species information to the pandas DataFrame.
                df = pd.concat([df, pd.DataFrame(d)], ignore_index=True)
            
            # Put the table into the SQL database. Add a primary key on the first pass. 
            if paths == paths[0] and i == 0:
                setup.create_table(df.fillna('None'), 'gtdb_r207_amino_acid_seqs', engine, primary_key='gene_name')
            else:
                setup.create_table(df.fillna('None'), 'gtdb_r207_amino_acid_seqs', engine, primary_key=None)
    
    if create_gene_to_genome_map:
        # Write the gene_to_genome map to a file. This takes a while. 
        write_gene_to_genome_map(gene_to_genome_map)


if __name__ == '__main__':
    
    print(f'Starting engine with URL {setup.url}')
    engine = sqlalchemy.create_engine(setup.url, echo=False)
    
    t_init = perf_counter()
    
    load(engine, create_gene_to_genome_map=False)

    t_final = perf_counter()

    print(f'\nTable uploaded in {t_final - t_init} seconds.')
 
