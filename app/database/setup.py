'''
Use this file to load data from FASTA and CSV files into the MySQL database. 
'''
import sqlalchemy
from sqlalchemy import Integer, Float, text, Boolean
from sqlalchemy.dialects.mysql import VARCHAR, TEXT, LONGTEXT, BLOB
import pandas as pd
import numpy as np
import os
from time import perf_counter
from tqdm import tqdm

gene_to_genome_map = {}

def write_gene_to_genome_map(filepath='./gene_to_genome_map.csv'):
    '''
    Write the gene to genome map to a file. 
    '''
    global gene_to_genome_map

    # Write to file in chunks so the process doesn't get killed. 
    chunk_size = 1000
    n = len(gene_to_genome_map)
    gene_names = list(gene_to_genome_map.keys())
    
    if n == 0:
        raise Exception('gene-to-genome map has not been populated.')
    
    for i in tqdm(range(int(n/chunk_size) + 1), desc='Writing gene_to_genome_map...'):
        chunk = gene_names[i*chunk_size:min((i+1)*chunk_size, n)]

        df = {'gene_name':[], 'genome_id':[]}
        for gene_name in chunk:
            df['gene_name'].append(gene_name)
            df['genome_id'].append(gene_to_genome_map[gene_name])
        df = pd.DataFrame(df)
        # mode='a' indicated append mode... 
        df.to_csv(filepath, mode='a')


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


def load_gtdb_r207_annotations_kegg(data_dir, engine, **kwargs):
    '''
    Load an annotation file into a pandas DataFrame. This function also renames
    the columns to match my naming convention, and drops unnecessary columns
    (like the KO definition). 
    '''
    global gene_to_genome_map

    if len(gene_to_genome_map) == 0:
        # If it is not already populated, try reading in the map from the file. 
        gene_to_genome_map = read_gene_to_genome_map()
        print('LOADED gene_to_genome_map')
        if len(gene_to_genome_map) == 0:
            raise Exception('The gene-to-genome map must be populated before calling this function')
    
    dfs = []
    # Look through every annotations file in the data directory. 

    files = os.listdir(f'{data_dir}/annotations/kegg/v1')    
    nfiles = len(files)
    files_chunk_size = 500
    
    # Keep track of number of things uploaded for the unique id column.
    curr_id = 0
        
    for i in tqdm(range(int(nfiles / files_chunk_size) + 1), desc='Loading annotation file chunks...'):    
        
        files_chunk = files[(files_chunk_size*i):min(nfiles, files_chunk_size*(i+1))] 
        
        dfs = []
        for f in files_chunk:
            # os.listdir doesn't spit out the entire filepath. 
            path = f'{data_dir}/annotations/kegg/v1/{f}'
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
                raise Exception('One of the genes in the annotation files was not present in the gene-to-genome map.'

            # Add to list of DataFrames...
            dfs.append(df)
        
        # Upload the chunk of data.
        upload_df(pd.concat(dfs), 'gtdb_r207_annotations_kegg', engine, if_exists='append', **kwargs)
        
        # Don't need to do this for the files on microbes.gps server. 

        # The column marked '#' indicates which genes exceed the specified
        # threshold; turn this into True/False conditions
        # df['threshold_met'] = df['threshold_met'].fillna(False).replace('*', True)
        

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


def load_gtdb_r207_amino_acid_seqs(data_dir, engine, **kwargs):
    '''
    Iterate over all of the files in proteins_faa_reps/{cat} and load the
    gene names, genome IDs, and amino acid sequences into a pandas DataFrame. 

    returns:
        : df (pd.DataFrame): A DataFrame containing the sequences, gene names,
            and genome IDs from all GTDB FASTA files.  
    '''
    global gene_to_genome_map

    paths = [f'{data_dir}/amino_acid_seqs/bacteria/',
            f'{data_dir}/amino_acid_seqs/archaea/']

    
    for path in paths:
        
        species = os.listdir(path)
        nspecies = len(species)
        species_chunk_size = 500
        
        for i in tqdm(range(int(nspecies / species_chunk_size) + 1), desc='Loading species chunks...'):
            
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
                    global gene_to_genome_map
                    gene_to_genome_map[gene_name] = genome_id

                # Add the species information to the pandas DataFrame.
                df = pd.concat([df, pd.DataFrame(d)], ignore_index=True)

            df = df.fillna('None')
            # upload_df(df, 'gtdb_r207_amino_acid_seqs', engine, if_exists='append', **kwargs)
    
    try:
        # Write the gene_to_genome map to a file. 
        write_gene_to_genome_map()
    except Exception as e:
        print('Failed to write to gene_to_genome_map.csv:')
        print(e)
        pass


def split_taxonomies(df):
    '''
    Genome taxonomy is represented as super long strings separated using
    semicolons. This expands the taxonomy string into multiple columns. 
    '''
    tax_cats = ['domain', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    cols = df.columns
    
    for col in cols:
        if 'taxonomy' in col:
            try:
                col_prefix = col[:-len('taxonomy')]
                new_cols = [col_prefix + cat for cat in tax_cats]
                df[new_cols] = getattr(df, col).str.split(';', expand=True)
            except:
                print(f'WARNING: Could not load {col_prefix[:-1]} taxonomy.')
                pass
    
    # Drop the taxonomy columns which have been expanded. 
    return df.drop(columns=[c for c in cols if 'taxonomy' in c])


def int_converter(val):
    '''
    Convert missing int values in the Metadata when reading the CSV file. 
    '''
    # If the encountered value is NaN...
    if val == 'none':
        # The problem columns use integers which should be positive (e.g. RNA
        # counts). 
        return -1
    else:
        return int(val)

 
def load_gtdb_r207_metadata(data_dir): 
    '''
    Load a metadata file into a pandas DataFrame. 

    args:
        : path (str): The path to the metadata file. 

    returns:
        : df (pd.DataFrame): A DataFrame containing the genome metadata. 
    '''
    paths = [data_dir + '/metadata/bac120_metadata_r207.tsv',
            data_dir + '/metadata/ar53_metadata_r207.tsv']
    dfs = []
    
    for path in paths:
        with open(path, 'r') as f:
            # The NCBI columns have datatype issues. Just read in as strings
            # using converters. 
            cols_to_int = ['ncbi_ncrna_count', 'ncbi_rrna_count', 'ncbi_ssu_count',
                'ncbi_translation_table', 'ncbi_trna_count',
                'ncbi_ungapped_length']
            df = pd.read_csv(f, delimiter='\t', converters={c:int_converter for c in cols_to_int})
            # Split up taxonomy information into multiple columns. 
            df = split_taxonomies(df)
            
            # TODO: This is being really weird, "invalid string value"?
            df = df.drop(columns=['ncbi_submitter'])

            # Rename the genome ID column for consistency with the sequence data. 
            df = df.rename(columns={'accession':'genome_id'})
            # Add the domain information to the DataFrame. 
            dfs.append(df)
   
    df = pd.concat(dfs).reset_index(drop=True)
    df = df.fillna('None') # Get rid of any residual NaNs, which still seem to show up.
    return df
 

def get_sql_dtypes(df):
    '''
    '''
    dtypes = {}
    for col_name in df.columns:
        
        t = type(df[col_name].iloc[0]) # Get the column datatype.  
        # Convert the datatype for SQLAlchemy usage. 

        if t == str:
            # Special case for amino acid sequences because they are so long. 
            if col_name == 'sequence':
                dtypes[col_name] = LONGTEXT
            elif col_name == 'gene_name':
                # Having some issues with this field too, because uploading in chunks with the
                # amino_acid_seqs table. This should fix it?
                dtypes[col_name] = VARCHAR(150)
            else:
                # Get the maximum length to be accomodated. 
                max_length = max(df[col_name].apply(len))
                dtypes[col_name] = VARCHAR(max_length)
        elif t in [int, np.int64]:
            dtypes[col_name] = Integer
        elif t in [np.float64, float]:
            dtypes[col_name] = Float
        elif t in [bool, np.bool_]:
            dtypes[col_name] = Boolean
        else:
            msg = 'Type ' + str(t) + ' is not handled by create_table.'
            raise Exception(msg)
    return dtypes


def upload_df(df, name, engine, if_exists='replace', **kwargs):
    '''
    Uploads a pandas DataFrame to the SQL database. If specified, appends the
    DataFrame information to an existing table. 
    '''
    if df.empty:
        print(f'Attempted to load an empty DataFrame to table {name}. Upload failed.')
    else:
        t_init = perf_counter()
        df.to_sql(name, engine, dtype=get_sql_dtypes(df), if_exists=if_exists, **kwargs)
        t_final = perf_counter()
        print(f'UPLOAD TO TABLE {name}: {t_final - t_init} seconds')
    

def setup(
        url,
        data_dir='/var/lib.pgsql/data',
        source='gtdb', 
        release='r207',
        chunksize=500,
        echo=True): 
    '''
    '''
    print(f'STARTING ENGINE WITH URL {url}')
    
    engine = sqlalchemy.create_engine(url, echo=echo)
    # Grab the data directory out of the config file. 

    # # DROP table if already present. 
    # inspector = sqlalchemy.inspect(engine)
    # existing_tables = inspector.get_table_names()
    # with engine.connect() as conn:
    #     for t in [f'{source}_{release}_metadata', f'{source}_{release}_amino_acid_seqs',
    #             f'{source}_{release}_annotations_kegg']:
    #         if t in existing_tables:
    #             print(f'DROP TABLE {t}')
    #             conn.execute(text(f'DROP TABLE {t}'))
    
    # TODO: Eventually should generalize this for multiple sources and releases. 
    # Load in the data from the CSV files. 
    t_init = perf_counter()
    
    # Specifications for DataFrame upload. 
    kwargs = {'chunksize':chunksize, 'method':'multi'}
    
    # NOTE: Amino acid sequences are handled slightly differently... uploaded as
    # they are loaded into DataFrames.
    load_gtdb_r207_amino_acid_seqs(data_dir, engine, **kwargs)
 
    gtdb_r207_metadata = load_gtdb_r207_metadata(data_dir)
    upload_df(gtdb_r207_metadata, 'gtdb_r207_metadata', engine, **kwargs)
    
    # Also ended up uploading these as chunks.
    gtdb_r207_annotations_kegg = load_gtdb_r207_annotations_kegg(data_dir, engine, **kwargs)
   
    t_final = perf_counter()
    print(f'UPLOAD PROCESS COMPLETE: {t_final - t_init} seconds')
    
